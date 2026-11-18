"""In-process task API. Importing this module does not start any workers."""
import asyncio
import builtins
import codecs
import ctypes
import json
import runpy
import secrets
import signal
import socket
import struct
import copy
import inspect
import logging
import math
import os
from pathlib import Path
import queue
import sys
import threading
import time
import traceback
import uuid
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone

_MANAGER = None
# Modules launched with `python backend/back.py` and package imports share this API.
sys.modules.setdefault('backapi', sys.modules[__name__])


def get_task_manager():
    if _MANAGER is None:
        raise RuntimeError('任务管理器尚未初始化')
    return _MANAGER


def install_task_manager(manager):
    global _MANAGER
    _MANAGER = manager


class TaskAPIError(Exception):
    @property
    def code(self):
        return type(self).__name__


class TaskNotFound(TaskAPIError): pass
class QueueNotFound(TaskAPIError): pass
class ScheduleNotFound(TaskAPIError): pass
class InvalidTaskState(TaskAPIError): pass
class RevisionConflict(TaskAPIError): pass
class ServiceStopping(TaskAPIError): pass
class InputAlreadyResolved(TaskAPIError): pass
class InputNotFound(TaskAPIError): pass
class InputTargetMismatch(TaskAPIError): pass
class InputTimeoutError(TaskAPIError): pass
class TaskCancelled(BaseException): pass


def now():
    return datetime.now(timezone.utc).isoformat()


def uid(prefix):
    return prefix + '-' + uuid.uuid4().hex


def positive(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(name + ' 必须为正的有限数值')
    return value


def text(value, name):
    if not isinstance(value, str):
        raise TypeError(name + ' 必须为字符串')
    return value


@dataclass(frozen=True)
class TaskCreation:
    task_id: str = None
    queue_id: str = None
    schedule_id: str = None
    entry_id: str = None


class TaskContext:
    def __init__(self, manager, task):
        self._manager, self._task = manager, task

    @property
    def task_id(self): return self._task['task_id']
    @property
    def queue_id(self): return self._task['queue_id']
    @property
    def schedule_id(self): return self._task['schedule_id']
    @property
    def entry_id(self): return self._task['entry_id']
    @property
    def run_id(self): return self._task['run_id']

    async def checkpoint(self):
        m, t = self._manager, self._task
        if t['cancel_requested']:
            raise TaskCancelled()
        if t['pause_requested']:
            t['pause_requested'] = False
            t['state'], t['paused_at'] = 'paused', now()
            t['_gate'].clear()
            q = m.queues[self.queue_id]
            if q['running_task_id'] == self.task_id:
                q['running_task_id'] = None
            m._changed()
            m._pump(self.queue_id)
        await t['_gate'].wait()
        if t['cancel_requested']:
            raise TaskCancelled()

    def _sync(self, coroutine):
        if threading.get_ident() == self._manager.thread_id:
            coroutine.close()
            raise RuntimeError('同步接口不能在事件循环线程调用')
        return asyncio.run_coroutine_threadsafe(coroutine, self._manager.loop).result()

    def checkpoint_sync(self):
        return self._sync(self.checkpoint())

    async def sleep(self, seconds):
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
            raise ValueError('seconds 必须为非负有限数值')
        end = time.monotonic() + seconds
        while True:
            await self.checkpoint()
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(self._task['_wake'].wait(), min(remaining, .1))
                self._task['_wake'].clear()
            except asyncio.TimeoutError:
                pass

    def sleep_sync(self, seconds):
        return self._sync(self.sleep(seconds))

    async def input(self, prompt='', timeout=None):
        m, t = self._manager, self._task
        await self.checkpoint()
        text(prompt, 'prompt')
        if len(prompt.encode('utf-8')) > 16384:
            raise ValueError('提示文字超过 16 KiB')
        if timeout is not None:
            positive(timeout, 'timeout')
        if t.get('_input_busy'):
            raise InvalidTaskState('同一任务不能同时等待两次输入')
        t['_input_busy'] = True
        key = uid('input')
        future = m.loop.create_future()
        request = dict(input_id=key, queue_id=self.queue_id, task_id=self.task_id,
                       task_name=t['name'], prompt=prompt, created_at=now(), state='pending')
        m.inputs[key] = (request, future)
        t['input_id'], t['waiting_for_input'] = key, True
        if prompt:
            m._log(t, prompt)
        m._changed('TASK_INPUT_REQUEST', **request)
        def expire():
            if key in m.inputs:
                future.set_exception(InputTimeoutError('等待输入超时'))
                m._resolve_input(key, 'failed')
        timer = None if timeout is None else m.loop.call_later(timeout, expire)
        try:
            while not future.done():
                if t['cancel_requested']:
                    raise TaskCancelled()
                await self.checkpoint()
                try:
                    await asyncio.wait_for(asyncio.shield(future), .05)
                except asyncio.TimeoutError:
                    pass
            await self.checkpoint()
            return future.result()
        finally:
            if timer: timer.cancel()
            if future.done() and not future.cancelled(): future.exception()
            if key in m.inputs:
                m._resolve_input(key, 'cancelled')
            t['input_id'], t['waiting_for_input'] = None, False
            t['_input_busy'] = False
            m._changed()

    def input_sync(self, prompt='', timeout=None):
        return self._sync(self.input(prompt, timeout))

    def log(self, value, *, level='info'):
        text(value, 'text')
        text(level, 'level')
        m = self._manager
        if threading.get_ident() == m.thread_id:
            m._log(self._task, value, level)
        else:
            try:
                m.thread_logs.put_nowait((self._task, value, level))
            except queue.Full:
                with m.drop_lock:
                    m.dropped += 1


class TaskManager:
    def __init__(self, backend_dir=None, max_workers=8, notify=None):
        self.backend_dir = Path(backend_dir or Path(__file__).parent).resolve()
        self.notify = notify
        self.loop = None
        self.thread_id = None
        self.executor = None
        self.max_workers = max_workers
        self.queues, self.tasks, self.schedules = OrderedDict(), OrderedDict(), OrderedDict()
        self.inputs, self.resolved = {}, OrderedDict()
        self.revision, self.stopping = 0, False
        self.thread_logs = queue.Queue(512)
        self.drop_lock, self.dropped = threading.Lock(), 0
        self.queues['main'] = self._queue('main', '主队列')
        self.ticker = None

    def _queue(self, key, name):
        return dict(queue_id=key, name=name, created_at=now(), is_main=key == 'main',
                    running_task_id=None, _tasks=[], _logs=deque(maxlen=1000),
                    _events=deque(maxlen=2000), _seq=0, _clear=0, _cleaning=0)

    async def start(self):
        if self.loop:
            return
        self.loop, self.thread_id = asyncio.get_running_loop(), threading.get_ident()
        self.executor = ThreadPoolExecutor(self.max_workers, thread_name_prefix='tiphereth-task')
        self.ticker = asyncio.create_task(self._tick())

    def _check(self, mutate=False):
        if self.loop is None:
            raise RuntimeError('任务管理器尚未启动')
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError('必须在任务管理器的事件循环调用')
        if mutate and self.stopping:
            raise ServiceStopping('服务正在退出')

    def _changed(self, command='TASK_CHANGED', **data):
        self.revision += 1
        self._emit(dict(command=command, revision=self.revision, **data))
        if command != 'TASK_CHANGED':
            self._emit(dict(command='TASK_CHANGED', revision=self.revision))

    def _emit(self, event):
        if self.notify:
            self.notify(event)

    def _task(self, key):
        try: return self.tasks[key]
        except (KeyError, TypeError): raise TaskNotFound('任务不存在') from None

    def _get_queue(self, key):
        try: return self.queues[key]
        except (KeyError, TypeError): raise QueueNotFound('队列不存在') from None

    def _schedule(self, key):
        try: return self.schedules[key]
        except (KeyError, TypeError): raise ScheduleNotFound('定时计划不存在') from None

    def _public(self, item):
        return copy.deepcopy({k: v for k, v in item.items() if not k.startswith('_')})

    def _definition(self, name, func, script, args, kwargs):
        text(name, 'name')
        if not name.strip(): raise ValueError('任务名称不能为空')
        if (func is None) == (script is None): raise ValueError('func 与 script 必须二选一')
        if not isinstance(args, (tuple, list)) or (kwargs is not None and not isinstance(kwargs, dict)):
            raise TypeError('args 必须为列表或元组，kwargs 必须为字典')
        if script is not None:
            if args or kwargs: raise ValueError('脚本不使用 args/kwargs，请使用 script.argv')
            if not isinstance(script, dict): raise TypeError('script 必须为字典')
            path = Path(text(script.get('path'), 'script.path'))
            if not path.is_absolute(): path = self.backend_dir / path
            path = path.resolve()
            cwd = Path(text(script.get('cwd', str(path.parent)), 'script.cwd'))
            if not cwd.is_absolute(): cwd = self.backend_dir / cwd
            cwd = cwd.resolve()
            argv = script.get('argv', [])
            if not isinstance(argv, list) or any(not isinstance(x, str) or '\0' in x for x in argv):
                raise TypeError('script.argv 必须为字符串列表')
            if path.suffix.lower() != '.py' or not path.is_file() or not cwd.is_dir():
                raise ValueError('脚本或工作目录不存在')
            script = dict(path=str(path), cwd=str(cwd), argv=list(argv))
        elif not callable(func): raise TypeError('func 必须可调用')
        return dict(entry_id=uid('entry'), name=name, _func=func, _script=script,
                    _args=tuple(args), _kwargs=dict(kwargs or {}))

    def _target(self, queue_id, new_queue_name):
        if new_queue_name is not None:
            if queue_id is not None or not text(new_queue_name, 'new_queue_name').strip():
                raise ValueError('新建队列须传 queue_id=None 和非空名称')
        else:
            self._get_queue(queue_id)

    def _timing(self, timing):
        if not isinstance(timing, dict): raise TypeError('timing 必须为字典')
        kind = timing.get('type')
        field = 'run_at' if kind == 'once' else 'start_at'
        if kind not in ('once', 'interval'): raise ValueError('不支持的定时类型')
        value = datetime.fromisoformat(text(timing.get(field), field).replace('Z', '+00:00'))
        if value.utcoffset() is None or value.timestamp() <= time.time():
            raise ValueError('时间必须带时区且晚于服务端当前时间')
        result = dict(type=kind, **{field: value.isoformat()})
        if kind == 'interval':
            seconds = positive(timing.get('seconds'), 'seconds')
            if seconds < 1: raise ValueError('间隔至少 1 秒')
            result['seconds'] = seconds
        return result, value.timestamp()

    async def create_task(self, name, func=None, *, script=None, args=(), kwargs=None,
                          queue_id='main', new_queue_name=None, timing=None, after_schedule_id=None):
        self._check(True)
        entry = self._definition(name, func, script, args, kwargs)
        if after_schedule_id is not None:
            if timing is not None or new_queue_name is not None or queue_id is not None:
                raise ValueError('追加计划只接受 queue_id=None，不接受时间或新队列设置')
            plan = self._schedule(after_schedule_id)
            plan['_entries'].append(entry)
            plan['revision'] += 1
            self._changed()
            return TaskCreation(schedule_id=after_schedule_id, entry_id=entry['entry_id'])
        self._target(queue_id, new_queue_name)
        if timing is not None:
            timing, stamp = self._timing(timing)
            key = uid('schedule')
            self.schedules[key] = dict(schedule_id=key, name=name, created_at=now(), timing=timing,
                queue_id=queue_id, new_queue_name=new_queue_name, next_run_at=datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
                last_triggered_at=None, last_run_id=None, last_task_ids=[], skipped_count=0,
                state='active', error=None, revision=1, _entries=[entry], _next=stamp)
            self._changed()
            return TaskCreation(schedule_id=key, entry_id=entry['entry_id'])
        q, items = self._enqueue([entry], queue_id, new_queue_name)
        self._changed()
        self._pump(q['queue_id'])
        return TaskCreation(task_id=items[0]['task_id'], queue_id=q['queue_id'])

    def _enqueue(self, entries, queue_id, new_queue_name, schedule_id=None, run_id=None):
        # Validate the complete batch before exposing any queue or task.
        for entry in entries:
            if entry['_script']:
                d = entry['_script']
                if not Path(d['path']).is_file() or not Path(d['cwd']).is_dir():
                    raise ValueError('脚本或工作目录已经消失')
        self._target(queue_id, new_queue_name)
        if new_queue_name is not None:
            queue_id = uid('queue')
            self.queues[queue_id] = self._queue(queue_id, new_queue_name)
        q, items = self.queues[queue_id], []
        for entry in entries:
            key = uid('task')
            t = dict(task_id=key, name=entry['name'], queue_id=queue_id, schedule_id=schedule_id,
                entry_id=entry['entry_id'] if schedule_id else None, run_id=run_id, state='queued',
                created_at=now(), started_at=None, paused_at=None, error=None, pause_requested=False,
                cancel_requested=False, kind='python_script' if entry['_script'] else 'function',
                can_pause=True, waiting_for_input=False, input_id=None,
                _entry=entry, _gate=asyncio.Event(), _wake=asyncio.Event(), _runner=None,
                _process=None, _cleaning=False, _partial=None, _style='', _sgr={}, _ansi_pending='', _cr=False)
            self.tasks[key] = t
            q['_tasks'].append(key)
            items.append(t)
        return q, items

    def _pump(self, key):
        q = self.queues.get(key)
        if not q or q['running_task_id'] or q['_cleaning'] or self.stopping: return
        for task_id in q['_tasks']:
            t = self.tasks[task_id]
            if t['state'] == 'queued' and not t['cancel_requested']:
                q['running_task_id'] = task_id
                t['state'], t['can_pause'] = 'running', t['kind'] == 'function'
                t['_gate'].set()
                if t['_runner'] is None:
                    t['_runner'] = asyncio.create_task(self._execute(t))
                self._changed()
                break

    async def _execute(self, t):
        ctx, entry = TaskContext(self, t), t['_entry']
        t['started_at'] = now()
        try:
            await ctx.checkpoint()
            if entry['_script']:
                await self._run_script(t, ctx)
            elif inspect.iscoroutinefunction(entry['_func']):
                await entry['_func'](ctx, *entry['_args'], **entry['_kwargs'])
            else:
                result = await self.loop.run_in_executor(self.executor,
                    lambda: entry['_func'](ctx, *entry['_args'], **entry['_kwargs']))
                if inspect.isawaitable(result): await result
            self._finish(t, 'cancelled' if t['cancel_requested'] else 'succeeded')
        except (TaskCancelled, asyncio.CancelledError):
            self._finish(t, 'cancelled')
        except Exception as exc:
            self._log(t, traceback.format_exc(), 'error')
            t['error'] = str(exc) or type(exc).__name__
            self._finish(t, 'cancelled' if t['cancel_requested'] else 'failed')

    def _finish(self, t, state):
        q = self.queues.get(t['queue_id'])
        if not q or t['task_id'] not in self.tasks: return
        if t['input_id'] in self.inputs: self._resolve_input(t['input_id'], 'cancelled')
        if q['running_task_id'] == t['task_id']: q['running_task_id'] = None
        if t['_cleaning']: q['_cleaning'] -= 1
        t['state'], t['can_pause'] = state, False
        t['waiting_for_input'], t['input_id'] = False, None
        if state != 'failed':
            q['_tasks'].remove(t['task_id'])
            del self.tasks[t['task_id']]
        if not q['_tasks'] and not q['is_main']:
            del self.queues[q['queue_id']]
        self._changed(task=self._public(t))
        self._pump(t['queue_id'])

    async def pause_task(self, task_id):
        self._check(True)
        t = self._task(task_id)
        if t['cancel_requested'] or t['state'] == 'failed' or not t['can_pause']:
            raise InvalidTaskState('此任务当前不能暂停')
        if t['state'] == 'queued':
            t['state'], t['paused_at'] = 'paused', now()
            t['_gate'].clear()
        elif t['state'] == 'running': t['pause_requested'] = True
        t['_wake'].set()
        self._changed()
        return self._public(t)

    async def resume_task(self, task_id):
        self._check(True)
        t = self._task(task_id)
        if t['cancel_requested'] or t['state'] == 'failed': raise InvalidTaskState('此任务不能恢复')
        t['pause_requested'] = False
        if t['state'] == 'paused': t['state'] = 'queued'
        t['_wake'].set()
        self._changed()
        self._pump(t['queue_id'])
        return self._public(t)

    async def cancel_task(self, task_id):
        self._check()
        t = self._task(task_id)
        if t['cancel_requested']: return self._public(t)
        t['cancel_requested'] = True
        if t['_runner'] is None or t['state'] == 'failed':
            self._finish(t, 'cancelled')
        else:
            q = self.queues[t['queue_id']]
            t['_cleaning'] = True
            q['_cleaning'] += 1
            t['_gate'].set()
            t['_wake'].set()
            if t['input_id'] in self.inputs: self._resolve_input(t['input_id'], 'cancelled')
            self._changed()
        return self._public(t)

    async def get_task(self, key):
        self._check()
        return self._public(self._task(key))

    async def list_tasks(self, queue_id='main'):
        self._check()
        return [self._public(self.tasks[k]) for k in self._get_queue(queue_id)['_tasks']]

    async def list_queues(self):
        self._check()
        result = []
        for q in self.queues.values():
            d = self._public(q)
            d['task_count'] = len(q['_tasks'])
            d['state'] = ('running' if q['running_task_id'] else 'idle' if not q['_tasks'] else
                          'waiting' if any(self.tasks[k]['state'] == 'queued' for k in q['_tasks']) else 'suspended')
            result.append(d)
        return result

    async def list_schedules(self):
        self._check()
        result = []
        for plan in sorted(self.schedules.values(), key=lambda p: p['_next'] if p['state'] == 'active' else float('inf')):
            d = self._public(plan)
            d['entries'] = [dict(entry_id=e['entry_id'], name=e['name'], order=i) for i, e in enumerate(plan['_entries'])]
            result.append(d)
        return result

    async def get_snapshot(self):
        self._check()
        return dict(queues=await self.list_queues(), tasks=[self._public(t) for t in self.tasks.values()],
            schedules=await self.list_schedules(), inputs=[copy.deepcopy(r) for r, f in self.inputs.values()],
            revision=self.revision, stats=dict(main_tasks=len(self.queues['main']['_tasks']),
            queues=len(self.queues), running=sum(t['state'] == 'running' for t in self.tasks.values())))

    def _revision(self, plan, expected):
        if type(expected) is not int or expected != plan['revision']: raise RevisionConflict('定时计划已被其他操作修改')

    async def update_schedule(self, schedule_id, *, timing, expected_revision):
        self._check(True)
        p = self._schedule(schedule_id)
        self._revision(p, expected_revision)
        timing, stamp = self._timing(timing)
        self._target(p['queue_id'], p['new_queue_name'])
        p.update(timing=timing, _next=stamp, next_run_at=datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
                 state='active', error=None, revision=p['revision'] + 1)
        self._changed()
        return next(d for d in await self.list_schedules() if d['schedule_id'] == schedule_id)

    async def delete_schedule(self, schedule_id, *, expected_revision):
        self._check(True)
        p = self._schedule(schedule_id)
        self._revision(p, expected_revision)
        result = next(d for d in await self.list_schedules() if d['schedule_id'] == schedule_id)
        del self.schedules[schedule_id]
        self._changed()
        return result

    def _resolve_input(self, key, state):
        request, future = self.inputs.pop(key)
        t = self.tasks.get(request['task_id'])
        if t and t['input_id'] == key:
            t['waiting_for_input'], t['input_id'] = False, None
        self.resolved[key] = time.monotonic()
        while len(self.resolved) > 1024: self.resolved.popitem(last=False)
        if not future.done(): future.cancel()
        self._changed('TASK_INPUT_RESOLVED', input_id=key, queue_id=request['queue_id'],
                      task_id=request['task_id'], state=state)

    async def submit_input(self, queue_id, task_id, input_id, value=None, **kwargs):
        self._check(True)
        value = kwargs.get('text', value)
        text(value, 'text')
        if any(c in value for c in '\r\n\0') or len(value.encode('utf-8')) > 16384:
            raise ValueError('输入必须为不超过 16 KiB 的单行文字')
        if input_id in self.resolved: raise InputAlreadyResolved('此输入请求已结束')
        if input_id not in self.inputs: raise InputNotFound('输入请求不存在')
        r, future = self.inputs[input_id]
        if (r['queue_id'], r['task_id']) != (queue_id, task_id): raise InputTargetMismatch('输入对象不匹配')
        t = self._task(task_id)
        if t['state'] != 'running' or t['pause_requested'] or t['cancel_requested']:
            raise InvalidTaskState('任务暂停中或正在终止')
        future.set_result(value)
        self._resolve_input(input_id, 'accepted')
        self._log(t, value)
        return dict(input_id=input_id, accepted=True)

    def _line(self, t, value, level='info', replace=None):
        q = self.queues.get(t['queue_id'])
        if not q: return None
        encoded = value.encode('utf-8')
        if len(encoded) > 16384: value = encoded[:16384].decode('utf-8', 'ignore') + '…[已截断]'
        q['_seq'] += 1
        row = dict(row_id=replace or uid('row'), seq=q['_seq'], time=now(),
                   task_id=t['task_id'], task_name=t['name'], level=level, text=value)
        existing = next((r for r in q['_logs'] if r['row_id'] == replace), None) if replace else None
        op = 'replace' if existing else 'append'
        if existing is not None: existing.update(row)
        else: q['_logs'].append(row)
        event = dict(op=op, **row)
        q['_events'].append(event)
        self._emit(dict(command='TASK_LOGS', queue_id=t['queue_id'], entries=[event], next_seq=q['_seq']))
        return row['row_id']

    def _log(self, t, value, level='info'):
        # Lines from independent tasks never share a partial row.
        for line in value.replace('\r\n', '\n').replace('\r', '\n').split('\n'):
            self._line(t, line, level)

    def _output(self, t, chunk):
        q = self.queues.get(t['queue_id'])
        if not q: return
        import re
        for char in chunk:
            if char == '\n' and t['_cr']:
                t['_cr'] = False
                continue
            if char in '\r\n':
                if t['_partial'] is None: self._line(t, '')
                else: self._line(t, t['_partial'][1], replace=t['_partial'][0])
                t['_partial'] = None
                t['_cr'] = char == '\r'
                continue
            t['_cr'] = False
            if t['_partial'] is None:
                key = self._line(t, t['_style'])
                t['_partial'] = [key, t['_style'], False]
            partial = t['_partial']
            if not partial[2]:
                partial[1] += char
                if len(partial[1].encode('utf-8')) > 16384:
                    partial[1] = partial[1].encode('utf-8')[:16384].decode('utf-8', 'ignore') + '…[已截断]'
                    partial[2] = True
            # Track bounded SGR state for independently rendered rows.
            if char == '\x1b': t['_ansi_pending'] = char
            elif t['_ansi_pending']:
                t['_ansi_pending'] += char
                if char == 'm':
                    sgr = t['_ansi_pending']
                    if re.fullmatch(r'\x1b\[[0-9;]*m', sgr):
                        for code in (int(x or '0') for x in sgr[2:-1].split(';')):
                            if code == 0: t['_sgr'].clear()
                            elif code == 1: t['_sgr']['bold'] = code
                            elif code == 22: t['_sgr'].pop('bold', None)
                            elif code == 39: t['_sgr'].pop('fg', None)
                            elif code == 49: t['_sgr'].pop('bg', None)
                            elif 30 <= code <= 37 or 90 <= code <= 97: t['_sgr']['fg'] = code
                            elif 40 <= code <= 47 or 100 <= code <= 107: t['_sgr']['bg'] = code
                        t['_style'] = ('\x1b[' + ';'.join(str(x) for x in t['_sgr'].values()) + 'm') if t['_sgr'] else ''
                    t['_ansi_pending'] = ''
                elif len(t['_ansi_pending']) > 64: t['_ansi_pending'] = ''
        if t['_partial']:
            self._line(t, t['_partial'][1], replace=t['_partial'][0])

    async def read_logs(self, queue_id='main', *, after_seq=None, limit=1000):
        self._check()
        q = self._get_queue(queue_id)
        if type(limit) is not int or not 1 <= limit <= 1000: raise ValueError('limit 必须为 1 到 1000')
        if after_seq is not None and (type(after_seq) is not int or after_seq < 0): raise ValueError('无效游标')
        truncated = after_seq is not None and (after_seq < q['_clear'] or after_seq > q['_seq'] or
            (q['_events'] and after_seq < q['_events'][0]['seq'] - 1))
        if after_seq is None or truncated:
            return dict(mode='snapshot', entries=copy.deepcopy(list(q['_logs'])[-limit:]), next_seq=q['_seq'], truncated=truncated)
        events = [e for e in q['_events'] if e['seq'] > after_seq][:limit]
        return dict(mode='delta', entries=copy.deepcopy(events), next_seq=events[-1]['seq'] if events else after_seq, truncated=False)

    async def clear_logs(self, queue_id='main'):
        self._check()
        q = self._get_queue(queue_id)
        q['_logs'].clear(); q['_events'].clear()
        q['_seq'] += 1
        q['_clear'] = q['_seq']
        for key in q['_tasks']:
            self.tasks[key]['_partial'] = None
            self.tasks[key]['_style'] = ''
            self.tasks[key]['_sgr'].clear()
            self.tasks[key]['_ansi_pending'] = ''
        self._emit(dict(command='TASK_LOGS_CLEARED', queue_id=queue_id, seq=q['_seq']))
        return q['_seq']

    async def _tick(self):
        try:
            while not self.stopping:
                for _ in range(512):
                    try: t, value, level = self.thread_logs.get_nowait()
                    except queue.Empty: break
                    self._log(t, value, level)
                with self.drop_lock:
                    dropped, self.dropped = self.dropped, 0
                if dropped:
                    self._log(dict(queue_id='main', task_id=None, name='任务管理器'), '输出过快，已丢弃 %s 条日志' % dropped, 'warning')
                for p in list(self.schedules.values()):
                    if p['state'] != 'active' or p['_next'] > time.time(): continue
                    stamp = time.time()
                    overlap = any(k in self.tasks for k in p['last_task_ids'])
                    try:
                        if overlap:
                            p['skipped_count'] += 1
                            self._log(dict(queue_id='main', task_id=None, name=p['name']), '上一轮任务仍存在，跳过本次触发', 'warning')
                        else:
                            run_id = uid('run')
                            q, items = self._enqueue(p['_entries'], p['queue_id'], p['new_queue_name'], p['schedule_id'], run_id)
                            p.update(last_run_id=run_id, last_task_ids=[t['task_id'] for t in items], last_triggered_at=now())
                            self._pump(q['queue_id'])
                        if p['timing']['type'] == 'once':
                            del self.schedules[p['schedule_id']]
                        else:
                            step = p['timing']['seconds']
                            p['_next'] += (int((stamp - p['_next']) // step) + 1) * step
                            p['next_run_at'] = datetime.fromtimestamp(p['_next'], timezone.utc).isoformat()
                        p['revision'] += 1
                    except Exception as exc:
                        p.update(state='blocked', error=str(exc), next_run_at=None, revision=p['revision'] + 1)
                    self._changed()
                await asyncio.sleep(.05)
        except asyncio.CancelledError:
            pass

    async def _run_script(self, t, ctx):
        await run_script(self, t, ctx)

    async def shutdown(self):
        if self.stopping: return
        self.stopping = True
        if self.ticker:
            self.ticker.cancel()
            await asyncio.gather(self.ticker, return_exceptions=True)
        for key in list(self.tasks): await self.cancel_task(key)
        runners = [t['_runner'] for t in self.tasks.values() if t['_runner']]
        if runners:
            done, pending = await asyncio.wait(runners, timeout=10)
            if pending:
                logging.getLogger(__name__).warning('等待不响应终止的任务完成清理: %s', len(pending))
                # Do not close module resources underneath a still-running worker.
                await asyncio.gather(*pending, return_exceptions=True)
        if self.executor: self.executor.shutdown(wait=True)
        global _MANAGER
        if _MANAGER is self: _MANAGER = None


# --- WebSocket command adapter and bounded per-connection notifications ---

COMMANDS = {
    'TASK_GET_SNAPSHOT', 'TASK_READ_LOGS', 'TASK_PAUSE', 'TASK_RESUME', 'TASK_CANCEL',
    'TASK_CLEAR_LOGS', 'TASK_SCHEDULE_UPDATE', 'TASK_SCHEDULE_DELETE', 'TASK_INPUT_SUBMIT',
}


class TaskBridge:
    def __init__(self, manager, connections):
        self.manager, self.connections = manager, connections
        self.events = asyncio.Queue(128)
        self.overflow = False
        self.worker = None
        self.ready = set()
        self.senders = {}
        manager.notify = self.notify

    def notify(self, event):
        try: self.events.put_nowait(event)
        except asyncio.QueueFull: self.overflow = True

    def forget(self, connection):
        self.ready.discard(connection)
        sender = self.senders.pop(connection, None)
        if sender:
            sender[1].cancel()

    async def start(self):
        await self.manager.start()
        self.worker = asyncio.create_task(self.broadcast())

    async def broadcast(self):
        try:
            while True:
                event = await self.events.get()
                batch = [event]
                await asyncio.sleep(.025)
                while not self.events.empty(): batch.append(self.events.get_nowait())
                # State invalidations coalesce; bounded output loss forces readers to resync.
                revision = max((e.get('revision', 0) for e in batch), default=0)
                batch = [e for e in batch if e['command'] != 'TASK_CHANGED']
                if revision or self.overflow:
                    batch.append(dict(command='TASK_CHANGED', revision=self.manager.revision, resync=self.overflow))
                self.overflow = False
                for connection in tuple(self.connections):
                    if connection not in self.ready: continue
                    if connection not in self.senders:
                        outgoing = asyncio.Queue(8)
                        worker = asyncio.create_task(self.send_connection(connection, outgoing))
                        self.senders[connection] = (outgoing, worker)
                    outgoing, worker = self.senders[connection]
                    if outgoing.full():
                        # Drop obsolete pushes, then require a fresh state/log snapshot.
                        while not outgoing.empty(): outgoing.get_nowait()
                        outgoing.put_nowait([dict(command='TASK_CHANGED', revision=self.manager.revision, resync=True)])
                    else:
                        outgoing.put_nowait(batch)
                for connection in list(self.senders):
                    if connection not in self.connections:
                        _, worker = self.senders.pop(connection)
                        worker.cancel()
                        await asyncio.gather(worker, return_exceptions=True)
                self.ready.intersection_update(self.connections)
        except asyncio.CancelledError:
            pass

    async def send_connection(self, connection, outgoing):
        try:
            while True:
                batch = await outgoing.get()
                for item in batch:
                    await connection.send(json.dumps(item, ensure_ascii=False))
        except (Exception, asyncio.CancelledError):
            pass

    async def handle(self, connection, data):
        command = data.get('command')
        if command not in COMMANDS: return False
        response = dict(command='TASK_RESULT', request_id=data.get('request_id'))
        try:
            request_id = data.get('request_id')
            if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
                raise ValueError('request_id 必须为非空字符串且不超过 128 字符')
            m = self.manager
            if command == 'TASK_GET_SNAPSHOT': result = await m.get_snapshot()
            elif command == 'TASK_READ_LOGS': result = await m.read_logs(data.get('queue_id', 'main'),
                after_seq=data.get('after_seq'), limit=data.get('limit', 1000))
            elif command == 'TASK_CLEAR_LOGS': result = await m.clear_logs(data.get('queue_id', 'main'))
            elif command == 'TASK_PAUSE': result = await m.pause_task(data.get('task_id'))
            elif command == 'TASK_RESUME': result = await m.resume_task(data.get('task_id'))
            elif command == 'TASK_CANCEL': result = await m.cancel_task(data.get('task_id'))
            elif command == 'TASK_SCHEDULE_UPDATE': result = await m.update_schedule(data.get('schedule_id'),
                timing=data.get('timing'), expected_revision=data.get('expected_revision'))
            elif command == 'TASK_SCHEDULE_DELETE': result = await m.delete_schedule(data.get('schedule_id'),
                expected_revision=data.get('expected_revision'))
            else: result = await m.submit_input(data.get('queue_id'), data.get('task_id'),
                data.get('input_id'), text=data.get('text'))
            response.update(ok=True, data=result)
        except (TaskAPIError, TypeError, ValueError) as exc:
            response.update(ok=False, error=dict(code=type(exc).__name__, message=str(exc)))
        await connection.send(json.dumps(response, ensure_ascii=False))
        return True

    async def stop(self):
        await self.manager.shutdown()
        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        for outgoing, worker in self.senders.values(): worker.cancel()
        await asyncio.gather(*(worker for outgoing, worker in self.senders.values()), return_exceptions=True)
        self.senders.clear()

# --- Parent-process script supervision, output redirection and input bridge ---

class WindowsJob:
    """Attach before the launcher receives START; closing kills descendants too."""
    def __init__(self, pid):
        from ctypes import wintypes as w
        class Basic(ctypes.Structure):
            _fields_ = [('ProcessTime', ctypes.c_int64), ('JobTime', ctypes.c_int64),
                ('Flags', w.DWORD), ('MinWS', ctypes.c_size_t), ('MaxWS', ctypes.c_size_t),
                ('Active', w.DWORD), ('Affinity', ctypes.c_size_t), ('Priority', w.DWORD), ('Scheduling', w.DWORD)]
        class IO(ctypes.Structure):
            _fields_ = [(name, ctypes.c_uint64) for name in ('ReadOp', 'WriteOp', 'OtherOp', 'ReadBytes', 'WriteBytes', 'OtherBytes')]
        class Extended(ctypes.Structure):
            _fields_ = [('Basic', Basic), ('IO', IO), ('ProcessMemory', ctypes.c_size_t),
                ('JobMemory', ctypes.c_size_t), ('PeakProcess', ctypes.c_size_t), ('PeakJob', ctypes.c_size_t)]
        k = ctypes.WinDLL('kernel32', use_last_error=True)
        k.CreateJobObjectW.argtypes = [ctypes.c_void_p, w.LPCWSTR]; k.CreateJobObjectW.restype = w.HANDLE
        k.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
        k.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]; k.OpenProcess.restype = w.HANDLE
        k.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
        k.CloseHandle.argtypes = [w.HANDLE]
        self.kernel, self.handle = k, k.CreateJobObjectW(None, None)
        if not self.handle: raise ctypes.WinError(ctypes.get_last_error())
        info = Extended(); info.Basic.Flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        process = None
        try:
            if not k.SetInformationJobObject(self.handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
                raise ctypes.WinError(ctypes.get_last_error())
            process = k.OpenProcess(0x100 | 0x1, False, pid)
            if not process or not k.AssignProcessToJobObject(self.handle, process):
                raise ctypes.WinError(ctypes.get_last_error())
        except BaseException:
            self.close()
            raise
        finally:
            if process: k.CloseHandle(process)

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def signal_process_tree(process, job=None, *, force=False):
    """Signal only the managed session/job, never a shell or caller process group."""
    if os.name == 'nt':
        if force and job is not None:
            job.close()
        elif process.returncode is None:
            try:
                process.kill() if force else process.terminate()
            except ProcessLookupError:
                pass
    else:
        # start_new_session=True makes this script's PID its process-group ID.
        # Signal descendants too, even when their original parent already exited.
        try:
            os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            pass


async def read_frame(reader):
    size = struct.unpack('!I', await reader.readexactly(4))[0]
    if size > 65536: raise ValueError('控制消息超过 64 KiB')
    result = json.loads((await reader.readexactly(size)).decode('utf-8'))
    if not isinstance(result, dict): raise ValueError('无效控制消息')
    return result


async def send_frame(writer, value):
    data = json.dumps(value, ensure_ascii=False).encode('utf-8')
    writer.write(struct.pack('!I', len(data)) + data)
    await writer.drain()


async def run_script(manager, task, ctx):
    spec = task['_entry']['_script']
    token = secrets.token_hex(32)
    accepted = asyncio.get_running_loop().create_future()
    connections, handlers = set(), set()

    async def accept(reader, writer):
        current = asyncio.current_task()
        handlers.add(current); connections.add(writer)
        try:
            auth = await asyncio.wait_for(read_frame(reader), 5)
            if auth.get('token') != token or accepted.done(): return
            accepted.set_result((reader, writer))
            await writer.wait_closed()
        except (Exception, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            connections.discard(writer); handlers.discard(current)

    server = await asyncio.start_server(accept, '127.0.0.1', 0, limit=65540)
    process, job, output, control, waiter = None, None, None, None, None
    try:
        env = dict(os.environ, PYTHONIOENCODING='utf-8', PYTHONUNBUFFERED='1',
                   TIPHERETH_TASK_PORT=str(server.sockets[0].getsockname()[1]), TIPHERETH_TASK_TOKEN=token)
        process = await asyncio.create_subprocess_exec(sys.executable, '-u',
            str(Path(__file__).resolve()), '--run-script', spec['path'], *spec['argv'],
            cwd=spec['cwd'], env=env, stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            **({'creationflags': 0x08000000} if os.name == 'nt' else {'start_new_session': True}))
        task['_process'] = process
        if os.name == 'nt': job = WindowsJob(process.pid)

        async def read_output():
            decoder = codecs.getincrementaldecoder('utf-8')('replace')
            while True:
                chunk = await process.stdout.read(4096)
                if not chunk: break
                manager._output(task, decoder.decode(chunk))
            manager._output(task, decoder.decode(b'', final=True))

        async def requests():
            reader, writer = await asyncio.wait_for(accepted, 5)
            server.close()
            await send_frame(writer, dict(type='start'))
            while True:
                try: message = await read_frame(reader)
                except asyncio.IncompleteReadError: return
                if message.get('type') != 'input': raise ValueError('无效脚本输入请求')
                answer = await ctx.input(message.get('prompt', ''))
                await send_frame(writer, dict(type='answer', text=answer))

        output = asyncio.create_task(read_output())
        control = asyncio.create_task(requests())
        waiter = asyncio.create_task(process.wait())
        # process.wait() can wait for inherited stdout pipes after the root exits.
        # Observe the actual exit code so descendants cannot hold this loop forever.
        while process.returncode is None:
            if task['cancel_requested']: raise TaskCancelled()
            if control.done():
                control.result()
                # Normal channel EOF can precede the OS process exit.
            if output.done(): output.result()
            await asyncio.sleep(.025)
        if task['cancel_requested']: raise TaskCancelled()
        # Once the script exits, it no longer owns background descendants. Closing
        # their inherited pipe handles lets the remaining output drain to EOF.
        signal_process_tree(process, job, force=True)
        await asyncio.wait_for(asyncio.shield(output), 5)
        if control.done(): control.result()
        if process.returncode: raise RuntimeError('脚本退出码：%s' % process.returncode)
        ctx.log('脚本执行完毕（退出码 0）')
    finally:
        server.close()
        for writer in list(connections): writer.close()
        for h in list(handlers): h.cancel()
        await server.wait_closed()
        if process:
            if process.returncode is None:
                signal_process_tree(process, job)
                try:
                    await asyncio.wait_for(asyncio.shield(waiter) if waiter else process.wait(), 5)
                except asyncio.TimeoutError:
                    signal_process_tree(process, job, force=True)
                    await process.wait()
            signal_process_tree(process, job, force=True)
        for worker in (control, output, waiter):
            if worker and not worker.done(): worker.cancel()
        await asyncio.gather(*(w for w in (control, output, waiter) if w), return_exceptions=True)
        for h in list(handlers): h.cancel()
        await asyncio.gather(*list(handlers), return_exceptions=True)
        task['_process'] = None

# --- Child-process entry point: no manager, executor or server is started here ---

MAX_FRAME = 65536


def _read_child_frame(sock):
    def exact(size):
        data = bytearray()
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk: raise EOFError('任务输入通道已关闭')
            data.extend(chunk)
        return bytes(data)
    size = struct.unpack('!I', exact(4))[0]
    if size > MAX_FRAME: raise ValueError('控制消息过长')
    return json.loads(exact(size).decode('utf-8'))


def _send_child_frame(sock, data):
    payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
    if len(payload) > MAX_FRAME: raise ValueError('控制消息过长')
    sock.sendall(struct.pack('!I', len(payload)) + payload)


def _script_main():
    host = ('127.0.0.1', int(os.environ.pop('TIPHERETH_TASK_PORT')))
    token = os.environ.pop('TIPHERETH_TASK_TOKEN')
    channel = socket.create_connection(host, timeout=5)
    _send_child_frame(channel, dict(token=token))
    if _read_child_frame(channel).get('type') != 'start': raise RuntimeError('脚本启动未获确认')
    channel.settimeout(None)
    lock = threading.Lock()

    def task_input(prompt=''):
        if not lock.acquire(False): raise RuntimeError('同一任务不能同时请求两次输入')
        try:
            prompt = str(prompt)
            if len(prompt.encode('utf-8')) > 16384: raise ValueError('提示过长')
            sys.stdout.flush(); sys.stderr.flush()
            _send_child_frame(channel, dict(type='input', prompt=prompt))
            response = _read_child_frame(channel)
            if response.get('type') != 'answer': raise EOFError('任务输入已结束')
            return response['text']
        finally:
            lock.release()

    builtins.input = task_input
    path = Path(sys.argv[1]).resolve()
    sys.argv = [str(path)] + sys.argv[2:]
    sys.path[0] = str(path.parent)
    try:
        runpy.run_path(str(path), run_name='__main__')
    finally:
        channel.close()

if __name__ == '__main__':
    if len(sys.argv) < 3 or sys.argv[1] != '--run-script':
        raise SystemExit('Usage: python backapi.py --run-script script.py [args ...]')
    del sys.argv[1]
    _script_main()
