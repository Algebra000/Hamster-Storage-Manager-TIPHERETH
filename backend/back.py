"""TIPHERETH 主框架：配置、模块生命周期与单端口 WebSocket 接入。"""

import asyncio
import importlib
import json
import logging
import mimetypes
import os
from pathlib import Path
import sys
import tempfile
import uuid
from urllib.parse import quote

from aiohttp import web, WSMsgType, WSCloseCode

DEFAULT_PORT = 8768
BACK_VERSION = 'TIPHERETH 1.0.0'
SCRIPT_DIR = Path(__file__).resolve().parent

# web.AppKey() 的作用是创建一个带类型信息的专用键对象，
# 用于在 aiohttp Application 对象中存储共享数据
CONFIG_KEY = web.AppKey('global_config', dict)
MODULES_KEY = web.AppKey('modules', dict)
SESSIONS_KEY = web.AppKey('sessions', dict)
CONNECTIONS_KEY = web.AppKey('connections', set)
HANDLERS_KEY = web.AppKey('handlers', set)
STOPPING_KEY = web.AppKey('stopping', asyncio.Event)
RESOURCES_KEY = web.AppKey('resources', object)
BLOBS_KEY = web.AppKey('blobs', object)
LOGGER = logging.getLogger(__name__)


class ResourceRegistry:
    """以 base_dir 原始列表索引登记根目录，不接受客户端提供绝对磁盘路径。"""

    def __init__(self, roots, protected_paths=()):
        self.roots = {}
        self.protected_paths = tuple(Path(p).resolve() for p in protected_paths)
        for index, value in enumerate(roots):
            try:
                root = Path(value).resolve()
                if root.is_dir() and not self.is_protected(root):
                    self.roots[str(index)] = root
            except (OSError, RuntimeError, ValueError):
                # 无效目录保留编号空位，不影响后续配置项的索引。
                LOGGER.warning('无法登记资源根目录 base_dir[%s]: %s', index, value)

    def is_protected(self, path):
        return any(path == protected or protected in path.parents
                   for protected in self.protected_paths)

    def describe(self):
        return [{'id': key, 'name': root.name or root.anchor}
                for key, root in self.roots.items()]

    def resolve(self, root_id, relative_path):
        root = self.roots.get(root_id)
        if root is None:
            raise web.HTTPNotFound()
        # match_info 已经过 URL 解码，不再 unquote，避免二次解码路径穿越。
        parts = relative_path.split('/')
        if any(not part or part in ('.', '..') or part.endswith((' ', '.'))
               or any(c in part for c in '\\:\x00')
               or any(ord(c) < 32 for c in part) for part in parts):
            raise web.HTTPForbidden()
        try:
            target = root.joinpath(*parts).resolve(strict=True)
            # resolve 跟随符号链接和 Windows junction，检查最终磁盘路径。
            if (target != root and root not in target.parents) or self.is_protected(target):
                raise web.HTTPForbidden()
            if not target.is_file():
                raise web.HTTPNotFound()
            return target
        except PermissionError:
            raise web.HTTPForbidden() from None
        except (OSError, RuntimeError, ValueError):
            raise web.HTTPNotFound() from None

    def url_for(self, file_path):
        """模块可把允许目录内的文件转换为 URL 路径，不包含主机或端口。"""
        target = Path(file_path).resolve(strict=True)
        # 更深的根目录优先；重复目录选择配置中最小的原始索引。
        for root_id, root in sorted(
            self.roots.items(), key=lambda item: (-len(item[1].parts), int(item[0]))
        ):
            if target == root or root in target.parents:
                relative = target.relative_to(root).as_posix()
                self.resolve(root_id, relative)
                return '/resources/' + root_id + '/' + quote(relative, safe='/')
        raise ValueError('文件不在允许的资源目录内')


@web.middleware
async def resource_cors(request, handler):
    """仅通用只读 HTTP 路由接受跨源预检；WebSocket 不使用 CORS。"""
    if request.method == 'OPTIONS' and (
        request.path == '/api/server-info' or request.path.startswith(('/resources/', '/blobs/'))
    ):
        method = request.headers.get('Access-Control-Request-Method', 'GET').upper()
        requested_headers = {v.strip().lower() for v in request.headers.get(
            'Access-Control-Request-Headers', '').split(',') if v.strip()}
        if method not in ('GET', 'HEAD') or not requested_headers.issubset({
            'range', 'if-range', 'if-none-match', 'if-modified-since',
        }):
            raise web.HTTPForbidden()
        return web.Response(status=204)
    return await handler(request)


async def prepare_resource_headers(request, response):
    # FileResponse 在 prepare 时才确定 206/304/416 等结果，使用 aiohttp 的
    # on_response_prepare 钩子，保证这些响应与 404/403 一样带跨源响应头。
    if request.path == '/api/server-info' or request.path.startswith(('/resources/', '/blobs/')):
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = (
            'Range, If-Range, If-None-Match, If-Modified-Since')
        response.headers['Access-Control-Expose-Headers'] = (
            'Content-Length, Content-Range, Accept-Ranges, ETag, Last-Modified')
        response.headers['X-Content-Type-Options'] = 'nosniff'
        # 不发送 Allow-Credentials；客户端用 credentials: 'omit' 读取资源。


async def server_info_handler(request):
    return web.json_response({
        'version': BACK_VERSION,
        'protocol_version': 1,
        'base_port': request.app[CONFIG_KEY]['base-port'],
        'websocket_path': '/ws',
        'capabilities': {'http_resources': True, 'byte_ranges': True, 'websocket': True},
        'resource_roots': request.app[RESOURCES_KEY].describe(),
    }, headers={'Cache-Control': 'no-store'})


async def resource_handler(request):
    # 路径解析可能访问网络盘，放到线程执行，不阻塞 WebSocket 的事件循环。
    # run_in_executor 兼容 Python 3.8（asyncio.to_thread 从 3.9 才提供）。
    target = await asyncio.get_running_loop().run_in_executor(
        None,
        request.app[RESOURCES_KEY].resolve,
        request.match_info['root_id'], request.match_info['relative_path'],
    )
    # aiohttp 负责 HEAD、单段 Range、条件请求、MIME 和有界文件传输。
    # no-cache 允许缓存但要求再次校验，适合会被外部工具更新的资源。
    headers = {'Cache-Control': 'no-cache'}
    # Windows 注册表的 MIME 映射可能缺少字体类型，常见媒体类型明确指定。
    media_types = {
        '.woff': 'font/woff', '.woff2': 'font/woff2', '.ttf': 'font/ttf',
        '.otf': 'font/otf', '.mp4': 'video/mp4', '.webm': 'video/webm',
        '.mp3': 'audio/mpeg', '.m4a': 'audio/mp4', '.svg': 'image/svg+xml',
    }
    if target.suffix.lower() in media_types:
        headers['Content-Type'] = media_types[target.suffix.lower()]
    return web.FileResponse(target, headers=headers)


class BlobStore:
    """全应用共享的 blob 登记表，按创建顺序将内存数据转存到文件。"""

    def __init__(self, max_bytes, spill, directory):
        self.entries = {}
        self.memory_bytes = 0
        self.max_bytes = max_bytes
        self.spill = spill
        self.directory = Path(directory)

    def __len__(self):
        return len(self.entries)

    def get(self, token):
        return self.entries.get(token)

    def _write_file(self, data):
        self.directory.mkdir(parents=True, exist_ok=True)
        path = None
        try:
            with tempfile.NamedTemporaryFile(prefix='http-blob-', suffix='.bin',
                                             dir=str(self.directory), delete=False) as target:
                path = Path(target.name)
                target.write(data)
            return path
        except BaseException:
            if path is not None:
                path.unlink()
            raise

    def add(self, token, data, content_type, name):
        size = len(data)
        if self.memory_bytes + size > self.max_bytes and not self.spill:
            raise MemoryError('blob 数据总大小超过 max-blob-bytes')
        # 单个数据超限无法驻留内存，直接存为文件，避免无意义地转存所有旧数据。
        if size > self.max_bytes:
            path = self._write_file(data)
            self.entries[token] = (None, content_type, name, path)
            return
        for old_token, entry in self.entries.items():
            if self.memory_bytes + size <= self.max_bytes:
                break
            old_data, old_type, old_name, old_path = entry
            if old_data is not None:
                path = self._write_file(old_data)
                # 写入成功后才释放登记表中的 bytes，失败时原数据仍可读取。
                self.entries[old_token] = (None, old_type, old_name, path)
                self.memory_bytes -= len(old_data)
        self.entries[token] = (data, content_type, name, None)
        self.memory_bytes += size

    def pop(self, token, default=None):
        entry = self.entries.get(token)
        if entry is None:
            return default
        data, _, _, path = entry
        if path is not None:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        del self.entries[token]
        if data is not None:
            self.memory_bytes -= len(data)
        return entry


async def blob_handler(request):
    """读取模块登记的内存数据；HEAD 由 aiohttp 自动省略响应正文。"""
    entry = request.app[BLOBS_KEY].get(request.match_info['token'])
    if entry is None:
        raise web.HTTPNotFound()
    data, content_type, data_name, path = entry
    if request.match_info['data_name'] != data_name:
        raise web.HTTPNotFound()
    if path is not None:
        return web.FileResponse(path, headers={'Content-Type': content_type,
                                              'Cache-Control': 'no-store'})
    return web.Response(body=data, content_type=content_type,
                        headers={'Cache-Control': 'no-store'})


class FrameworkConnection:
    """功能模块使用的连接适配层，不包含任何模块业务状态。"""

    def __init__(self, websocket, *, send_timeout=30, resources=None, resource_origin=None, blobs=None):
        self.websocket = websocket
        self.connection_id = None
        self.ended = False
        self.attached = asyncio.Event()
        self.attached.set()
        self.expiry = None
        self.worker = None
        self.attach_lock = asyncio.Lock()
        self.resources = resources
        self.resource_origin = resource_origin
        self._blobs = blobs
        self._blob_tokens = {}
        self.incoming = asyncio.Queue(maxsize=32)
        self.send_timeout = send_timeout
        self._send_lock = asyncio.Lock()

    @property
    def closed(self):
        return self.websocket.closed

    async def send(self, data):
        """保持 str/bytes 消息边界；不解析 JSON，也不修改 HSV1 内容。"""
        if not isinstance(data, (str, bytes, bytearray, memoryview)):
            raise TypeError('send() only accepts text or binary data')

        if self.connection_id:
            await self.attached.wait()
            if self.ended:
                raise ConnectionError('Session ended')

        async def write():
            async with self._send_lock:
                if transport.closed:
                    raise ConnectionError('WebSocket is closed')
                if isinstance(data, str):
                    await transport.send_str(data)
                else:
                    await transport.send_bytes(data)

        while True:
            transport = self.websocket
            try:
                await asyncio.wait_for(write(), timeout=self.send_timeout)
                return
            except (asyncio.TimeoutError, ConnectionError, OSError) as error:
                if isinstance(error, asyncio.TimeoutError):
                    await transport.close(code=WSCloseCode.TRY_AGAIN_LATER, message=b'Send timeout')
                if not self.connection_id or self.ended:
                    raise ConnectionError('WebSocket send failed') from error
                # 不建立消息缓存；当前发送协程等待传输恢复（最多到会话到期）。
                while self.websocket is transport and not self.ended:
                    await asyncio.sleep(0.1)
                await self.attached.wait()
                if self.ended:
                    raise ConnectionError('Session ended')

    async def recv(self):
        """由框架唯一的业务消费者调用，网络读取由接入层负责。"""
        return await self.incoming.get()

    async def close(self, code=WSCloseCode.OK, reason=''):
        if not self.connection_id:
            self._clear_blob_urls()
        await self.websocket.close(code=code, message=reason.encode('utf-8'))

    def resource_url(self, file_path):
        """对根目录下的文件生成允许资源文件的绝对 HTTP(S) URL，使用本URL访问该文件。"""
        if self.resources is None:
            raise RuntimeError('资源登记表不可用')
        if self.resource_origin is None:
            raise RuntimeError('连接的资源服务地址不可用')
        return self.resource_origin + self.resources.url_for(file_path)

    def blob_url(self, data_name, data: bytes):
        """登记 bytes 并返回独立的绝对 HTTP(S) URL；名称仅用于推断 MIME。

        每次调用创建新资源，即使名称或内容相同。同步方法，在框架事件循环中调用。
        """
        if not isinstance(data_name, str) or not data_name:
            raise ValueError('data_name must be a non-empty string')
        if data_name in ('.', '..') or any(c in data_name for c in '/\\') or any(ord(c) < 32 or ord(c) == 127 for c in data_name):
            raise ValueError('data_name must be a filename without separators or control characters')
        if not isinstance(data, bytes):
            raise TypeError('data must be bytes')
        if self.closed:
            raise ConnectionError('WebSocket is closed')
        if self._blobs is None or self.resource_origin is None:
            raise RuntimeError('连接的二进制资源服务不可用')
        token = uuid.uuid4().hex
        url = self.resource_origin + '/blobs/' + token + '/' + quote(data_name, safe='')
        content_type = mimetypes.guess_type(data_name)[0] or 'application/octet-stream'
        self._blobs.add(token, data, content_type, data_name)
        self._blob_tokens[url] = token
        return url

    def delete_blob_url(self, blob_url: str):
        """按创建时返回的完整 URL 删除本连接资源，返回是否存在；不删除其他连接资源。"""
        if not isinstance(blob_url, str):
            raise TypeError('blob_url must be a string')
        token = self._blob_tokens.get(blob_url)
        if token is None:
            return False
        self._blobs.pop(token, None)
        del self._blob_tokens[blob_url]
        return True

    def _clear_blob_urls(self):
        """清理该连接持有的全部数据，不影响其他连接。"""
        if self._blobs is not None:
            for token in self._blob_tokens.values():
                self._blobs.pop(token, None)
        self._blob_tokens.clear()


def load_global_config(path):
    """缺少端口或 HTTP URL 配置时原子补写默认值，不回写运行时资源路径。"""
    path = Path(path)
    try:
        with path.open(encoding='utf-8') as source:
            config = json.load(source)
    except FileNotFoundError:
        config = {'base_dir': ['./']}
    if not isinstance(config, dict):
        raise ValueError('global_config.json 必须是 JSON 对象')
    port = config.get('base-port', DEFAULT_PORT)
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError('base-port 必须是 1～65535 的整数')
    roots = config.get('base_dir', [])
    if not isinstance(roots, list) or any(not isinstance(p, str) for p in roots):
        raise ValueError('base_dir 必须是路径字符串列表')
    http_config = config.get('http-url-config', {})
    if not isinstance(http_config, dict):
        raise ValueError('http-url-config 必须是 JSON 对象')
    max_bytes = http_config.get('max-blob-bytes', 104857600)
    if type(max_bytes) is not int or max_bytes < 0:
        raise ValueError('max-blob-bytes 必须是非负整数')
    if type(http_config.get('tmp-blob-file', False)) is not bool:
        raise ValueError('tmp-blob-file 必须是布尔值')
    timeout = config.get('connect_timeout', 1200)
    if type(timeout) is not int or timeout <= 0:
        raise ValueError('connect_timeout 必须是正整数（秒）')
    needs_write = any(key not in config for key in ('base-port', 'http-url-config', 'connect_timeout'))
    config.setdefault('connect_timeout', timeout)
    config.setdefault('base-port', port)
    config.setdefault('http-url-config', {
        'max-blob-bytes': 104857600,
        'tmp-blob-file': False,
    })
    if needs_write:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w', encoding='utf-8', dir=path.parent,
                prefix=path.name + '.', suffix='.tmp', delete=False,
            ) as target:
                temporary_path = target.name
                json.dump(config, target, ensure_ascii=False, indent=4)
                target.write('\n')
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary_path, path)
        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)
    return config


class ModuleManager:
    def __init__(self, global_config, backend_dir=SCRIPT_DIR):
        self.modules = {}
        config_path = Path(backend_dir) / 'config' / 'module_config.json'
        if not config_path.is_file():
            LOGGER.info('模块配置文件不存在，未加载任何模块')
            return
        with config_path.open(encoding='utf-8') as source:
            module_config = json.load(source)
        # 保持现有的模块导入名称和配置顺序。
        if str(backend_dir) not in sys.path:
            sys.path.insert(0, str(backend_dir))
        for name, info in module_config.get('modules', {}).items():
            if not info.get('enabled', True):
                continue
            try:
                module = importlib.import_module(info['path'].replace('/', '.'))
                instance = module.create_module(global_config, BACK_VERSION)
                if not instance:
                    LOGGER.warning('模块 %s 拒绝加载当前后端版本', name)
                    continue
                self.modules[name] = instance
                LOGGER.info('成功加载模块: %s', name)
            except Exception:
                LOGGER.exception('加载模块 %s 失败', name)

    def get_module(self, name):
        return self.modules.get(name)


async def dispatch_connection(connection, app, handshake=True):
    """一个连接只有一个业务消费者，保持模块回调和消息的顺序。"""
    modules = app[MODULES_KEY]
    try:
        if handshake:
            # 首条消息声明支持的前端版本，必须早于路径和模块初始化消息。
            await connection.send(json.dumps({
                'command': 'BACKEND_VERSION',
                'version': [BACK_VERSION],
            }, ensure_ascii=False))
            await connection.send(json.dumps({
                'command': 'SET_NOWPATH',
                'path': '; '.join(app[CONFIG_KEY]['base_dir']),
            }, ensure_ascii=False))
        for module in modules.values():
            await module.when_connect(connection)
        while True:
            response = await connection.recv()
            try:
                data = json.loads(response)
                if not isinstance(data, dict):
                    raise ValueError('消息必须是 JSON 对象')
            except (ValueError, UnicodeError, TypeError):
                await connection.close(code=WSCloseCode.INVALID_TEXT,
                                       reason='Expected a JSON object')
                return
            for module in modules.values():
                await module.message_proc(connection, data)
    except ConnectionError:
        pass
    except Exception:
        LOGGER.exception('处理客户端消息失败')
        await connection.close(code=WSCloseCode.INTERNAL_ERROR,
                               reason='Module callback failed')


async def receive_messages(connection, transport=None):
    # 独立读取网络，以便业务协程等待时仍处理心跳与关闭帧。
    transport = transport if transport is not None else connection.websocket
    async for message in transport:
        if transport is not connection.websocket:
            return
        if message.type == WSMsgType.TEXT:
            if connection.connection_id:
                try:
                    command = json.loads(message.data)
                except (ValueError, TypeError):
                    command = None
                if isinstance(command, dict) and command.get('command') == 'HEARTBEAT_PING':
                    sequence = command.get('sequence')
                    if type(sequence) is int and 0 <= sequence <= 9007199254740991:
                        await asyncio.wait_for(transport.send_json({
                            'command': 'HEARTBEAT_PONG', 'sequence': sequence}), timeout=3)
                    continue
                if isinstance(command, dict) and command.get('command') == 'SESSION_DISCONNECT':
                    connection.ended = True
                    return
            try:
                connection.incoming.put_nowait(message.data)
            except asyncio.QueueFull:
                await connection.close(code=WSCloseCode.TRY_AGAIN_LATER,
                                       reason='Too many pending messages')
                return
        elif message.type == WSMsgType.BINARY:
            # 现有上行协议仅接受 JSON；HSV1 是后端到前端的原样二进制数据。
            await connection.close(code=WSCloseCode.UNSUPPORTED_DATA,
                                   reason='Client messages must be JSON text')
            return
        elif message.type == WSMsgType.ERROR:
            return


async def end_session(app, connection):
    """仅逻辑会话结束时清理模块及 blob；允许重复调用。"""
    if app[SESSIONS_KEY].get(connection.connection_id) is not connection:
        return
    del app[SESSIONS_KEY][connection.connection_id]
    connection.ended = True
    connection.attached.set()
    if connection.expiry and connection.expiry is not asyncio.current_task():
        connection.expiry.cancel()
    if connection.worker:
        connection.worker.cancel()
        await asyncio.gather(connection.worker, return_exceptions=True)
    for name, module in app[MODULES_KEY].items():
        try:
            await asyncio.wait_for(module.when_disconnect(connection), 5)
        except Exception:
            LOGGER.exception('模块 %s 会话清理失败', name)
    connection._clear_blob_urls()
    await connection.close()
    app[CONNECTIONS_KEY].discard(connection)


async def expire_session(app, connection):
    await asyncio.sleep(app[CONFIG_KEY]['connect_timeout'])
    await end_session(app, connection)


async def resumable_websocket(request, connection_id):
    app = request.app
    connection = app[SESSIONS_KEY].get(connection_id)
    websocket = web.WebSocketResponse(heartbeat=30, timeout=5, max_msg_size=1024 * 1024, compress=False)
    fresh = connection is None
    if fresh:
        connection = FrameworkConnection(websocket, resources=app[RESOURCES_KEY],
            resource_origin=str(request.url.origin()), blobs=app[BLOBS_KEY])
        connection.connection_id = connection_id
        connection.attached.clear()
        app[SESSIONS_KEY][connection_id] = connection
        app[CONNECTIONS_KEY].add(connection)
    current = asyncio.current_task()
    app[HANDLERS_KEY].add(current)
    reader = None
    old_close = None
    try:
        # 握手串行；完成前原传输仍可用。切换后旧 handler 不得清理当前会话。
        async with connection.attach_lock:
            if connection.ended or app[STOPPING_KEY].is_set():
                raise web.HTTPGone(text='Session ended')
            await websocket.prepare(request)
            await websocket.send_json({'command': 'BACKEND_VERSION', 'version': [BACK_VERSION],
                                       'connectionId': connection_id, 'resumed': connection.worker is not None})
            await websocket.send_json({'command': 'SET_NOWPATH', 'path': '; '.join(app[CONFIG_KEY]['base_dir'])})
            if connection.ended:
                return websocket
            old = connection.websocket
            connection.websocket = websocket
            if connection.expiry:
                connection.expiry.cancel()
                connection.expiry = None
            connection.attached.set()
            if connection.worker is None:
                connection.worker = asyncio.create_task(dispatch_connection(connection, app, handshake=False))
            if old is not websocket:
                old_close = asyncio.create_task(old.close(code=1000, message=b'Transport replaced'))
            reader = asyncio.create_task(receive_messages(connection, websocket))
        await asyncio.wait((reader, connection.worker), return_when=asyncio.FIRST_COMPLETED)
    finally:
        if reader:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        # 所有会话状态变更必须在关闭 await 之前完成，防止迟到 finally 清理新传输。
        if connection.websocket is websocket:
            connection.attached.clear()
            if connection.ended or app[STOPPING_KEY].is_set() or (connection.worker and connection.worker.done()):
                await end_session(app, connection)
            elif connection.worker is None:
                await end_session(app, connection)
            else:
                connection.expiry = asyncio.create_task(expire_session(app, connection))
        await websocket.close()
        if old_close:
            await asyncio.gather(old_close, return_exceptions=True)
        app[HANDLERS_KEY].discard(current)
    return websocket


async def websocket_handler(request):
    """处理 GET /ws：完成升级握手，并管理一个客户端连接的完整生命周期。

    aiohttp 在路由匹配 /ws 时调用本协程，request 是本次 HTTP 请求对象。
    握手成功后，本协程持续运行至连接结束；每个客户端有自己的协程实例。
    本函数负责连接接入、任务协调和清理，具体消息由 dispatch_connection
    按模块顺序分发，不在这里处理功能模块的业务逻辑。
    """
    # 1. 从 aiohttp 的 Request 获取所属 Application，读取框架共享的运行状态。
    #    停机标记已设置时，抛出 aiohttp HTTP 异常会返回 503，拒绝升级连接。
    app = request.app
    if app[STOPPING_KEY].is_set():
        raise web.HTTPServiceUnavailable()
    connection_id = request.query.get('connectionId')
    if connection_id is not None:
        try:
            connection_id = str(uuid.UUID(connection_id))
        except ValueError:
            raise web.HTTPBadRequest(text='Invalid connectionId')
        return await resumable_websocket(request, connection_id)
    # 2. 创建 aiohttp WebSocket 响应对象。此时还没有发送握手响应。
    #    heartbeat：启用 Ping/Pong 存活检测，参数为 30 秒，收到数据会重置计时。
    #    timeout：关闭握手的等待超时（5 秒），不是普通消息接收超时。
    #    max_msg_size：单条接收消息最多 1 MiB，超过限制由 aiohttp 关闭连接。
    #    默认写缓冲水位为 64 KiB，达到水位时发送会等待缓冲排空；
    #                  它用于背压，不是单条发送消息的大小上限。
    #    compress=False：不协商 WebSocket 消息压缩。
    #    默认 autoping/autoclose 保持启用，由 aiohttp 响应 Ping 和关闭帧。
    websocket = web.WebSocketResponse(
        heartbeat=30, timeout=5, max_msg_size=1024 * 1024,
        compress=False,
    )
    # 3. 校验客户端的 Upgrade 等握手字段，成功时发送 HTTP 101 并升级为
    #    WebSocket；普通 HTTP 请求或无效握手由 aiohttp 返回相应错误响应。
    #    prepare 完成后使用 send_str/send_bytes/receive 等 WebSocket API，
    #    不再向此响应写普通 HTTP 正文。握手失败不会进入下面的连接登记流程。
    await websocket.prepare(request)

    # 4. 包装为模块使用的连接接口，登记连接和当前处理协程。
    #    服务退出时据此找到所有连接，并等待各连接的清理流程完成。
    connection = FrameworkConnection(
        websocket, resources=app[RESOURCES_KEY],
        resource_origin=str(request.url.origin()),
        blobs=app[BLOBS_KEY],
    )
    current = asyncio.current_task()
    app[CONNECTIONS_KEY].add(connection)
    app[HANDLERS_KEY].add(current)
    # 5. 启动两个 asyncio 任务：reader 唯一读取 aiohttp WebSocket，将文本
    #    放入有界队列；worker 消费队列，初始化模块并依次分发业务消息。
    #    这样模块异步等待时 reader 仍能处理 Ping/Pong 和断线；同步阻塞代码
    #    仍会阻塞事件循环，create_task 并不会把工作移到另一个线程。
    reader = asyncio.create_task(receive_messages(connection))
    worker = asyncio.create_task(dispatch_connection(connection, app))
    try:
        # 6. 握手 prepare 中发生过 await，期间服务可能开始停机，因此再检查
        #    一次。GOING_AWAY 是 WebSocket 标准关闭码 1001，表示服务即将离开。
        if app[STOPPING_KEY].is_set():
            await connection.close(code=WSCloseCode.GOING_AWAY)

        # 7. 从这里同时启动 reader 和 worker 任务，等待任一任务结束。
        #  也就是正式启动连接处理流程。这里是该连接的主循环。
        #   任一任务结束即进入清理：网络断线或业务处理结束都意味着该连接
        #    无需继续工作。FIRST_COMPLETED 本身不会取消另一个任务。
        await asyncio.wait((reader, worker), return_when=asyncio.FIRST_COMPLETED)
    finally:
        # 8. 无论正常结束、异常还是本协程被取消，都取消剩余任务并等待退出。
        #    return_exceptions=True 收集任务异常/取消结果，避免其中一个任务
        #    的异常跳过后面的断线清理。
        for task in (reader, worker):
            if not task.done():
                task.cancel()
        await asyncio.gather(reader, worker, return_exceptions=True)
        try:
            # 9. 按模块顺序通知断线，各模块负责清理属于该连接的业务资源。
            #    wait_for 超时会请求取消回调；单个回调失败不阻止其他模块清理。
            for name, module in app[MODULES_KEY].items():
                try:
                    await asyncio.wait_for(module.when_disconnect(connection), timeout=5)
                except Exception:
                    LOGGER.exception('模块 %s 断线清理失败', name)
            # 10. 适配层最终调用 aiohttp WebSocketResponse.close()，完成关闭。
            #     aiohttp 允许对已关闭的连接再次调用 close，故可作为统一收尾。
            await connection.close()
        finally:
            # 11. 即使关闭操作失败，也移除登记，避免共享集合残留失效连接/任务。
            app[CONNECTIONS_KEY].discard(connection)
            connection._clear_blob_urls()
            app[HANDLERS_KEY].discard(current)
    # 12. 将已经处理完的 WebSocketResponse 返回给 aiohttp，结束路由处理。
    #     握手响应已在 prepare 中发送，这里不会再发送一份普通 HTTP 响应。
    return websocket


async def stop_connections(app):
    app[STOPPING_KEY].set()
    await asyncio.gather(*(
        connection.close(code=WSCloseCode.GOING_AWAY, reason='Server shutdown')
        for connection in tuple(app[CONNECTIONS_KEY])
    ), return_exceptions=True)
    if app[HANDLERS_KEY]:
        await asyncio.gather(*tuple(app[HANDLERS_KEY]), return_exceptions=True)
    for connection in tuple(app[SESSIONS_KEY].values()):
        await end_session(app, connection)


async def shutdown_modules(app):
    for name, module in app[MODULES_KEY].items():
        shutdown = getattr(module, 'shutdown', None)
        if shutdown:
            try:
                await asyncio.wait_for(shutdown(), timeout=10)
            except Exception:
                LOGGER.exception('模块 %s 关闭清理失败', name)


def create_app(config_path=None, *, modules=None, backend_dir=SCRIPT_DIR):
    """构建应用；导入本文件本身不会修改配置、初始化模块或监听端口。"""
    backend_dir = Path(backend_dir)
    config = load_global_config(config_path or backend_dir / 'config' / 'global_config.json')
    roots = []
    resource_roots = []
    for root in config.get('base_dir', []):
        real_path = os.path.abspath(os.path.join(backend_dir, root))
        resource_roots.append(real_path)
        if os.path.exists(real_path):
            roots.append(real_path)
        else:
            LOGGER.warning('根目录不存在: %s', real_path)
    config = {**config, 'base_dir': roots}
    if modules is None:
        modules = ModuleManager(config, backend_dir).modules
    app = web.Application(middlewares=[resource_cors])

    #把 aiohttp 应用对象当作共享状态容器使用
    app[CONFIG_KEY] = config
    app[MODULES_KEY] = modules
    app[CONNECTIONS_KEY] = set()
    app[SESSIONS_KEY] = {}
    http_config = config.get('http-url-config', {})
    app[BLOBS_KEY] = BlobStore(http_config.get('max-blob-bytes', 104857600),
                              http_config.get('tmp-blob-file', False),
                              backend_dir / 'classify_shower' / 'temp')
    app[HANDLERS_KEY] = set()
    app[STOPPING_KEY] = asyncio.Event()
    # 应用自身及全局配置不作为资源发布，即使 base_dir 指向其上级目录。
    # 使用未过滤的配置列表编号，避免不存在的目录导致后续 rootId 前移。
    app[RESOURCES_KEY] = ResourceRegistry(resource_roots, protected_paths=(
        SCRIPT_DIR.parent, backend_dir,
        Path(config_path or backend_dir / 'config' / 'global_config.json'),
    ))
    app.router.add_get('/ws', websocket_handler, allow_head=False)
    app.router.add_get('/api/server-info', server_info_handler)
    app.router.add_get('/blobs/{token}/{data_name}', blob_handler)
    app.router.add_get('/resources/{root_id}/{relative_path:.*}', resource_handler)
    app.on_response_prepare.append(prepare_resource_headers)
    app.on_shutdown.append(stop_connections)
    app.on_cleanup.append(shutdown_modules)
    return app


async def main():
    app = create_app()
    port = app[CONFIG_KEY]['base-port']
    runner = web.AppRunner(app, shutdown_timeout=10)
    try:
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', port).start()
        LOGGER.info('后端已启动: 0.0.0.0:%s，WebSocket 路径 /ws', port)
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        LOGGER.exception('后端启动或运行失败')
        sys.exit(1)
