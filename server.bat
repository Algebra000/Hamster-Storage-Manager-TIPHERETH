@echo off
setlocal DisableDelayedExpansion
pushd "%~dp0"
if errorlevel 1 exit /b 1

rem Use system Python, with the same launcher fallback as install.bat.
set "PYTHON_EXE="
for /f "delims=" %%P in ('python -c "import sys; print(sys.executable) if sys.version_info.major == 3 else sys.exit(1)" 2^>nul') do set "PYTHON_EXE=%%P"
if defined PYTHON_EXE goto python_ready
for /f "delims=" %%P in ('py -3 -c "import sys; print(sys.executable)" 2^>nul') do set "PYTHON_EXE=%%P"
if defined PYTHON_EXE goto python_ready

echo Python 3 was not found. Please run install.bat first.
popd
pause
exit /b 1

:python_ready
echo Starting Hamster Storage Manager backend...
echo Press Ctrl+C to stop the service.
"%PYTHON_EXE%" "%~dp0backend\back.py"
set "SERVER_EXIT_CODE=%ERRORLEVEL%"
if not "%SERVER_EXIT_CODE%"=="0" (
    echo Backend stopped with an error. Please check the output above.
    pause
)
popd
exit /b %SERVER_EXIT_CODE%
