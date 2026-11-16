@echo off
cd /d "%~dp0"
echo Starting RTMP server...
python --version >nul 2>nul
if %errorlevel%==0 (
  python server.py
) else (
  py server.py
)
pause
