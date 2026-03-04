@echo off
setlocal
set PORT=%1
if "%PORT%"=="" set PORT=8000

echo Starting Trade UI on http://127.0.0.1:%PORT%
set PORT=%PORT%
python trade_ui_server.py
