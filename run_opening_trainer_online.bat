@echo off
setlocal
set SCRIPT_DIR=%~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run_opening_trainer_online.ps1" %*
endlocal
