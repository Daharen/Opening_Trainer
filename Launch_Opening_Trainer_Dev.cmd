@echo off
setlocal
set REPO_ROOT=%~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%REPO_ROOT%run.ps1" -Action Menu
endlocal
