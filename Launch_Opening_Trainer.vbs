Set shell = CreateObject("WScript.Shell")
repoRoot = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File """ & repoRoot & "\run.ps1"" -Action Auto"
shell.Run cmd, 0, False
