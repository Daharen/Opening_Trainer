#define MyAppName "Opening Trainer"
#define MyAppVersion "0.1.0"
#define MyAppPublisher "Opening Trainer"
#define MyAppExeName "OpeningTrainer.exe"

[Setup]
AppId={{88DAAB2D-10A2-4027-AE53-2BDF249A1902}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\Opening Trainer
DefaultGroupName=Opening Trainer
OutputDir=installer\dist
OutputBaseFilename=OpeningTrainerSetup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=admin
UninstallDisplayIcon={app}\{#MyAppExeName}

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

[Files]
Source: "dist\consumer\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion
Source: "installer\consumer_content_manifest.json"; DestDir: "{app}\installer"; Flags: ignoreversion
Source: "installer\scripts\install_consumer_content.ps1"; DestDir: "{app}\installer"; Flags: ignoreversion

[Icons]
Name: "{group}\Opening Trainer"; Filename: "{app}\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"
Name: "{autodesktop}\Opening Trainer"; Filename: "{app}\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"; Tasks: desktopicon

[Run]
Filename: "powershell.exe"; \
    Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\installer\install_consumer_content.ps1"" -ManifestPath ""{app}\installer\consumer_content_manifest.json"" -AppStateRoot ""{localappdata}\OpeningTrainer"" -ContentRoot ""{localappdata}\OpeningTrainerContent"""; \
    StatusMsg: "Downloading and installing Opening Trainer content..."; \
    Flags: waituntilterminated runhidden
Filename: "{app}\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"; Description: "Launch Opening Trainer"; Flags: nowait postinstall skipifsilent

[Code]
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  RemoveContent: Integer;
begin
  if CurUninstallStep = usUninstall then
  begin
    DelTree(ExpandConstant('{localappdata}\\OpeningTrainer'), True, True, True);
    RemoveContent := MsgBox(
      'Remove downloaded opening content in %LocalAppData%\\OpeningTrainerContent as well?',
      mbConfirmation,
      MB_YESNO
    );
    if RemoveContent = IDYES then
      DelTree(ExpandConstant('{localappdata}\\OpeningTrainerContent'), True, True, True);
  end;
end;
