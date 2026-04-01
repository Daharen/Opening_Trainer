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
OutputDir=dist
OutputBaseFilename=OpeningTrainerSetup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=admin
UninstallDisplayIcon={app}\{#MyAppExeName}
SetupLogging=yes

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

[Files]
Source: "..\dist\consumer\*"; DestDir: "{app}\bootstrap_payload"; Flags: recursesubdirs ignoreversion
Source: "..\dist\consumer_app_payload\OpeningTrainer-app.zip"; DestDir: "{app}\installer"; Flags: ignoreversion
Source: "consumer_content_manifest.json"; DestDir: "{app}\installer"; Flags: ignoreversion
Source: "app_update_manifest.json"; DestDir: "{app}\installer"; Flags: ignoreversion
Source: "scripts\install_consumer_content.ps1"; DestDir: "{app}\installer"; Flags: ignoreversion
Source: "scripts\install_consumer_app.ps1"; DestDir: "{app}\installer"; Flags: ignoreversion
Source: "scripts\apply_app_update.ps1"; DestDir: "{app}\installer"; Flags: ignoreversion

[Icons]
Name: "{group}\Opening Trainer"; Filename: "{localappdata}\OpeningTrainer\App\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"; WorkingDir: "{localappdata}\OpeningTrainer\App"
Name: "{autodesktop}\Opening Trainer"; Filename: "{localappdata}\OpeningTrainer\App\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"; WorkingDir: "{localappdata}\OpeningTrainer\App"; Tasks: desktopicon

[Run]
Filename: "powershell.exe"; \
    Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\installer\install_consumer_app.ps1"" -BootstrapRoot ""{app}\bootstrap_payload"" -AppStateRoot ""{localappdata}\OpeningTrainer"" -DefaultAppRoot ""{localappdata}\OpeningTrainer\App"" -Channel ""dev"" -AppVersion ""{#MyAppVersion}"" -BuildId ""bootstrap-{#MyAppVersion}"" -PayloadFilename ""OpeningTrainer-app.zip"" -DefaultManifestUrl ""https://raw.githubusercontent.com/daharen/Opening_Trainer/main/installer/app_update_manifest.json"" -UpdaterHelperScriptPath ""{app}\installer\apply_app_update.ps1"" -ContentRoot ""{localappdata}\OpeningTrainerContent"" -LogPath ""{localappdata}\OpeningTrainer\install_consumer_app.log"""; \
    StatusMsg: "Installing Opening Trainer app payload..."; \
    Flags: waituntilterminated runasoriginaluser
Filename: "powershell.exe"; \
    Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\installer\install_consumer_content.ps1"" -ManifestPath ""{app}\installer\consumer_content_manifest.json"" -AppStateRoot ""{localappdata}\OpeningTrainer"" -ContentRoot ""{localappdata}\OpeningTrainerContent"" -LogPath ""{localappdata}\OpeningTrainer\install_consumer_content.log"""; \
    StatusMsg: "Installing Opening Trainer content..."; \
    Flags: waituntilterminated runasoriginaluser
Filename: "{localappdata}\OpeningTrainer\App\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"; WorkingDir: "{localappdata}\OpeningTrainer\App"; Description: "Launch Opening Trainer"; Flags: nowait postinstall skipifsilent runasoriginaluser

[Code]

procedure AppendMissingIfAbsent(var Missing: String; const LabelName: String; const FilePath: String);
begin
  if not FileExists(FilePath) then
  begin
    if Missing <> '' then
      Missing := Missing + #13#10;
    Missing := Missing + LabelName + ': ' + FilePath;
  end;
end;

function VerifyPerUserProvisioning(var FailureDetail: String): Boolean;
var
  AppStateRoot: String;
  MutableRoot: String;
  Missing: String;
begin
  Result := False;
  Missing := '';
  AppStateRoot := ExpandConstant('{localappdata}\OpeningTrainer');
  MutableRoot := AppStateRoot + '\App';

  AppendMissingIfAbsent(Missing, 'installed_app_manifest', AppStateRoot + '\installed_app_manifest.json');
  AppendMissingIfAbsent(Missing, 'app_state_updater_helper', AppStateRoot + '\updater\apply_app_update.ps1');
  AppendMissingIfAbsent(Missing, 'app_state_updater_config', AppStateRoot + '\updater\updater_config.json');
  AppendMissingIfAbsent(Missing, 'app_install_log', AppStateRoot + '\install_consumer_app.log');
  AppendMissingIfAbsent(Missing, 'content_install_log', AppStateRoot + '\install_consumer_content.log');
  AppendMissingIfAbsent(Missing, 'mutable_executable', MutableRoot + '\{#MyAppExeName}');
  AppendMissingIfAbsent(Missing, 'mutable_payload_identity', MutableRoot + '\payload_identity.json');
  AppendMissingIfAbsent(Missing, 'mutable_updater_helper', MutableRoot + '\updater\apply_app_update.ps1');

  if Missing <> '' then
  begin
    FailureDetail :=
      'Program Files bootstrap payload was installed, but per-user LocalAppData provisioning failed for the current interactive user.' + #13#10 +
      'Missing required files under ' + AppStateRoot + ':' + #13#10 + Missing;
    Exit;
  end;

  Result := True;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  FailureDetail: String;
begin
  if CurStep = ssPostInstall then
  begin
    if not VerifyPerUserProvisioning(FailureDetail) then
    begin
      MsgBox(FailureDetail, mbCriticalError, MB_OK);
      RaiseException(FailureDetail);
    end;
  end;
end;

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
