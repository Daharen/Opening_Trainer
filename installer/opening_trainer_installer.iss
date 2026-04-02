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
Filename: "{localappdata}\OpeningTrainer\App\{#MyAppExeName}"; Parameters: "--runtime-mode consumer"; WorkingDir: "{localappdata}\OpeningTrainer\App"; Description: "Launch Opening Trainer"; Flags: nowait postinstall skipifsilent runasoriginaluser; Check: ShouldLaunchPostinstallApp

[Code]
var
  PerUserProvisioningSucceeded: Boolean;

{ IMPORTANT: Validate [Code] compatibility by running a real ISCC build via
  installer/scripts/build_consumer_installer.ps1. Do not assume Delphi helpers
  are available in Inno Pascal unless compilation confirms it. }
function BoolText(const Value: Boolean): String;
begin
  if Value then
    Result := 'True'
  else
    Result := 'False';
end;

function EscapePowerShellArg(const Value: String): String;
var
  Escaped: String;
begin
  Escaped := Value;
  StringChangeEx(Escaped, '"', '""', True);
  Result := Escaped;
end;

procedure AppendMissingIfAbsent(var Missing: String; const LabelName: String; const FilePath: String);
begin
  if not FileExists(FilePath) then
  begin
    if Missing <> '' then
      Missing := Missing + #13#10;
    Missing := Missing + LabelName + ': ' + FilePath;
  end;
end;

function VerifyRequiredAppScaffold(var FailureDetail: String): Boolean;
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
  AppendMissingIfAbsent(Missing, 'mutable_executable', MutableRoot + '\{#MyAppExeName}');
  AppendMissingIfAbsent(Missing, 'mutable_payload_identity', MutableRoot + '\payload_identity.json');
  AppendMissingIfAbsent(Missing, 'mutable_updater_helper', MutableRoot + '\updater\apply_app_update.ps1');

  if Missing <> '' then
  begin
    FailureDetail :=
      'Program Files bootstrap payload was installed, but per-user APP provisioning outputs are missing for the current interactive user.' + #13#10 +
      'Missing required files under ' + AppStateRoot + ':' + #13#10 + Missing;
    Exit;
  end;

  Result := True;
end;

function VerifyRequiredContentScaffold(var FailureDetail: String): Boolean;
var
  AppStateRoot: String;
  Missing: String;
begin
  Result := False;
  Missing := '';
  AppStateRoot := ExpandConstant('{localappdata}\OpeningTrainer');

  AppendMissingIfAbsent(Missing, 'content_runtime_config', AppStateRoot + '\runtime.consumer.json');
  AppendMissingIfAbsent(Missing, 'installed_content_manifest', AppStateRoot + '\installed_content_manifest.json');

  if Missing <> '' then
  begin
    FailureDetail :=
      'Program Files bootstrap payload and APP provisioning succeeded, but content provisioning outputs are missing.' + #13#10 +
      'Missing required files under ' + AppStateRoot + ':' + #13#10 + Missing;
    Exit;
  end;

  Result := True;
end;

function RunProvisioningScriptAsOriginalUser(
  const ScriptName: String;
  const Parameters: String;
  const FailureLogHint: String;
  var FailureDetail: String
): Boolean;
var
  ScriptPath: String;
  PowerShellPath: String;
  ResultCode: Integer;
  ResolvedParams: String;
  LaunchOk: Boolean;
begin
  Result := False;
  ScriptPath := ExpandConstant('{app}\installer\' + ScriptName);
  PowerShellPath := ExpandConstant('{sys}\WindowsPowerShell\v1.0\powershell.exe');
  ResolvedParams := '-NoProfile -ExecutionPolicy Bypass -File "' + EscapePowerShellArg(ScriptPath) + '" ' + Parameters;

  Log('PER_USER_PROVISIONING script=' + ScriptName + ' powershell=' + PowerShellPath);
  Log('PER_USER_PROVISIONING resolved_script_path=' + ScriptPath);
  Log('PER_USER_PROVISIONING localappdata=' + ExpandConstant('{localappdata}'));
  Log('PER_USER_PROVISIONING app_state_root=' + ExpandConstant('{localappdata}\OpeningTrainer'));
  Log('PER_USER_PROVISIONING content_root=' + ExpandConstant('{localappdata}\OpeningTrainerContent'));
  Log('PER_USER_PROVISIONING command_line=' + PowerShellPath + ' ' + ResolvedParams);

  LaunchOk := ExecAsOriginalUser(
    PowerShellPath,
    ResolvedParams,
    ExpandConstant('{app}\installer'),
    SW_SHOWNORMAL,
    ewWaitUntilTerminated,
    ResultCode
  );

  Log(Format('PER_USER_PROVISIONING exec_result script=%s launched=%s result_code=%d', [ScriptName, BoolText(LaunchOk), ResultCode]));

  if not LaunchOk then
  begin
    FailureDetail :=
      'Failed to launch per-user provisioning script ' + ScriptName + ' as the original interactive user.' + #13#10 +
      'System error: ' + SysErrorMessage(ResultCode) + #13#10 +
      'See installer log and provisioning log: ' + FailureLogHint;
    Exit;
  end;

  if ResultCode <> 0 then
  begin
    FailureDetail :=
      'Per-user provisioning script ' + ScriptName + ' exited with code ' + IntToStr(ResultCode) + '.' + #13#10 +
      'See provisioning log: ' + FailureLogHint;
    Exit;
  end;

  Result := True;
end;

procedure RunAuthoritativePerUserProvisioning;
var
  FailureDetail: String;
  AppParams: String;
  ContentParams: String;
begin
  FailureDetail := '';

  AppParams :=
    '-BootstrapRoot "' + EscapePowerShellArg(ExpandConstant('{app}\bootstrap_payload')) + '" ' +
    '-AppStateRoot "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainer')) + '" ' +
    '-DefaultAppRoot "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainer\App')) + '" ' +
    '-Channel "dev" ' +
    '-AppVersion "{#MyAppVersion}" ' +
    '-BuildId "bootstrap-{#MyAppVersion}" ' +
    '-PayloadFilename "OpeningTrainer-app.zip" ' +
    '-DefaultManifestUrl "https://raw.githubusercontent.com/daharen/Opening_Trainer/main/installer/app_update_manifest.json" ' +
    '-UpdaterHelperScriptPath "' + EscapePowerShellArg(ExpandConstant('{app}\installer\apply_app_update.ps1')) + '" ' +
    '-ContentRoot "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainerContent')) + '" ' +
    '-LogPath "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainer\install_consumer_app.log')) + '"';

  if not RunProvisioningScriptAsOriginalUser(
    'install_consumer_app.ps1',
    AppParams,
    ExpandConstant('{localappdata}\OpeningTrainer\install_consumer_app.log'),
    FailureDetail
  ) then
    RaiseException(FailureDetail);

  if not VerifyRequiredAppScaffold(FailureDetail) then
    RaiseException(FailureDetail);

  ContentParams :=
    '-ManifestPath "' + EscapePowerShellArg(ExpandConstant('{app}\installer\consumer_content_manifest.json')) + '" ' +
    '-AppStateRoot "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainer')) + '" ' +
    '-ContentRoot "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainerContent')) + '" ' +
    '-LogPath "' + EscapePowerShellArg(ExpandConstant('{localappdata}\OpeningTrainer\install_consumer_content.log')) + '"';

  if not RunProvisioningScriptAsOriginalUser(
    'install_consumer_content.ps1',
    ContentParams,
    ExpandConstant('{localappdata}\OpeningTrainer\install_consumer_content.log'),
    FailureDetail
  ) then
    RaiseException(FailureDetail);

  if not VerifyRequiredContentScaffold(FailureDetail) then
    RaiseException(FailureDetail);

  PerUserProvisioningSucceeded := True;
  Log('PER_USER_PROVISIONING status=success app_and_content_verified=true');
end;

function ShouldLaunchPostinstallApp: Boolean;
begin
  Result := PerUserProvisioningSucceeded;
  Log(Format('POSTINSTALL_LAUNCH check=%s', [BoolText(Result)]));
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    PerUserProvisioningSucceeded := False;
    RunAuthoritativePerUserProvisioning;
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  RemoveContent: Integer;
begin
  if CurUninstallStep = usUninstall then
  begin
    DelTree(ExpandConstant('{localappdata}\OpeningTrainer'), True, True, True);
    RemoveContent := MsgBox(
      'Remove downloaded opening content in %LocalAppData%\OpeningTrainerContent as well?',
      mbConfirmation,
      MB_YESNO
    );
    if RemoveContent = IDYES then
      DelTree(ExpandConstant('{localappdata}\OpeningTrainerContent'), True, True, True);
  end;
end;
