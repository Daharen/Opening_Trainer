# Windows Consumer Installer Baseline

This folder contains the consumer installer lane for Opening Trainer.

## One-command packaging flow

### 1) Build consumer payload

Run:

```powershell
.\installer\scripts\build_consumer_payload.ps1
```

This script:

- ensures PyInstaller is available
- builds from the repo entrypoint using `installer/packaging/opening_trainer_consumer.spec`
- cleans stale build outputs deterministically
- emits `dist/consumer/OpeningTrainer.exe`

### 2) Build installer

Run:

```powershell
.\installer\scripts\build_consumer_installer.ps1
```

This script:

- ensures/produces the payload
- validates `installer/consumer_content_manifest.json`
- compiles `installer/opening_trainer_installer.iss` via `ISCC.exe`
- emits `installer/dist/OpeningTrainerSetup.exe`

### 3) Publish a dev-channel app update (no installer rebuild)

Run:

```powershell
.\installer\scripts\publish_dev_update.ps1
```

This script rebuilds the app payload zip, computes SHA-256, updates `installer/app_update_manifest.json` (including `build_id`), and copies the payload into `installer/payloads/dev/`.

## Consumer content bootstrap

During install, the wizard runs `installer/scripts/install_consumer_content.ps1` visibly. The bootstrap:

1. Reads `installer/consumer_content_manifest.json`.
2. Downloads the content archive from `download_url` with live progress.
3. Optionally verifies SHA-256 when `sha256` is provided.
4. Extracts content to `%LocalAppData%\OpeningTrainerContent`.
5. Handles one optional wrapper directory in the archive layout.
6. Writes `%LocalAppData%\OpeningTrainer\runtime.consumer.json`.
7. Writes bootstrap logs to `%LocalAppData%\OpeningTrainer\install.log`.

The installer fails clearly if download, checksum, extraction, or required-content validation fails.

## Uninstall behavior

Uninstall removes:

- app binaries and shortcuts (Inno Setup default behavior)
- `%LocalAppData%\OpeningTrainer` automatically

It also prompts the user whether to remove `%LocalAppData%\OpeningTrainerContent`.
