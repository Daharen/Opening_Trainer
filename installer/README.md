# Windows Consumer Installer Baseline

This folder contains the first consumer installer lane for Opening Trainer.

## Inputs

The Inno Setup script expects a prebuilt consumer app payload at:

- `dist/consumer/`

At minimum that payload must include `OpeningTrainer.exe` and everything it needs to launch.

## Build the installer

1. Install [Inno Setup 6](https://jrsoftware.org/isinfo.php).
2. Build or copy the consumer app payload into `dist/consumer/`.
3. Compile `installer/opening_trainer_installer.iss` with Inno Setup.

Example (ISCC on PATH):

```powershell
ISCC.exe installer\opening_trainer_installer.iss
```

Output artifact:

- `installer/dist/OpeningTrainerSetup.exe`

## Consumer content bootstrap

During install, the wizard runs `installer/scripts/install_consumer_content.ps1` which:

1. Reads `installer/consumer_content_manifest.json`.
2. Downloads the content archive from `download_url`.
3. Optionally verifies SHA-256 when `sha256` is provided.
4. Extracts content to `%LocalAppData%\OpeningTrainerContent`.
5. Writes `%LocalAppData%\OpeningTrainer\runtime.consumer.json`.

The installer fails clearly if download/extract/validation fails.

## Uninstall behavior

Uninstall removes:

- app binaries and shortcuts (Inno Setup default behavior)
- `%LocalAppData%\OpeningTrainer` automatically

It also prompts the user whether to remove `%LocalAppData%\OpeningTrainerContent`.
