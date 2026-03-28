from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_SETTINGS_FILENAME = 'trainer_settings.json'
MINIMUM_TRAINING_DEPTH = 2
CONSERVATIVE_FALLBACK_MAX_DEPTH = 5


@dataclass(frozen=True)
class TrainerSettings:
    good_moves_acceptable: bool = True
    active_training_ply_depth: int = CONSERVATIVE_FALLBACK_MAX_DEPTH
    smart_profile_enabled: bool = True
    side_panel_visible: bool = False
    move_list_visible: bool = True
    last_bundle_path: str | None = None
    last_corpus_catalog_root: str | None = None

    def normalized(self, *, maximum_depth: int | None = None) -> 'TrainerSettings':
        effective_maximum = maximum_depth if maximum_depth is not None else max(self.active_training_ply_depth, CONSERVATIVE_FALLBACK_MAX_DEPTH)
        clamped_depth = max(MINIMUM_TRAINING_DEPTH, min(int(self.active_training_ply_depth), int(effective_maximum)))
        bundle_path = str(self.last_bundle_path).strip() if self.last_bundle_path is not None and str(self.last_bundle_path).strip() else None
        catalog_root = str(self.last_corpus_catalog_root).strip() if self.last_corpus_catalog_root is not None and str(self.last_corpus_catalog_root).strip() else None
        return TrainerSettings(
            good_moves_acceptable=bool(self.good_moves_acceptable),
            active_training_ply_depth=clamped_depth,
            smart_profile_enabled=bool(self.smart_profile_enabled),
            side_panel_visible=bool(self.side_panel_visible),
            move_list_visible=bool(self.move_list_visible),
            last_bundle_path=bundle_path,
            last_corpus_catalog_root=catalog_root,
        )


class TrainerSettingsStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / DEFAULT_SETTINGS_FILENAME

    def load(self, *, maximum_depth: int | None = None) -> TrainerSettings:
        if not self.path.exists():
            return TrainerSettings().normalized(maximum_depth=maximum_depth)
        try:
            payload = json.loads(self.path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return TrainerSettings().normalized(maximum_depth=maximum_depth)
        settings = TrainerSettings(
            good_moves_acceptable=bool(payload.get('good_moves_acceptable', True)),
            active_training_ply_depth=int(payload.get('active_training_ply_depth', CONSERVATIVE_FALLBACK_MAX_DEPTH)),
            smart_profile_enabled=bool(payload.get('smart_profile_enabled', True)),
            side_panel_visible=bool(payload.get('side_panel_visible', False)),
            move_list_visible=bool(payload.get('move_list_visible', True)),
            last_bundle_path=payload.get('last_bundle_path') or None,
            last_corpus_catalog_root=payload.get('last_corpus_catalog_root') or None,
        )
        normalized = settings.normalized(maximum_depth=maximum_depth)
        if normalized != settings:
            self.save(normalized)
        return normalized

    def save(self, settings: TrainerSettings, *, maximum_depth: int | None = None) -> TrainerSettings:
        normalized = settings.normalized(maximum_depth=maximum_depth)
        self.path.write_text(json.dumps(asdict(normalized), indent=2), encoding='utf-8')
        return normalized
