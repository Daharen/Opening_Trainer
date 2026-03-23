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
    side_panel_visible: bool = True

    def normalized(self, *, maximum_depth: int | None = None) -> 'TrainerSettings':
        effective_maximum = maximum_depth if maximum_depth is not None else max(self.active_training_ply_depth, CONSERVATIVE_FALLBACK_MAX_DEPTH)
        clamped_depth = max(MINIMUM_TRAINING_DEPTH, min(int(self.active_training_ply_depth), int(effective_maximum)))
        return TrainerSettings(
            good_moves_acceptable=bool(self.good_moves_acceptable),
            active_training_ply_depth=clamped_depth,
            side_panel_visible=bool(self.side_panel_visible),
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
            side_panel_visible=bool(payload.get('side_panel_visible', True)),
        )
        normalized = settings.normalized(maximum_depth=maximum_depth)
        if normalized != settings:
            self.save(normalized)
        return normalized

    def save(self, settings: TrainerSettings, *, maximum_depth: int | None = None) -> TrainerSettings:
        normalized = settings.normalized(maximum_depth=maximum_depth)
        self.path.write_text(json.dumps(asdict(normalized), indent=2), encoding='utf-8')
        return normalized
