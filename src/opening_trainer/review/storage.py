from __future__ import annotations

import json
import shutil
from pathlib import Path

from .models import ProfileMeta, ReviewItem, TrainerStats, utc_now_iso


class ReviewStorage:
    def __init__(self, root: Path | str = 'runtime/profiles'):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.root / 'profiles_registry.json'
        if not self.registry_path.exists():
            default = ProfileMeta('default', 'Default', utc_now_iso(), utc_now_iso(), is_default=True)
            self.registry_path.write_text(json.dumps({'active_profile_id': 'default', 'profiles': [default.to_dict()]}, indent=2), encoding='utf-8')
            self._ensure_profile_files(default.profile_id, default)

    def _ensure_profile_files(self, profile_id: str, meta: ProfileMeta | None = None) -> Path:
        profile_dir = self.root / profile_id
        profile_dir.mkdir(parents=True, exist_ok=True)
        if meta is not None:
            (profile_dir / 'profile_meta.json').write_text(json.dumps(meta.to_dict(), indent=2), encoding='utf-8')
        for name, payload in [('review_items.json', []), ('trainer_stats.json', TrainerStats().to_dict())]:
            path = profile_dir / name
            if not path.exists():
                path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
        history = profile_dir / 'session_history.jsonl'
        history.touch(exist_ok=True)
        return profile_dir

    def load_registry(self) -> dict:
        return json.loads(self.registry_path.read_text(encoding='utf-8'))

    def save_registry(self, registry: dict) -> None:
        self.registry_path.write_text(json.dumps(registry, indent=2), encoding='utf-8')

    def list_profiles(self) -> list[ProfileMeta]:
        return [ProfileMeta(**p) for p in self.load_registry()['profiles']]

    def get_active_profile_id(self) -> str:
        return self.load_registry()['active_profile_id']

    def set_active_profile(self, profile_id: str) -> None:
        registry = self.load_registry()
        registry['active_profile_id'] = profile_id
        self.save_registry(registry)

    def create_profile(self, display_name: str) -> ProfileMeta:
        registry = self.load_registry()
        profile_id = display_name.lower().replace(' ', '_')
        now = utc_now_iso()
        meta = ProfileMeta(profile_id, display_name, now, now, is_default=False)
        registry['profiles'].append(meta.to_dict())
        self.save_registry(registry)
        self._ensure_profile_files(profile_id, meta)
        return meta

    def delete_profile(self, profile_id: str) -> None:
        registry = self.load_registry()
        registry['profiles'] = [p for p in registry['profiles'] if p['profile_id'] != profile_id]
        if registry['active_profile_id'] == profile_id:
            registry['active_profile_id'] = 'default'
        self.save_registry(registry)
        shutil.rmtree(self.root / profile_id, ignore_errors=True)

    def reset_profile(self, profile_id: str) -> None:
        profile_dir = self._ensure_profile_files(profile_id)
        (profile_dir / 'review_items.json').write_text('[]', encoding='utf-8')
        (profile_dir / 'trainer_stats.json').write_text(json.dumps(TrainerStats().to_dict(), indent=2), encoding='utf-8')
        (profile_dir / 'session_history.jsonl').write_text('', encoding='utf-8')

    def load_profile_meta(self, profile_id: str) -> ProfileMeta:
        return ProfileMeta(**json.loads((self.root / profile_id / 'profile_meta.json').read_text(encoding='utf-8')))

    def load_items(self, profile_id: str) -> list[ReviewItem]:
        self._ensure_profile_files(profile_id)
        payload = json.loads((self.root / profile_id / 'review_items.json').read_text(encoding='utf-8'))
        return [ReviewItem.from_dict(row) for row in payload]

    def save_items(self, profile_id: str, items: list[ReviewItem]) -> None:
        (self.root / profile_id / 'review_items.json').write_text(json.dumps([item.to_dict() for item in items], indent=2), encoding='utf-8')

    def load_stats(self, profile_id: str) -> TrainerStats:
        self._ensure_profile_files(profile_id)
        return TrainerStats.from_dict(json.loads((self.root / profile_id / 'trainer_stats.json').read_text(encoding='utf-8')))

    def save_stats(self, profile_id: str, stats: TrainerStats) -> None:
        (self.root / profile_id / 'trainer_stats.json').write_text(json.dumps(stats.to_dict(), indent=2), encoding='utf-8')

    def append_history(self, profile_id: str, event: dict) -> None:
        with (self.root / profile_id / 'session_history.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(event) + '\n')
