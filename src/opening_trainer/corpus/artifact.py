from __future__ import annotations

import json
from pathlib import Path

from .models import CandidateMoveRecord, CorpusArtifact, PositionRecord


def save_artifact(artifact: CorpusArtifact, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = artifact.to_dict()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path



def load_artifact(path: str | Path) -> CorpusArtifact:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return CorpusArtifact(
        schema_version=payload["schema_version"],
        source_files=tuple(payload["source_files"]),
        target_rating_band=dict(payload["target_rating_band"]),
        rating_policy=payload["rating_policy"],
        retained_ply_depth=payload["retained_ply_depth"],
        sparse_policy=dict(payload["sparse_policy"]),
        weighting_policy=dict(payload["weighting_policy"]),
        positions=tuple(
            PositionRecord(
                position_key=position["position_key"],
                side_to_move=position["side_to_move"],
                total_observed_count=position["total_observed_count"],
                sparse=position["sparse"],
                sparse_reason=position.get("sparse_reason"),
                fallback_position_key=position.get("fallback_position_key"),
                candidate_moves=tuple(
                    CandidateMoveRecord(
                        uci=move["uci"],
                        raw_count=move["raw_count"],
                        effective_weight=move["effective_weight"],
                    )
                    for move in position["candidate_moves"]
                ),
            )
            for position in payload["positions"]
        ),
    )
