from __future__ import annotations

from .types import AuthoritySource, BookAuthorityResult, CanonicalJudgment, EngineAuthorityResult


def resolve_canonical_judgment(
    book_result: BookAuthorityResult,
    engine_result: EngineAuthorityResult,
) -> tuple[bool, CanonicalJudgment, AuthoritySource]:
    if book_result.accepted:
        return True, CanonicalJudgment.BOOK, AuthoritySource.BOOK

    if engine_result.accepted:
        return True, CanonicalJudgment.BETTER, AuthoritySource.ENGINE

    if not engine_result.available:
        return False, CanonicalJudgment.AUTHORITY_UNAVAILABLE, AuthoritySource.ENGINE

    return False, CanonicalJudgment.FAIL, AuthoritySource.NONE
