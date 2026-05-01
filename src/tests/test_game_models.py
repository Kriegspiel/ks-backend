from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.models.game import (
    CreateGameRequest,
    CreateGameResponse,
    GameDocument,
    GameMetadataResponse,
    JoinGameResponse,
    MaterialSideSummary,
    MaterialSummary,
    OpenGamesResponse,
    ReserveSideSummary,
    ReserveSummary,
)


@pytest.mark.parametrize("state", ["waiting", "active", "completed"])
def test_game_document_accepts_step_300_states_only(state: str) -> None:
    now = datetime.now(UTC)
    doc = GameDocument.model_validate(
        {
            "_id": "664b2c",
            "game_code": "A7K2M9",
            "rule_variant": "berkeley_any",
            "white": {"user_id": "u1", "username": "alexfil", "connected": True},
            "black": {"user_id": "u2", "username": "opponent1", "connected": True},
            "state": state,
            "turn": "white",
            "move_number": 12,
            "created_at": now,
            "updated_at": now,
        }
    )

    assert doc.state == state


def test_game_document_rejects_out_of_scope_states() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        GameDocument.model_validate(
            {
                "_id": "664b2c",
                "game_code": "A7K2M9",
                "rule_variant": "berkeley_any",
                "white": {"user_id": "u1", "username": "alexfil", "connected": True},
                "state": "paused",
                "created_at": now,
                "updated_at": now,
            }
        )


def test_game_document_from_mongo_converts_id_to_string() -> None:
    now = datetime.now(UTC)
    payload = {
        "_id": 12345,
        "game_code": "A7K2M9",
        "white": {"user_id": "u1", "username": "alexfil", "connected": True},
        "state": "waiting",
        "created_at": now,
        "updated_at": now,
    }

    doc = GameDocument.from_mongo(payload)

    assert doc.id == "12345"


def test_game_document_from_mongo_allows_documents_without_id() -> None:
    now = datetime.now(UTC)

    doc = GameDocument.from_mongo(
        {
            "game_code": "A7K2M9",
            "white": {"user_id": "u1", "username": "alexfil", "connected": True},
            "state": "waiting",
            "created_at": now,
            "updated_at": now,
        }
    )

    assert doc.id is None


def test_create_game_request_defaults() -> None:
    req = CreateGameRequest.model_validate({})

    assert req.rule_variant == "berkeley_any"
    assert req.play_as == "random"
    assert req.time_control == "rapid"


@pytest.mark.parametrize("rule_variant", ["berkeley", "berkeley_any", "cincinnati", "wild16", "rand", "english", "crazykrieg"])
def test_create_game_request_accepts_supported_rule_variants(rule_variant: str) -> None:
    req = CreateGameRequest(rule_variant=rule_variant)

    assert req.rule_variant == rule_variant


def test_create_game_request_validates_bot_fields() -> None:
    with pytest.raises(ValidationError, match="bot_id is required"):
        CreateGameRequest(opponent_type="bot")

    with pytest.raises(ValidationError, match="only allowed when opponent_type is bot"):
        CreateGameRequest(opponent_type="human", bot_id="507f1f77bcf86cd799439011")


def test_create_game_response_contract_shape() -> None:
    response = CreateGameResponse.model_validate(
        {
            "game_id": "664b2c",
            "game_code": "A7K2M9",
            "play_as": "white",
            "rule_variant": "berkeley_any",
            "state": "waiting",
            "join_url": "https://kriegspiel.org/join/A7K2M9",
        }
    )

    assert response.model_dump() == {
        "game_id": "664b2c",
        "game_code": "A7K2M9",
        "play_as": "white",
        "rule_variant": "berkeley_any",
        "state": "waiting",
        "join_url": "https://kriegspiel.org/join/A7K2M9",
        "game_url": None,
        "opponent_type": "human",
        "bot": None,
    }


def test_join_game_response_contract_shape() -> None:
    response = JoinGameResponse.model_validate(
        {
            "game_id": "664b2c",
            "game_code": "A7K2M9",
            "play_as": "black",
            "rule_variant": "berkeley_any",
            "state": "active",
            "game_url": "https://kriegspiel.org/game/664b2c",
        }
    )

    assert response.state == "active"
    assert response.play_as == "black"


def test_material_summary_contract_shape() -> None:
    response = MaterialSummary.model_validate(
        {
            "white": {"pieces_remaining": 15, "pawns_captured": 1},
            "black": {"pieces_remaining": 16, "pawns_captured": None},
        }
    )

    assert response.white == MaterialSideSummary(pieces_remaining=15, pawns_captured=1)
    assert response.black == MaterialSideSummary(pieces_remaining=16, pawns_captured=None)


def test_material_summary_rejects_impossible_counts() -> None:
    with pytest.raises(ValidationError):
        MaterialSummary.model_validate(
            {
                "white": {"pieces_remaining": 17, "pawns_captured": 0},
                "black": {"pieces_remaining": 16, "pawns_captured": 9},
            }
        )


def test_reserve_summary_contract_shape() -> None:
    response = ReserveSummary.model_validate(
        {
            "white": {"pawns": 2, "knights": 1, "bishops": 0, "rooks": 0, "queens": 0},
            "black": {"pawns": 0, "knights": 0, "bishops": 1, "rooks": 1, "queens": 0},
        }
    )

    assert response.white == ReserveSideSummary(pawns=2, knights=1, bishops=0, rooks=0, queens=0)
    assert response.black == ReserveSideSummary(pawns=0, knights=0, bishops=1, rooks=1, queens=0)


def test_open_games_response_contract_shape() -> None:
    now = datetime(2026, 3, 13, 20, 0, tzinfo=UTC)
    response = OpenGamesResponse.model_validate(
        {
            "games": [
                {
                    "game_code": "A7K2M9",
                    "rule_variant": "berkeley_any",
                    "created_by": "alexfil",
                    "created_at": now,
                    "available_color": "black",
                }
            ]
        }
    )

    assert response.games[0].available_color == "black"


def test_game_metadata_response_contract_shape() -> None:
    now = datetime(2026, 3, 13, 20, 0, tzinfo=UTC)
    response = GameMetadataResponse.model_validate(
        {
            "game_id": "664b2c",
            "game_code": "A7K2M9",
            "rule_variant": "berkeley_any",
            "state": "active",
            "white": {"username": "alexfil", "connected": True},
            "black": {"username": "opponent1", "connected": True},
            "turn": "white",
            "move_number": 12,
            "created_at": now,
            "updated_at": now,
            "result": {"winner": "white", "reason": "checkmate"},
            "rating_snapshot": {"overall": {"white_before": 1200, "white_after": 1216}},
        }
    )

    assert response.white.username == "alexfil"
    assert response.black and response.black.username == "opponent1"
    assert response.result == {"winner": "white", "reason": "checkmate"}


def test_game_metadata_response_allows_guest_player_role() -> None:
    now = datetime(2026, 4, 28, 10, 47, tzinfo=UTC)
    response = GameMetadataResponse.model_validate(
        {
            "game_id": "664b2c",
            "game_code": "A7K2M9",
            "rule_variant": "berkeley_any",
            "state": "active",
            "white": {"username": "randobot", "connected": True, "role": "bot"},
            "black": {"username": "guest_adolf_adams", "connected": True, "role": "guest"},
            "turn": "white",
            "move_number": 3,
            "created_at": now,
            "updated_at": now,
        }
    )

    assert response.black and response.black.role == "guest"
