"""
Tests for database models and functionality.
"""

import os
import sys
import tempfile

# Add ks-game to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ks-game"))

import chess
import pytest

from kriegspiel_wrapper import ExtendedBerkeleyGame

from models import (
    db,
    Game,
    initialize_database,
    close_database,
    get_game_by_id,
    get_game_history,
    save_game_state,
    save_move_history,
    reconstruct_game_from_history,
    serialize_game_to_json,
    deserialize_game_from_json,
    save_game_with_serialization,
    load_game_from_serialization,
)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    # Create a temporary file for the test database
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db_path = temp_file.name
    temp_file.close()

    # Update the database path
    global db
    db.init(temp_db_path)

    # Initialize the database
    initialize_database()

    yield db

    # Cleanup
    close_database()
    os.unlink(temp_db_path)


def test_create_game(temp_db):
    """Test creating a new game in the database."""
    game_id = "test-game-123"
    board_fen = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

    game = save_game_state(
        game_id=game_id,
        board_fen=board_fen,
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    assert game.game_id == game_id
    assert game.board_fen == board_fen
    assert game.current_turn == "white"
    assert game.is_game_over is False
    assert game.move_count == 0
    assert game.any_rule is True  # default value


def test_get_game_by_id(temp_db):
    """Test retrieving a game by ID."""
    game_id = "test-game-456"
    board_fen = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1"

    # Create game
    save_game_state(
        game_id=game_id,
        board_fen=board_fen,
        current_turn="black",
        is_game_over=False,
        move_count=1,
    )

    # Retrieve game
    game = get_game_by_id(game_id)
    assert game is not None
    assert game.game_id == game_id
    assert game.board_fen == board_fen
    assert game.current_turn == "black"
    assert game.move_count == 1


def test_get_nonexistent_game(temp_db):
    """Test retrieving a game that doesn't exist."""
    game = get_game_by_id("nonexistent-game")
    assert game is None


def test_save_move_history(temp_db):
    """Test saving move history."""
    game_id = "test-game-789"

    # Create a game first
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Save a move
    move = save_move_history(
        game_id=game_id,
        move_number=1,
        player="white",
        question_type="COMMON",
        move_uci="e2e4",
        board_fen_before="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        board_fen_after="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        main_announcement="REGULAR_MOVE",
        is_legal=True,
    )

    assert move.move_number == 1
    assert move.player == "white"
    assert move.question_type == "COMMON"
    assert move.move_uci == "e2e4"
    assert move.main_announcement == "REGULAR_MOVE"
    assert move.is_legal is True


def test_get_game_history(temp_db):
    """Test retrieving game history."""
    game_id = "test-game-history"

    # Create a game
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Add some moves
    save_move_history(
        game_id=game_id,
        move_number=1,
        player="white",
        question_type="COMMON",
        move_uci="e2e4",
        board_fen_before="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        board_fen_after="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        main_announcement="REGULAR_MOVE",
        is_legal=True,
    )

    save_move_history(
        game_id=game_id,
        move_number=2,
        player="black",
        question_type="COMMON",
        move_uci="e7e5",
        board_fen_before="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        board_fen_after="rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2",
        main_announcement="REGULAR_MOVE",
        is_legal=True,
    )

    # Get history
    history = get_game_history(game_id)
    assert len(history) == 2
    assert history[0].move_number == 1
    assert history[0].player == "white"
    assert history[0].move_uci == "e2e4"
    assert history[1].move_number == 2
    assert history[1].player == "black"
    assert history[1].move_uci == "e7e5"


def test_save_ask_any_move(temp_db):
    """Test saving an ASK_ANY move."""
    game_id = "test-ask-any"

    # Create a game
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Save an ASK_ANY move
    move = save_move_history(
        game_id=game_id,
        move_number=1,
        player="white",
        question_type="ASK_ANY",
        move_uci=None,  # No move for ASK_ANY
        board_fen_before="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        board_fen_after="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",  # Same board
        main_announcement="NO_ANY",
        is_legal=False,  # ASK_ANY doesn't move pieces
        has_any=False,
    )

    assert move.question_type == "ASK_ANY"
    assert move.move_uci is None
    assert move.main_announcement == "NO_ANY"
    assert move.has_any is False


def test_update_existing_game(temp_db):
    """Test updating an existing game state."""
    game_id = "test-update-game"

    # Create initial game
    game1 = save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Update the same game
    game2 = save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        current_turn="black",
        is_game_over=False,
        move_count=1,
    )

    # Should be the same database record
    assert game1.id == game2.id
    assert game2.move_count == 1
    assert game2.current_turn == "black"

    # Verify only one game exists in database
    all_games = Game.select().where(Game.game_id == game_id)
    assert len(all_games) == 1


def test_reconstruct_game_from_history(temp_db):
    """Test reconstructing a game from move history."""

    game_id = "test-reconstruction"

    # Create a game with some moves
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Add some moves to history
    save_move_history(
        game_id=game_id,
        move_number=1,
        player="white",
        question_type="COMMON",
        move_uci="e2e4",
        board_fen_before="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        board_fen_after="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        main_announcement="REGULAR_MOVE",
        is_legal=True,
    )

    save_move_history(
        game_id=game_id,
        move_number=2,
        player="black",
        question_type="COMMON",
        move_uci="e7e5",
        board_fen_before="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        board_fen_after="rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2",
        main_announcement="REGULAR_MOVE",
        is_legal=True,
    )

    # Reconstruct the game
    reconstructed_game = reconstruct_game_from_history(game_id)

    # Verify the game was reconstructed correctly
    assert reconstructed_game is not None
    assert reconstructed_game.turn == chess.WHITE  # Should be white's turn after 2 moves

    # The reconstructed board should match the final state
    final_fen = reconstructed_game._game._board.fen()
    assert "4p3/4P3" in final_fen  # Should have pawns on e4 and e5


def test_reconstruct_empty_game(temp_db):
    """Test reconstructing a game with no moves."""

    game_id = "test-empty-reconstruction"

    # Create a game with no moves
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Reconstruct the game (should work even with no moves)
    reconstructed_game = reconstruct_game_from_history(game_id)

    # Should be a fresh game
    assert reconstructed_game is not None
    assert reconstructed_game.turn == chess.WHITE
    initial_fen = reconstructed_game._game._board.fen()
    assert initial_fen.startswith("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR")


def test_reconstruct_nonexistent_game(temp_db):
    """Test reconstructing a game that doesn't exist."""

    try:
        reconstruct_game_from_history("nonexistent-game")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "not found" in str(e)


def test_serialize_game_to_json(temp_db):
    """Test serializing a game to JSON string."""
    # Create a simple game
    game = ExtendedBerkeleyGame(any_rule=True)

    # Serialize to JSON
    json_str = serialize_game_to_json(game)

    # Verify it's valid JSON and contains expected fields
    import json

    game_data = json.loads(json_str)

    assert "version" in game_data
    assert "game_type" in game_data
    assert game_data["game_type"] == "BerkeleyGame"
    assert "game_state" in game_data
    assert "any_rule" in game_data["game_state"]
    assert game_data["game_state"]["any_rule"] is True
    assert "board_fen" in game_data["game_state"]


def test_deserialize_game_from_json(temp_db):
    """Test deserializing a game from JSON string."""
    # Create and serialize a game
    original_game = ExtendedBerkeleyGame(any_rule=True)
    json_str = serialize_game_to_json(original_game)

    # Deserialize it back
    restored_game = deserialize_game_from_json(json_str)

    # Verify the restored game matches the original
    assert isinstance(restored_game, ExtendedBerkeleyGame)
    assert restored_game._game._any_rule == original_game._game._any_rule
    assert restored_game._game._board.fen() == original_game._game._board.fen()
    assert restored_game.turn == original_game.turn
    assert restored_game.game_over == original_game.game_over


def test_serialize_deserialize_game_with_moves(temp_db):
    """Test serializing and deserializing a game with moves made."""
    from kriegspiel.move import KriegspielMove, QuestionAnnouncement

    # Create a game and make some moves
    game = ExtendedBerkeleyGame(any_rule=True)

    # Make a move: e2e4
    move1 = KriegspielMove(QuestionAnnouncement.COMMON, chess.Move.from_uci("e2e4"))
    answer1 = game.ask_for(move1)
    assert answer1.move_done

    # Make another move: e7e5
    move2 = KriegspielMove(QuestionAnnouncement.COMMON, chess.Move.from_uci("e7e5"))
    answer2 = game.ask_for(move2)
    assert answer2.move_done

    # Serialize and deserialize
    json_str = serialize_game_to_json(game)
    restored_game = deserialize_game_from_json(json_str)

    # Verify the game state is preserved
    assert restored_game._game._board.fen() == game._game._board.fen()
    assert restored_game.turn == game.turn
    assert restored_game._game._board.fullmove_number == game._game._board.fullmove_number

    # Verify scoresheets are preserved
    assert len(restored_game._game._whites_scoresheet._KriegspielScoresheet__moves_own) == 1
    assert len(restored_game._game._blacks_scoresheet._KriegspielScoresheet__moves_own) == 1


def test_save_game_with_serialization(temp_db):
    """Test saving a game using JSON serialization."""
    game_id = "test-serialization-save"
    game = ExtendedBerkeleyGame(any_rule=True)

    # Save the game
    db_game = save_game_with_serialization(game_id, game)

    # Verify database record was created
    assert db_game.game_id == game_id
    assert db_game.game_state_json is not None
    assert db_game.board_fen == game._game._board.fen()
    assert db_game.current_turn == "white"
    assert db_game.is_game_over is False
    assert db_game.move_count == 1  # Initial position is move 1

    # Verify the JSON can be deserialized
    restored_game = deserialize_game_from_json(db_game.game_state_json)
    assert restored_game._game._board.fen() == game._game._board.fen()


def test_load_game_from_serialization(temp_db):
    """Test loading a game from JSON serialization."""
    game_id = "test-serialization-load"
    original_game = ExtendedBerkeleyGame(any_rule=False)  # Test with any_rule=False

    # Save the game
    save_game_with_serialization(game_id, original_game)

    # Load the game back
    loaded_game = load_game_from_serialization(game_id)

    # Verify the loaded game matches the original
    assert loaded_game is not None
    assert isinstance(loaded_game, ExtendedBerkeleyGame)
    assert loaded_game._game._any_rule == original_game._game._any_rule
    assert loaded_game._game._board.fen() == original_game._game._board.fen()
    assert loaded_game.turn == original_game.turn


def test_load_game_from_serialization_nonexistent(temp_db):
    """Test loading a game that doesn't exist returns None."""
    result = load_game_from_serialization("nonexistent-game")
    assert result is None


def test_load_game_from_serialization_no_json(temp_db):
    """Test loading a game with no JSON data returns None."""
    game_id = "test-no-json"

    # Save a game without JSON (old style)
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    # Try to load - should return None because no JSON data
    result = load_game_from_serialization(game_id)
    assert result is None


def test_serialization_preserves_game_state_after_moves(temp_db):
    """Test that serialization preserves complex game state."""
    from kriegspiel.move import KriegspielMove, QuestionAnnouncement

    game_id = "test-complex-serialization"
    game = ExtendedBerkeleyGame(any_rule=True)

    # Make several moves to create complex state
    moves = ["e2e4", "e7e5", "g1f3", "b8c6", "f1c4", "f8c5"]

    for move_uci in moves:
        move = KriegspielMove(QuestionAnnouncement.COMMON, chess.Move.from_uci(move_uci))
        answer = game.ask_for(move)
        assert answer.move_done

    # Save and reload
    save_game_with_serialization(game_id, game)
    loaded_game = load_game_from_serialization(game_id)

    # Verify complex state is preserved
    assert loaded_game is not None
    assert loaded_game._game._board.fen() == game._game._board.fen()
    assert loaded_game.turn == game.turn
    assert loaded_game._game._board.fullmove_number == game._game._board.fullmove_number

    # Make a move on both games and verify they behave the same
    test_move = KriegspielMove(QuestionAnnouncement.COMMON, chess.Move.from_uci("d2d4"))

    answer_original = game.ask_for(test_move)
    answer_loaded = loaded_game.ask_for(test_move)

    assert answer_original.main_announcement == answer_loaded.main_announcement
    assert answer_original.move_done == answer_loaded.move_done
