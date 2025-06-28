"""
Tests for database models and functionality.
"""

import pytest
import os
import tempfile
import chess
from models import *


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    # Create a temporary file for the test database
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
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
        move_count=0
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
        move_count=1
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
        move_count=0
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
        is_legal=True
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
        move_count=0
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
        is_legal=True
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
        is_legal=True
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
        move_count=0
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
        has_any=False
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
        move_count=0
    )
    
    # Update the same game
    game2 = save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
        current_turn="black",
        is_game_over=False,
        move_count=1
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
    from models import reconstruct_game_from_history
    
    game_id = "test-reconstruction"
    
    # Create a game with some moves
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0
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
        is_legal=True
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
        is_legal=True
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
    from models import reconstruct_game_from_history
    
    game_id = "test-empty-reconstruction"
    
    # Create a game with no moves
    save_game_state(
        game_id=game_id,
        board_fen="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
        current_turn="white",
        is_game_over=False,
        move_count=0
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
    from models import reconstruct_game_from_history
    
    try:
        reconstruct_game_from_history("nonexistent-game")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "not found" in str(e)