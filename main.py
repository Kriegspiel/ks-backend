import os
import sys
import uuid
from typing import Optional

# Add ks-game to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ks-game"))

import chess
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from kriegspiel.move import KriegspielMove, QuestionAnnouncement

from kriegspiel_wrapper import ExtendedBerkeleyGame
from models import initialize_database, close_database, GameHistory
from models import (
    save_game_state,
    save_move_history,
    get_game_by_id,
    get_game_history as get_game_history_from_db,
    reconstruct_game_from_history,
)

app = FastAPI(title="Kriegspiel Chess API", description="API for playing Kriegspiel chess")

# Initialize database
initialize_database()

# Add CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
    ],  # Vite and CRA default ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

games = {}


@app.on_event("shutdown")
def shutdown_event():
    """Close database connection on app shutdown."""
    close_database()


# Mount the frontend static files
frontend_dist_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.exists(frontend_dist_path):
    # Mount static assets at root level for proper asset loading
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_dist_path, "assets")),
        name="assets",
    )
    # Mount the main app
    app.mount("/app", StaticFiles(directory=frontend_dist_path, html=True), name="frontend")


@app.get("/")
async def root():
    return {"message": "Kriegspiel Chess API"}


@app.post("/games")
async def create_game(any_rule: bool = True):
    game_id = str(uuid.uuid4())
    game_engine = ExtendedBerkeleyGame(any_rule=any_rule)
    games[game_id] = game_engine

    # Save initial game state to database
    initial_board_fen = game_engine._game._board.fen()
    save_game_state(
        game_id=game_id,
        board_fen=initial_board_fen,
        current_turn="white",
        is_game_over=False,
        move_count=0,
    )

    return {"game_id": game_id, "status": "created", "any_rule": any_rule}


@app.get("/games/{game_id}")
async def get_game_state(game_id: str, player: str):
    # Try to get game from memory first
    if game_id not in games:
        # Try to load from database and reconstruct
        db_game = get_game_by_id(game_id)
        if not db_game:
            raise HTTPException(status_code=404, detail="Game not found")

        try:
            # Reconstruct game from database history
            reconstructed_game = reconstruct_game_from_history(game_id)
            games[game_id] = reconstructed_game
            print(f"Successfully reconstructed game {game_id} from database")
        except Exception as e:
            print(f"Failed to reconstruct game {game_id}: {e}")
            raise HTTPException(status_code=503, detail="Game reconstruction failed")

    game = games[game_id]
    if player not in ["white", "black"]:
        raise HTTPException(status_code=400, detail="Player must be 'white' or 'black'")

    color = chess.WHITE if player == "white" else chess.BLACK
    visible_board = game.get_visible_board(color)

    return {
        "game_id": game_id,
        "player": player,
        "visible_board": str(visible_board),
        "board_fen": visible_board.fen(),
        "turn": "white" if game.turn == chess.WHITE else "black",
        "is_game_over": game.game_over,
    }


@app.post("/games/{game_id}/move")
async def make_move(
    game_id: str,
    player: str,
    move_uci: Optional[str] = None,
    question_type: str = "COMMON",
):
    # Ensure game is loaded in memory (reconstruct if needed)
    if game_id not in games:
        db_game = get_game_by_id(game_id)
        if not db_game:
            raise HTTPException(status_code=404, detail="Game not found")

        try:
            # Reconstruct game from database history
            reconstructed_game = reconstruct_game_from_history(game_id)
            games[game_id] = reconstructed_game
            print(f"Successfully reconstructed game {game_id} from database for move")
        except Exception as e:
            print(f"Failed to reconstruct game {game_id}: {e}")
            raise HTTPException(status_code=503, detail="Game reconstruction failed")

    game = games[game_id]
    if player not in ["white", "black"]:
        raise HTTPException(status_code=400, detail="Player must be 'white' or 'black'")

    color = chess.WHITE if player == "white" else chess.BLACK

    if game.turn != color:
        raise HTTPException(status_code=400, detail="Not your turn")

    try:
        question = QuestionAnnouncement.COMMON if question_type == "COMMON" else QuestionAnnouncement.ASK_ANY

        if question_type == "ASK_ANY":
            # For ASK_ANY questions, move should be None
            ks_move = KriegspielMove(question, None)
        else:
            # For COMMON questions, move is required
            if move_uci is None:
                raise HTTPException(status_code=400, detail="move_uci is required for COMMON questions")
            move = chess.Move.from_uci(move_uci)
            ks_move = KriegspielMove(question, move)

        # Save board state before move
        board_fen_before = game._game._board.fen()

        answer = game.ask_for(ks_move)

        # Get updated visible board after move
        color = chess.WHITE if player == "white" else chess.BLACK
        visible_board = game.get_visible_board(color)
        board_fen_after = game._game._board.fen()

        # Save move to database
        try:
            save_move_history(
                game_id=game_id,
                move_number=game._game._board.fullmove_number,
                player=player,
                question_type=question_type,
                move_uci=move_uci,
                board_fen_before=board_fen_before,
                board_fen_after=board_fen_after if answer.move_done else None,
                main_announcement=answer.main_announcement.name,
                special_announcement=answer.special_announcement.name if answer.special_announcement else None,
                capture_square=chess.square_name(answer.capture_at_square) if answer.capture_at_square is not None else None,
                is_legal=answer.move_done,
                has_any=(answer.main_announcement.name == "HAS_ANY") if question_type == "ASK_ANY" else None,
            )

            # Update game state in database
            save_game_state(
                game_id=game_id,
                board_fen=board_fen_after,
                current_turn="white" if game.turn == chess.WHITE else "black",
                is_game_over=game.game_over,
                move_count=game._game._board.fullmove_number,
            )
        except Exception as db_error:
            # Log the error but don't fail the API call
            print(f"Database error: {db_error}")  # In production, use proper logging

        # Build response based on question type
        response = {
            "game_id": game_id,
            "player": player,
            "announcement": answer.main_announcement.name if answer.main_announcement else None,
            "special_case": answer.special_announcement.name if answer.special_announcement else None,
            "visible_board": str(visible_board),
            "board_fen": visible_board.fen(),
            "is_game_over": game.game_over,
        }

        if question_type == "ASK_ANY":
            # For ASK_ANY, return has_any instead of legal/move
            response["has_any"] = answer.main_announcement.name == "HAS_ANY"
        else:
            # For COMMON moves, return move and legal status
            response["move"] = move_uci
            response["legal"] = answer.move_done

        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid move: {str(e)}")


@app.get("/games/{game_id}/history")
async def get_game_history(game_id: str):
    """Get the complete move history for a game."""
    history = get_game_history_from_db(game_id)
    if not history:
        # Check if game exists
        db_game = get_game_by_id(game_id)
        if not db_game:
            raise HTTPException(status_code=404, detail="Game not found")
        return {"game_id": game_id, "history": []}

    # Convert history to JSON-serializable format
    history_data = []
    for entry in history:
        history_data.append(
            {
                "move_number": entry.move_number,
                "player": entry.player,
                "question_type": entry.question_type,
                "move_uci": entry.move_uci,
                "main_announcement": entry.main_announcement,
                "special_announcement": entry.special_announcement,
                "capture_square": entry.capture_square,
                "is_legal": entry.is_legal,
                "has_any": entry.has_any,
                "timestamp": entry.timestamp.isoformat(),
            }
        )

    return {"game_id": game_id, "history": history_data}


@app.delete("/games/{game_id}")
async def delete_game(game_id: str):
    # Check if game exists in memory or database
    game_in_memory = game_id in games
    db_game = get_game_by_id(game_id)

    if not game_in_memory and not db_game:
        raise HTTPException(status_code=404, detail="Game not found")

    # Remove from memory if present
    if game_in_memory:
        del games[game_id]

    # Remove from database if present
    if db_game:
        # Delete game history first (due to foreign key constraint)
        GameHistory.delete().where(GameHistory.game == db_game).execute()
        # Delete the game
        db_game.delete_instance()

    return {"message": "Game deleted"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
