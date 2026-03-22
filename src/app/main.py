import os
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import Settings, get_settings

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
FRONTEND_DIST_PATH = os.path.join(BASE_DIR, "frontend", "dist")


def build_cors_origins(settings: Settings) -> list[str]:
    origins = [settings.SITE_ORIGIN, "http://localhost:5173", "http://localhost:3000"]
    if settings.ENVIRONMENT == "development":
        origins.append("http://localhost:8000")

    deduped: list[str] = []
    for origin in origins:
        if origin not in deduped:
            deduped.append(origin)
    return deduped


@asynccontextmanager
async def lifespan(app: FastAPI):
    close_database = None
    try:
        from models import close_database as _close_database, initialize_database

        close_database = _close_database
        initialize_database()
    except ModuleNotFoundError:
        # Allow factory/health checks to run when optional game deps are unavailable.
        pass

    try:
        yield
    finally:
        if close_database is not None:
            close_database()


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings if settings is not None else get_settings()
    app = FastAPI(title="Kriegspiel Chess API", description="API for playing Kriegspiel chess", lifespan=lifespan)
    app.state.settings = resolved_settings

    app.add_middleware(
        CORSMiddleware,
        allow_origins=build_cors_origins(resolved_settings),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

    if os.path.exists(FRONTEND_DIST_PATH):
        assets_path = os.path.join(FRONTEND_DIST_PATH, "assets")
        if os.path.exists(assets_path):
            app.mount("/assets", StaticFiles(directory=assets_path), name="assets")
        app.mount("/app", StaticFiles(directory=FRONTEND_DIST_PATH, html=True), name="frontend")

    @app.get("/")
    async def root():
        return {"message": "Kriegspiel Chess API"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/games")
    async def create_game(any_rule: bool = True):
        from kriegspiel_wrapper import ExtendedBerkeleyGame
        from models import save_game_with_serialization

        game_id = str(uuid.uuid4())
        game_engine = ExtendedBerkeleyGame(any_rule=any_rule)
        save_game_with_serialization(game_id, game_engine)
        return {"game_id": game_id, "status": "created", "any_rule": any_rule}

    @app.get("/games/{game_id}")
    async def get_game_state(game_id: str, player: str):
        import chess
        from models import get_game_by_id, load_game_from_serialization, reconstruct_game_from_history

        db_game = get_game_by_id(game_id)
        if not db_game:
            raise HTTPException(status_code=404, detail="Game not found")

        game = load_game_from_serialization(game_id)
        if not game:
            try:
                game = reconstruct_game_from_history(game_id)
            except Exception:
                raise HTTPException(status_code=503, detail="Game reconstruction failed")

        if player not in ["white", "black"]:
            raise HTTPException(status_code=400, detail="Player must be white or black")

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
        import chess
        from kriegspiel.move import KriegspielMove, QuestionAnnouncement
        from models import get_game_by_id, load_game_from_serialization, reconstruct_game_from_history
        from models import save_game_with_serialization, save_move_history

        db_game = get_game_by_id(game_id)
        if not db_game:
            raise HTTPException(status_code=404, detail="Game not found")

        game = load_game_from_serialization(game_id)
        if not game:
            try:
                game = reconstruct_game_from_history(game_id)
            except Exception:
                raise HTTPException(status_code=503, detail="Game reconstruction failed")

        if player not in ["white", "black"]:
            raise HTTPException(status_code=400, detail="Player must be white or black")

        color = chess.WHITE if player == "white" else chess.BLACK
        if game.turn != color:
            raise HTTPException(status_code=400, detail="Not your turn")

        try:
            question = QuestionAnnouncement.COMMON if question_type == "COMMON" else QuestionAnnouncement.ASK_ANY

            if question_type == "ASK_ANY":
                ks_move = KriegspielMove(question, None)
            else:
                if move_uci is None:
                    raise HTTPException(status_code=400, detail="move_uci is required for COMMON questions")
                move = chess.Move.from_uci(move_uci)
                ks_move = KriegspielMove(question, move)

            board_fen_before = game._game._board.fen()
            answer = game.ask_for(ks_move)

            visible_board = game.get_visible_board(color)
            board_fen_after = game._game._board.fen()

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
                    capture_square=(
                        chess.square_name(answer.capture_at_square) if answer.capture_at_square is not None else None
                    ),
                    is_legal=answer.move_done,
                    has_any=(answer.main_announcement.name == "HAS_ANY") if question_type == "ASK_ANY" else None,
                )
                save_game_with_serialization(game_id, game)
            except Exception:
                pass

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
                response["has_any"] = answer.main_announcement.name == "HAS_ANY"
            else:
                response["move"] = move_uci
                response["legal"] = answer.move_done

            return response
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid move: {str(e)}")

    @app.get("/games/{game_id}/history")
    async def get_game_history(game_id: str):
        from models import get_game_by_id, get_game_history as get_game_history_from_db

        history = get_game_history_from_db(game_id)
        if not history:
            db_game = get_game_by_id(game_id)
            if not db_game:
                raise HTTPException(status_code=404, detail="Game not found")
            return {"game_id": game_id, "history": []}

        history_data = [
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
            for entry in history
        ]
        return {"game_id": game_id, "history": history_data}

    @app.delete("/games/{game_id}")
    async def delete_game(game_id: str):
        from models import GameHistory, get_game_by_id

        db_game = get_game_by_id(game_id)
        if not db_game:
            raise HTTPException(status_code=404, detail="Game not found")

        GameHistory.delete().where(GameHistory.game == db_game).execute()
        db_game.delete_instance()
        return {"message": "Game deleted"}

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
