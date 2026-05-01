from __future__ import annotations

from typing import Any, Literal

from app.services.engine_adapter import full_fen, serialize_scoresheet, visible_fen

PlayerColor = Literal['white', 'black']
_ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS = {
    'REGULAR_MOVE',
    'CAPTURE_DONE',
    'HAS_ANY',
    'NO_ANY',
    'ILLEGAL_MOVE',
    'NONSENSE',
}

_ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS = {
    'DRAW_TOOMANYREVERSIBLEMOVES',
    'DRAW_STALEMATE',
    'DRAW_INSUFFICIENT',
    'CHECKMATE_WHITE_WINS',
    'CHECKMATE_BLACK_WINS',
    'CHECK_RANK',
    'CHECK_FILE',
    'CHECK_LONG_DIAGONAL',
    'CHECK_SHORT_DIAGONAL',
    'CHECK_KNIGHT',
    'CHECK_DOUBLE',
}

_PUBLIC_ANNOUNCEMENT_TEXT = {
    'ILLEGAL_MOVE': 'Illegal move',
    'NONSENSE': 'Nonsense',
    'REGULAR_MOVE': 'Move complete',
    'CAPTURE_DONE': 'Capture done',
    'HAS_ANY': 'Has pawn captures',
    'NO_ANY': 'No pawn captures',
    'DRAW_TOOMANYREVERSIBLEMOVES': 'Draw by too many reversible moves',
    'DRAW_STALEMATE': 'Draw by stalemate',
    'DRAW_INSUFFICIENT': 'Draw by insufficient material',
    'CHECKMATE_WHITE_WINS': 'Checkmate — White wins',
    'CHECKMATE_BLACK_WINS': 'Checkmate — Black wins',
    'CHECK_RANK': 'Check on rank',
    'CHECK_FILE': 'Check on file',
    'CHECK_LONG_DIAGONAL': 'Check on long diagonal',
    'CHECK_SHORT_DIAGONAL': 'Check on short diagonal',
    'CHECK_KNIGHT': 'Check by knight',
    'CHECK_DOUBLE': 'Double check',
}


def project_player_fen(*, engine: Any, viewer_color: PlayerColor, game_state: str) -> str:
    if game_state == 'completed':
        return full_fen(engine)
    return visible_fen(engine, viewer_color)


def allowed_moves_for_player(*, engine: Any, game_state: str, viewer_color: PlayerColor, turn: str | None) -> list[str]:
    if game_state != 'active' or turn != viewer_color:
        return []

    return sorted(
        option.chess_move.uci()
        for option in engine.possible_to_ask
        if option.question_type.name == 'COMMON' and option.chess_move is not None
    )


def compute_possible_actions(
    *,
    engine: Any,
    game_state: str,
    viewer_color: PlayerColor,
    turn: str | None,
    rule_variant: str | None = None,
) -> list[str]:
    if game_state != 'active' or turn != viewer_color:
        return []

    has_move = False
    has_ask_any = False
    allow_ask_any = (rule_variant or 'berkeley_any') in {'berkeley_any', 'english', 'crazykrieg'}
    for option in engine.possible_to_ask:
        question_type = option.question_type.name
        has_move = has_move or question_type == 'COMMON'
        has_ask_any = has_ask_any or (allow_ask_any and question_type == 'ASK_ANY')

    actions: list[str] = []
    if has_move:
        actions.append('move')
    if has_ask_any:
        actions.append('ask_any')
    return actions


def _format_public_announcement(
    code: str,
    capture_square: str | None,
    *,
    captured_piece_announcement: str | None = None,
) -> str:
    text = _PUBLIC_ANNOUNCEMENT_TEXT.get(code)
    if not text:
        return ''
    if code == 'CAPTURE_DONE' and capture_square:
        captured_piece_labels = {
            'PAWN': 'Pawn',
            'PIECE': 'Piece',
            'KNIGHT': 'Knight',
            'BISHOP': 'Bishop',
            'ROOK': 'Rook',
            'QUEEN': 'Queen',
        }
        label = captured_piece_labels.get(captured_piece_announcement or '')
        if label:
            return f'{label} captured at {capture_square.upper()}'
        return f'{text} at {capture_square.upper()}'
    return text


def _format_piece_announcement(value: Any) -> str:
    labels = {
        'PAWN': 'Pawn',
        'KNIGHT': 'Knight',
        'BISHOP': 'Bishop',
        'ROOK': 'Rook',
        'QUEEN': 'Queen',
    }
    return labels.get(str(value or '').upper(), '')


def _next_turn_message(*, next_turn_pawn_tries: Any, next_turn_has_pawn_capture: Any, next_turn_pawn_try_squares: Any) -> str:
    if isinstance(next_turn_pawn_try_squares, list):
        squares = [str(square).upper() for square in next_turn_pawn_try_squares if isinstance(square, str)]
        if not squares:
            return 'No pawn captures'
        if len(squares) == 1:
            return f'Pawn try from {squares[0]}'
        return 'Pawn tries from ' + ', '.join(squares)
    if isinstance(next_turn_pawn_tries, int):
        if next_turn_pawn_tries <= 0:
            return 'No pawn captures'
        if next_turn_pawn_tries == 1:
            return '1 pawn try'
        return f'{next_turn_pawn_tries} pawn tries'
    if next_turn_has_pawn_capture is True:
        return 'Has pawn capture'
    if next_turn_has_pawn_capture is False:
        return 'No pawn captures'
    return ''


def _move_prompt_label(move: dict[str, Any], *, perspective: Literal['own', 'opponent']) -> str:
    question_type = str(move.get('question_type') or '').upper()
    if question_type == 'ASK_ANY':
        return 'Ask any pawn captures' if perspective == 'own' else 'Opponent asked any pawn captures'
    return 'Move attempt' if perspective == 'own' else 'Opponent move'


def _announcement_kind(question_type: str, main: str | None) -> str:
    if question_type == 'ASK_ANY':
        return 'ask_any'
    if main in {'ILLEGAL_MOVE', 'NONSENSE'}:
        return 'illegal_move'
    if main == 'CAPTURE_DONE':
        return 'capture'
    return 'move'


def _next_turn_side(*, color: PlayerColor, turn: int) -> tuple[int, PlayerColor]:
    if color == 'white':
        return turn, 'black'
    return turn + 1, 'white'


def _status_announcement(message: str, *, perspective: Literal['own', 'opponent']) -> dict[str, Any]:
    return {
        'kind': 'status',
        'actor': 'self' if perspective == 'own' else 'opponent',
        'prompt': None,
        'message': message,
        'messages': [message],
        'move_uci': None,
        'question_type': None,
    }


def _move_next_turn_message(move: dict[str, Any]) -> str:
    return _next_turn_message(
        next_turn_pawn_tries=move.get('next_turn_pawn_tries'),
        next_turn_has_pawn_capture=move.get('next_turn_has_pawn_capture'),
        next_turn_pawn_try_squares=move.get('next_turn_pawn_try_squares'),
    )


def _answer_next_turn_message(answer: dict[str, Any]) -> str:
    return _next_turn_message(
        next_turn_pawn_tries=answer.get('next_turn_pawn_tries'),
        next_turn_has_pawn_capture=answer.get('next_turn_has_pawn_capture'),
        next_turn_pawn_try_squares=answer.get('next_turn_pawn_try_squares'),
    )


def _move_messages(move: dict[str, Any]) -> list[str]:
    out: list[str] = []
    announcement = move.get('announcement')
    special_announcement = move.get('special_announcement')
    capture_square = move.get('capture_square')
    captured_piece_announcement = move.get('captured_piece_announcement')
    dropped_piece_announcement = move.get('dropped_piece_announcement')

    if announcement in _ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS:
        out.append(
            _format_public_announcement(
                announcement,
                capture_square,
                captured_piece_announcement=captured_piece_announcement,
            )
        )

    dropped_piece = _format_piece_announcement(dropped_piece_announcement)
    if dropped_piece:
        out.append(f'{dropped_piece} dropped')

    if move.get('promotion_announced') is True:
        out.append('Promotion')

    if special_announcement in _ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS:
        out.append(_format_public_announcement(special_announcement, None))

    return [item for item in out if item]


def _build_turn_announcement(move: dict[str, Any], *, perspective: Literal['own', 'opponent']) -> dict[str, Any] | None:
    question_type = str(move.get('question_type') or 'COMMON').upper()
    main = move.get('announcement') if isinstance(move.get('announcement'), str) else None
    messages = _move_messages(move)
    if not messages:
        return None
    prompt = _move_prompt_label(move, perspective=perspective)
    return {
        'kind': _announcement_kind(question_type, main),
        'actor': 'self' if perspective == 'own' else 'opponent',
        'prompt': prompt,
        'message': f'{prompt} — ' + ' · '.join(messages),
        'messages': messages,
        'move_uci': None if perspective == 'opponent' else move.get('uci'),
        'question_type': question_type,
    }


def build_referee_log(moves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for move in moves:
        announcement = move.get('announcement')
        special_announcement = move.get('special_announcement')
        base_item = {
            'ply': move.get('ply'),
            'timestamp': move.get('timestamp'),
        }

        if announcement in _ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS:
            out.append(
                {
                    **base_item,
                    'announcement': announcement,
                    'special_announcement': None,
                    'capture_square': move.get('capture_square'),
                }
            )

        next_turn_message = _move_next_turn_message(move)
        if next_turn_message:
            next_ply = (move.get('ply') + 1) if isinstance(move.get('ply'), int) else None
            out.append(
                {
                    **base_item,
                    'ply': next_ply,
                    'announcement': next_turn_message,
                    'special_announcement': None,
                    'capture_square': None,
                }
            )

        if special_announcement in _ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS:
            out.append(
                {
                    **base_item,
                    'announcement': special_announcement,
                    'special_announcement': special_announcement,
                    'capture_square': None,
                }
            )
    return out


def build_referee_turns(moves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    turns: dict[int, dict[str, Any]] = {}
    for index, move in enumerate(moves):
        ply = move.get('ply')
        turn = (ply + 1) // 2 if isinstance(ply, int) and ply > 0 else (index // 2) + 1

        color = str(move.get('color') or '').lower()
        if color not in {'white', 'black'}:
            color = 'white' if index % 2 == 0 else 'black'

        announcement = _build_turn_announcement(move, perspective='own')
        if not announcement:
            continue

        entry = turns.setdefault(turn, {'turn': turn, 'white': [], 'black': []})
        entry[color].append(announcement)

        next_turn_message = _move_next_turn_message(move)
        if next_turn_message:
            next_turn, next_color = _next_turn_side(color=color, turn=turn)
            next_entry = turns.setdefault(next_turn, {'turn': next_turn, 'white': [], 'black': []})
            next_entry[next_color].append(_status_announcement(next_turn_message, perspective='own'))

    return [turns[key] for key in sorted(turns)]


def build_viewer_scoresheet(*, viewer_color: PlayerColor, stored_scoresheet: dict[str, Any] | None) -> dict[str, Any]:
    scoresheet = stored_scoresheet or {'color': viewer_color, 'moves_own': [], 'moves_opponent': [], 'last_move_number': 0}
    own_turns = scoresheet.get('moves_own') if isinstance(scoresheet.get('moves_own'), list) else []
    opponent_turns = scoresheet.get('moves_opponent') if isinstance(scoresheet.get('moves_opponent'), list) else []
    turn_count = max(len(own_turns), len(opponent_turns))
    turn_map: dict[int, dict[str, Any]] = {}

    def ensure_turn(turn_number: int) -> dict[str, Any]:
        return turn_map.setdefault(turn_number, {'turn': turn_number, 'white': [], 'black': []})

    def append_entries(
        raw_entries: list[Any],
        *,
        turn_number: int,
        color: PlayerColor,
        perspective: Literal['own', 'opponent'],
    ) -> None:
        for raw_entry in raw_entries:
            if not raw_entry:
                continue

            normalized = _normalize_scoresheet_entry(raw_entry, perspective=perspective)
            if normalized:
                ensure_turn(turn_number)[color].append(normalized)

            answer = (
                raw_entry.get('answer')
                if isinstance(raw_entry, dict) and isinstance(raw_entry.get('answer'), dict)
                else {}
            )
            next_turn_message = _answer_next_turn_message(answer)
            if next_turn_message:
                next_turn, next_color = _next_turn_side(color=color, turn=turn_number)
                next_perspective = 'own' if next_color == viewer_color else 'opponent'
                ensure_turn(next_turn)[next_color].append(_status_announcement(next_turn_message, perspective=next_perspective))

    for index in range(turn_count):
        turn_number = index + 1
        white_turns = own_turns if viewer_color == 'white' else opponent_turns
        black_turns = own_turns if viewer_color == 'black' else opponent_turns
        white_perspective: Literal['own', 'opponent'] = 'own' if viewer_color == 'white' else 'opponent'
        black_perspective: Literal['own', 'opponent'] = 'own' if viewer_color == 'black' else 'opponent'

        append_entries(
            white_turns[index] if index < len(white_turns) else [],
            turn_number=turn_number,
            color='white',
            perspective=white_perspective,
        )
        append_entries(
            black_turns[index] if index < len(black_turns) else [],
            turn_number=turn_number,
            color='black',
            perspective=black_perspective,
        )

    turns = [turn_map[key] for key in sorted(turn_map)]
    turns = [turn for turn in turns if turn['white'] or turn['black']]

    return {
        'viewer_color': viewer_color,
        'last_move_number': int(scoresheet.get('last_move_number', 0) or 0),
        'turns': turns,
    }


def build_viewer_referee_turns(*, viewer_color: PlayerColor, stored_scoresheet: dict[str, Any] | None) -> list[dict[str, Any]]:
    return build_viewer_scoresheet(viewer_color=viewer_color, stored_scoresheet=stored_scoresheet)['turns']


def build_viewer_referee_log(*, viewer_color: PlayerColor, stored_scoresheet: dict[str, Any] | None) -> list[dict[str, Any]]:
    turns = build_viewer_referee_turns(viewer_color=viewer_color, stored_scoresheet=stored_scoresheet)
    out: list[dict[str, Any]] = []

    for turn in turns:
        turn_number = int(turn.get('turn', 0) or 0)
        for color in ('white', 'black'):
            entries = turn.get(color, [])
            if not isinstance(entries, list):
                continue

            ply = ((turn_number - 1) * 2) + (1 if color == 'white' else 2) if turn_number > 0 else None
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                out.append(
                    {
                        'ply': ply,
                        'announcement': entry.get('message', ''),
                        'special_announcement': None,
                        'capture_square': None,
                        'timestamp': None,
                    }
                )

    return out


def serialize_engine_scoresheets(engine: Any) -> dict[str, dict[str, Any]]:
    return {
        'white': serialize_scoresheet(getattr(engine, '_whites_scoresheet', None)),  # noqa: SLF001
        'black': serialize_scoresheet(getattr(engine, '_blacks_scoresheet', None)),  # noqa: SLF001
    }


def reconstruct_scoresheets_from_moves(moves: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    white_own: list[list[dict[str, Any]]] = []
    white_opponent: list[list[dict[str, Any]]] = []
    black_own: list[list[dict[str, Any]]] = []
    black_opponent: list[list[dict[str, Any]]] = []
    last_move_number = 0

    for index, move in enumerate(moves):
        ply = move.get('ply')
        turn = (ply + 1) // 2 if isinstance(ply, int) and ply > 0 else (index // 2) + 1
        color = str(move.get('color') or '').lower()
        if color not in {'white', 'black'}:
            color = 'white' if index % 2 == 0 else 'black'
        last_move_number = max(last_move_number, turn)

        pair = {
            'question': {
                'question_type': str(move.get('question_type') or 'COMMON').upper(),
                'move_uci': move.get('uci'),
            },
            'answer': {
                'main_announcement': move.get('announcement'),
                'special_announcement': move.get('special_announcement'),
                'capture_square': move.get('capture_square'),
                'captured_piece_announcement': move.get('captured_piece_announcement'),
                'dropped_piece_announcement': move.get('dropped_piece_announcement'),
                'promotion_announced': move.get('promotion_announced'),
                'next_turn_pawn_tries': move.get('next_turn_pawn_tries'),
                'next_turn_has_pawn_capture': move.get('next_turn_has_pawn_capture'),
                'next_turn_pawn_try_squares': move.get('next_turn_pawn_try_squares'),
                'checks': [],
                'move_done': bool(move.get('move_done', False)),
            },
        }
        opponent_pair = {
            'question': {'question_type': pair['question']['question_type']},
            'answer': dict(pair['answer']),
        }

        _append_turn_entry(white_own, turn, pair if color == 'white' else None)
        _append_turn_entry(white_opponent, turn, opponent_pair if color == 'black' else None)
        _append_turn_entry(black_own, turn, pair if color == 'black' else None)
        _append_turn_entry(black_opponent, turn, opponent_pair if color == 'white' else None)

    return {
        'white': {
            'color': 'white',
            'last_move_number': last_move_number,
            'moves_own': white_own,
            'moves_opponent': white_opponent,
        },
        'black': {
            'color': 'black',
            'last_move_number': last_move_number,
            'moves_own': black_own,
            'moves_opponent': black_opponent,
        },
    }


def _append_turn_entry(target: list[list[dict[str, Any]]], turn: int, value: dict[str, Any] | None) -> None:
    if value is None:
        return
    while len(target) < turn:
        target.append([])
    target[turn - 1].append(value)


def _normalize_scoresheet_entry(entry: dict[str, Any], *, perspective: Literal['own', 'opponent']) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    question = entry.get('question') if isinstance(entry.get('question'), dict) else {}
    answer = entry.get('answer') if isinstance(entry.get('answer'), dict) else {}
    question_type = str(question.get('question_type') or '').upper()
    move_uci = question.get('move_uci')
    answer_texts = _scoresheet_answer_texts(answer)
    if not answer_texts:
        return None

    prompt = 'Move attempt'
    if perspective == 'own':
        if question_type == 'ASK_ANY':
            prompt = 'Ask any pawn captures'
    elif question_type == 'ASK_ANY':
        prompt = 'Opponent asked any pawn captures'
    else:
        prompt = 'Opponent move'

    kind = _announcement_kind(question_type, answer.get('main_announcement'))
    return {
        'kind': kind,
        'actor': 'self' if perspective == 'own' else 'opponent',
        'prompt': prompt,
        'message': f'{prompt} — ' + ' · '.join(answer_texts),
        'messages': answer_texts,
        'move_uci': move_uci if perspective == 'own' else None,
        'question_type': question_type,
    }


def _scoresheet_answer_texts(answer: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    main = answer.get('main_announcement')
    if isinstance(main, str) and main in _ALLOWED_PUBLIC_MAIN_ANNOUNCEMENTS:
        texts.append(
            _format_public_announcement(
                main,
                answer.get('capture_square'),
                captured_piece_announcement=answer.get('captured_piece_announcement'),
            )
        )
    special = answer.get('special_announcement')
    if isinstance(special, str) and special in _ALLOWED_PUBLIC_SPECIAL_ANNOUNCEMENTS:
        texts.append(_format_public_announcement(special, None))
    dropped_piece = _format_piece_announcement(answer.get('dropped_piece_announcement'))
    if dropped_piece:
        texts.append(f'{dropped_piece} dropped')
    if answer.get('promotion_announced') is True:
        texts.append('Promotion')
    return [text for text in texts if text]
