from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

import chess

from ..review.models import ManualForcedPlayerColor, ManualPresentationMode, UrgencyTier
from .board_view import BoardView, PIECE_GLYPHS

PIECE_CHOICES: tuple[str, ...] = ('K', 'Q', 'R', 'B', 'N', 'P', 'k', 'q', 'r', 'b', 'n', 'p')


def build_setup_fen(*, board_fen: str, turn: str, castling: str) -> str:
    cleaned_turn = (turn or 'w').strip().lower()
    if cleaned_turn not in {'w', 'b'}:
        raise ValueError('Side to move must be white or black.')
    normalized_castling = ''.join(ch for ch in castling if ch in 'KQkq')
    normalized_castling = normalized_castling or '-'
    fen = f'{board_fen} {cleaned_turn} {normalized_castling} - 0 1'
    board = chess.Board(fen)
    if len(board.pieces(chess.KING, chess.WHITE)) != 1:
        raise ValueError('Board setup must contain exactly one white king.')
    if len(board.pieces(chess.KING, chess.BLACK)) != 1:
        raise ValueError('Board setup must contain exactly one black king.')
    if board.status() != chess.STATUS_VALID:
        raise ValueError('Board state is not legal enough for trainer startup.')
    return board.fen()


class BoardSetupEditorDialog(tk.Toplevel):
    def __init__(
        self,
        master,
        on_save,
        *,
        title: str = 'Board Setup Editor',
        initial: dict | None = None,
        save_label: str = 'Save',
    ):
        super().__init__(master)
        self.title(title)
        self.transient(master)
        self.grab_set()
        self.on_save = on_save
        self.resizable(True, True)
        self.minsize(860, 640)
        initial = initial or {}

        initial_fen = initial.get('target_fen') or chess.STARTING_FEN
        try:
            self.board = chess.Board(initial_fen)
        except ValueError:
            self.board = chess.Board()

        self.orientation = chess.WHITE
        self.selected_tool = tk.StringVar(value='P')
        self.turn_var = tk.StringVar(value='w' if self.board.turn == chess.WHITE else 'b')
        self.castle_k_var = tk.BooleanVar(value=self.board.has_kingside_castling_rights(chess.WHITE))
        self.castle_q_var = tk.BooleanVar(value=self.board.has_queenside_castling_rights(chess.WHITE))
        self.castle_k_black_var = tk.BooleanVar(value=self.board.has_kingside_castling_rights(chess.BLACK))
        self.castle_q_black_var = tk.BooleanVar(value=self.board.has_queenside_castling_rights(chess.BLACK))
        self.forced_color_var = tk.StringVar(value=initial.get('manual_forced_player_color', ManualForcedPlayerColor.AUTO.value))
        self.urgency_var = tk.StringVar(value=initial.get('urgency_tier', UrgencyTier.ORDINARY.value))
        self.note_var = tk.StringVar(value=initial.get('operator_note', ''))
        self.error_var = tk.StringVar(value='')

        root = ttk.Frame(self, padding=12)
        root.pack(fill='both', expand=True)
        root.columnconfigure(0, weight=0)
        root.columnconfigure(1, weight=1)
        root.rowconfigure(0, weight=1)

        board_frame = ttk.Frame(root)
        board_frame.grid(row=0, column=0, sticky='nsew', padx=(0, 16))
        self.board_view = BoardView(board_frame, board_size=520, min_board_size=360)
        self.board_view.pack(fill='both', expand=True)
        self.board_view.bind('<Button-1>', self._on_board_click)

        controls = ttk.Frame(root)
        controls.grid(row=0, column=1, sticky='nsew')

        palette = ttk.LabelFrame(controls, text='Piece palette')
        palette.pack(fill='x')
        for idx, symbol in enumerate(PIECE_CHOICES):
            display = f"{PIECE_GLYPHS[symbol]} {symbol}"
            ttk.Radiobutton(palette, text=display, value=symbol, variable=self.selected_tool).grid(row=idx // 3, column=idx % 3, sticky='w', padx=6, pady=2)
        ttk.Radiobutton(palette, text='🧽 Erase', value='erase', variable=self.selected_tool).grid(row=4, column=0, sticky='w', padx=6, pady=4)

        board_controls = ttk.Frame(controls)
        board_controls.pack(fill='x', pady=(10, 0))
        ttk.Button(board_controls, text='Clear Board', command=self._clear_board).pack(side='left')
        ttk.Button(board_controls, text='Standard Start', command=self._load_start).pack(side='left', padx=(6, 0))
        ttk.Button(board_controls, text='Flip View', command=self._flip_view).pack(side='left', padx=(6, 0))
        ttk.Button(board_controls, text='Copy FEN', command=self._copy_fen).pack(side='left', padx=(6, 0))
        ttk.Button(board_controls, text='Paste FEN', command=self._paste_fen).pack(side='left', padx=(6, 0))

        state_box = ttk.LabelFrame(controls, text='State')
        state_box.pack(fill='x', pady=(12, 0))
        ttk.Label(state_box, text='Side to move').grid(row=0, column=0, sticky='w', padx=6, pady=4)
        ttk.Radiobutton(state_box, text='White', value='w', variable=self.turn_var).grid(row=0, column=1, sticky='w', padx=4)
        ttk.Radiobutton(state_box, text='Black', value='b', variable=self.turn_var).grid(row=0, column=2, sticky='w', padx=4)
        ttk.Label(state_box, text='Castling rights').grid(row=1, column=0, sticky='w', padx=6)
        ttk.Checkbutton(state_box, text='K', variable=self.castle_k_var).grid(row=1, column=1, sticky='w')
        ttk.Checkbutton(state_box, text='Q', variable=self.castle_q_var).grid(row=1, column=2, sticky='w')
        ttk.Checkbutton(state_box, text='k', variable=self.castle_k_black_var).grid(row=2, column=1, sticky='w')
        ttk.Checkbutton(state_box, text='q', variable=self.castle_q_black_var).grid(row=2, column=2, sticky='w')

        meta_box = ttk.LabelFrame(controls, text='Manual setup metadata')
        meta_box.pack(fill='x', pady=(12, 0))
        ttk.Label(meta_box, text='Forced player color').grid(row=0, column=0, sticky='w', padx=6, pady=4)
        ttk.Combobox(meta_box, state='readonly', textvariable=self.forced_color_var, values=[ManualForcedPlayerColor.AUTO.value, ManualForcedPlayerColor.WHITE.value, ManualForcedPlayerColor.BLACK.value], width=12).grid(row=0, column=1, sticky='w', padx=4, pady=4)
        ttk.Label(meta_box, text='Urgency').grid(row=1, column=0, sticky='w', padx=6, pady=4)
        ttk.Combobox(meta_box, state='readonly', textvariable=self.urgency_var, values=[UrgencyTier.ORDINARY.value, UrgencyTier.BOOSTED.value, UrgencyTier.EXTREME.value], width=16).grid(row=1, column=1, sticky='w', padx=4, pady=4)
        ttk.Label(meta_box, text='Note').grid(row=2, column=0, sticky='w', padx=6, pady=4)
        ttk.Entry(meta_box, textvariable=self.note_var).grid(row=2, column=1, sticky='ew', padx=4, pady=4)
        meta_box.columnconfigure(1, weight=1)

        ttk.Label(controls, textvariable=self.error_var, foreground='red', wraplength=340).pack(fill='x', pady=(10, 0))

        buttons = ttk.Frame(controls)
        buttons.pack(fill='x', pady=(10, 0))
        ttk.Button(buttons, text='Cancel', command=self.destroy).pack(side='right')
        ttk.Button(buttons, text=save_label, command=self._save).pack(side='right', padx=(0, 8))

        self._render()

    def _render(self) -> None:
        self.board_view.render(self.board, self.orientation)

    def _castling_rights_token(self) -> str:
        rights = ''.join(
            token
            for token, enabled in (
                ('K', self.castle_k_var.get()),
                ('Q', self.castle_q_var.get()),
                ('k', self.castle_k_black_var.get()),
                ('q', self.castle_q_black_var.get()),
            )
            if enabled
        )
        return rights or '-'

    def _on_board_click(self, event: tk.Event) -> None:
        square = self.board_view.square_at_xy(event.x, event.y, self.orientation)
        if square is None:
            return
        tool = self.selected_tool.get()
        if tool == 'erase':
            self.board.remove_piece_at(square)
        else:
            try:
                self.board.set_piece_at(square, chess.Piece.from_symbol(tool))
            except ValueError:
                return
        self._render()

    def _clear_board(self) -> None:
        self.board = chess.Board(None)
        self._render()

    def _load_start(self) -> None:
        self.board = chess.Board()
        self.turn_var.set('w')
        self.castle_k_var.set(True)
        self.castle_q_var.set(True)
        self.castle_k_black_var.set(True)
        self.castle_q_black_var.set(True)
        self._render()

    def _flip_view(self) -> None:
        self.orientation = chess.BLACK if self.orientation == chess.WHITE else chess.WHITE
        self._render()

    def _copy_fen(self) -> None:
        try:
            fen = build_setup_fen(board_fen=self.board.board_fen(), turn=self.turn_var.get(), castling=self._castling_rights_token())
        except ValueError as exc:
            self.error_var.set(str(exc))
            return
        self.clipboard_clear()
        self.clipboard_append(fen)
        self.update_idletasks()

    def _paste_fen(self) -> None:
        try:
            fen = self.clipboard_get().strip()
        except tk.TclError:
            return
        if not fen:
            return
        try:
            loaded = chess.Board(fen)
        except ValueError as exc:
            messagebox.showerror('Paste FEN', f'Clipboard does not contain valid FEN:\n{exc}', parent=self)
            return
        self.board = loaded
        self.turn_var.set('w' if loaded.turn == chess.WHITE else 'b')
        self.castle_k_var.set(loaded.has_kingside_castling_rights(chess.WHITE))
        self.castle_q_var.set(loaded.has_queenside_castling_rights(chess.WHITE))
        self.castle_k_black_var.set(loaded.has_kingside_castling_rights(chess.BLACK))
        self.castle_q_black_var.set(loaded.has_queenside_castling_rights(chess.BLACK))
        self._render()

    def _save(self) -> None:
        try:
            fen = build_setup_fen(board_fen=self.board.board_fen(), turn=self.turn_var.get(), castling=self._castling_rights_token())
        except ValueError as exc:
            self.error_var.set(str(exc))
            return
        self.error_var.set('')
        self.on_save(
            target_fen=fen,
            predecessor_line_uci=None,
            urgency_tier=self.urgency_var.get().strip(),
            allow_below_threshold_reach=False,
            manual_presentation_mode=ManualPresentationMode.MANUAL_SETUP_START.value,
            manual_forced_player_color=self.forced_color_var.get().strip(),
            operator_note=self.note_var.get().strip() or None,
        )
        self.destroy()
