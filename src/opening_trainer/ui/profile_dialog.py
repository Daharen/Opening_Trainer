from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, simpledialog


class ProfileDialog:
    def __init__(self, master, session, refresh_callback, switch_callback=None, reset_callback=None, palette: dict[str, str] | None = None):
        self.master = master
        self.session = session
        self.refresh_callback = refresh_callback
        self.switch_callback = switch_callback
        self.reset_callback = reset_callback
        self.palette = palette or {
            'app_bg': '#f0f0f0',
            'surface_bg': '#ffffff',
            'text_fg': '#111111',
            'button_bg': '#f0f0f0',
            'button_active_bg': '#dfdfdf',
            'border_color': '#cfcfcf',
            'muted_fg': '#555555',
            'select_bg': '#cde2ff',
        }

    def open(self):
        top = tk.Toplevel(self.master)
        top.title('Profiles')
        top.configure(bg=self.palette['app_bg'])
        listbox = tk.Listbox(
            top,
            width=40,
            bg=self.palette['surface_bg'],
            fg=self.palette['text_fg'],
            selectbackground=self.palette['select_bg'],
            selectforeground=self.palette['text_fg'],
            disabledforeground=self.palette['muted_fg'],
            highlightbackground=self.palette['border_color'],
            highlightcolor=self.palette['border_color'],
            relief='flat',
        )
        profiles = self.session.profile_service.list_profiles()
        for profile in profiles:
            marker = ' (active)' if profile.profile_id == self.session.active_profile_id else ''
            listbox.insert('end', f'{profile.display_name} [{profile.profile_id}]{marker}')
        listbox.pack(fill='both', expand=True, padx=12, pady=12)

        def selected_id():
            if not listbox.curselection():
                return None
            index = listbox.curselection()[0]
            return profiles[index].profile_id

        button_opts = {
            'bg': self.palette['button_bg'],
            'fg': self.palette['text_fg'],
            'activebackground': self.palette['button_active_bg'],
            'activeforeground': self.palette['text_fg'],
            'highlightbackground': self.palette['border_color'],
        }
        tk.Button(top, text='Create', command=lambda: self._create(top), **button_opts).pack(fill='x', padx=12)
        tk.Button(top, text='Switch', command=lambda: self._switch(selected_id(), top), **button_opts).pack(fill='x', padx=12)
        tk.Button(top, text='Reset', command=lambda: self._reset(selected_id()), **button_opts).pack(fill='x', padx=12)
        tk.Button(top, text='Delete', command=lambda: self._delete(selected_id(), top), **button_opts).pack(fill='x', padx=12, pady=(0, 12))

    def _create(self, top):
        name = simpledialog.askstring('Create profile', 'Profile display name:', parent=top)
        if name:
            self.session.profile_service.create_profile(name)
            self.refresh_callback()
            top.destroy()
            self.open()

    def _switch(self, profile_id, top):
        if profile_id:
            self.session.switch_profile(profile_id)
            if self.switch_callback is not None:
                self.switch_callback()
            else:
                self.refresh_callback()
            top.destroy()

    def _reset(self, profile_id):
        if profile_id and messagebox.askyesno('Confirm reset', 'Clear this profile review memory, stats, and Smart Profile ladder state?'):
            if self.reset_callback is not None:
                self.reset_callback(profile_id)
            elif hasattr(self.session, 'reset_profile'):
                self.session.reset_profile(profile_id)
                self.refresh_callback()
            else:
                self.session.profile_service.reset_profile(profile_id)
                self.refresh_callback()

    def _delete(self, profile_id, top):
        if profile_id and profile_id != 'default' and messagebox.askyesno('Confirm delete', 'Delete this profile and its local review data?'):
            self.session.profile_service.delete_profile(profile_id)
            self.refresh_callback()
            top.destroy()
