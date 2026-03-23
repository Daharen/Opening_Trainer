from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, simpledialog


class ProfileDialog:
    def __init__(self, master, session, refresh_callback):
        self.master = master
        self.session = session
        self.refresh_callback = refresh_callback

    def open(self):
        top = tk.Toplevel(self.master)
        top.title('Profiles')
        listbox = tk.Listbox(top, width=40)
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

        tk.Button(top, text='Create', command=lambda: self._create(top)).pack(fill='x', padx=12)
        tk.Button(top, text='Switch', command=lambda: self._switch(selected_id(), top)).pack(fill='x', padx=12)
        tk.Button(top, text='Reset', command=lambda: self._reset(selected_id())).pack(fill='x', padx=12)
        tk.Button(top, text='Delete', command=lambda: self._delete(selected_id(), top)).pack(fill='x', padx=12, pady=(0, 12))

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
            self.refresh_callback()
            top.destroy()

    def _reset(self, profile_id):
        if profile_id and messagebox.askyesno('Confirm reset', 'Clear this profile review memory and stats?'):
            self.session.profile_service.reset_profile(profile_id)
            self.refresh_callback()

    def _delete(self, profile_id, top):
        if profile_id and profile_id != 'default' and messagebox.askyesno('Confirm delete', 'Delete this profile and its local review data?'):
            self.session.profile_service.delete_profile(profile_id)
            self.refresh_callback()
            top.destroy()
