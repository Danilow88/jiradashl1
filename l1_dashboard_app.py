#!/usr/bin/env python3
"""
L1 Dashboard - App com interface gráfica (Tkinter).
Conecta no Jira, busca issues pela JQL e exibe em tabela.
Permite usar filtros salvos do Jira e filtros rápidos (Projeto, Status).
"""

import os
import sys
import threading
import webbrowser

# Garantir que o diretório do script está no path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog
except ImportError as e:
    print('Tkinter não encontrado. Este Python não foi instalado com suporte a Tk.')
    print()
    print('No macOS:')
    print('  1. Se você usa Homebrew, instale o Tk para sua versão do Python:')
    print('     brew install python-tk@3.12   # use 3.11, 3.12 etc. conforme "python3 --version"')
    print('  2. Ou rode com o Python do sistema (já vem com Tk):')
    print('     /usr/bin/python3 l1_dashboard_app.py')
    print('     (instale dependências antes: /usr/bin/python3 -m pip install requests python-dotenv)')
    print()
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

# Reutilizar lógica do dashboard em linha de comando
from l1_dashboard import (
    JIRA_URL,
    get_jira_credentials,
    resolve_custom_fields,
    search_jql,
    get_row_values,
    COLUMNS,
    DEFAULT_JQL,
    write_html,
    fetch_my_filters,
    fetch_filter_by_id,
    fetch_projects,
    fetch_statuses,
)


def open_issue_in_browser(key):
    """Abre o issue no navegador (Jira)."""
    url = f'{JIRA_URL.rstrip("/")}/browse/{key}'
    webbrowser.open(url)


class L1DashboardApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title('L1 Dashboard - Jira')
        self.root.minsize(900, 500)
        self.root.geometry('1100x600')

        self.auth = None
        self.field_ids = {}
        self.current_issues = []
        self.current_jql = ''
        self.saved_filters = []  # [(id, name, jql?), ...]
        self.projects_list = []  # [(key, name), ...]
        self.statuses_list = []  # [name, ...]

        self._build_ui()
        self._load_credentials()

    def _build_ui(self):
        # ---- Top: JQL e controles ----
        top = ttk.Frame(self.root, padding=10)
        top.pack(fill=tk.X)

        ttk.Label(top, text='JQL:').pack(anchor=tk.W)
        self.jql_entry = tk.Text(top, height=3, wrap=tk.WORD, font=('Menlo', 11))
        self.jql_entry.pack(fill=tk.X, pady=(0, 6))
        self.jql_entry.insert('1.0', DEFAULT_JQL)

        row1 = ttk.Frame(top)
        row1.pack(fill=tk.X)
        ttk.Label(row1, text='Limite:').pack(side=tk.LEFT, padx=(0, 4))
        self.limit_var = tk.StringVar(value='50')
        self.limit_spin = ttk.Spinbox(row1, from_=1, to=500, width=6, textvariable=self.limit_var)
        self.limit_spin.pack(side=tk.LEFT, padx=(0, 12))
        self.btn_buscar = ttk.Button(row1, text='Buscar', command=self._on_buscar)
        self.btn_buscar.pack(side=tk.LEFT, padx=(0, 8))
        self.btn_export = ttk.Button(row1, text='Exportar HTML', command=self._on_export_html, state=tk.DISABLED)
        self.btn_export.pack(side=tk.LEFT)

        # ---- Filtros do Jira ----
        filtros_frame = ttk.LabelFrame(top, text='Filtros do Jira', padding=6)
        filtros_frame.pack(fill=tk.X, pady=(10, 0))

        row_filtros = ttk.Frame(filtros_frame)
        row_filtros.pack(fill=tk.X)
        self.btn_carregar_filtros = ttk.Button(row_filtros, text='Carregar meus filtros', command=self._on_carregar_filtros)
        self.btn_carregar_filtros.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(row_filtros, text='Filtro salvo:').pack(side=tk.LEFT, padx=(0, 4))
        self.filter_combo_var = tk.StringVar()
        self.filter_combo = ttk.Combobox(row_filtros, textvariable=self.filter_combo_var, width=40, state='readonly')
        self.filter_combo.pack(side=tk.LEFT, padx=(0, 4))
        self.filter_combo.bind('<<ComboboxSelected>>', self._on_filter_selected)

        row_rapidos = ttk.Frame(filtros_frame)
        row_rapidos.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(row_rapidos, text='Projeto:').pack(side=tk.LEFT, padx=(0, 4))
        self.project_combo_var = tk.StringVar()
        self.project_combo = ttk.Combobox(row_rapidos, textvariable=self.project_combo_var, width=18, state='readonly')
        self.project_combo.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(row_rapidos, text='Status:').pack(side=tk.LEFT, padx=(0, 4))
        self.status_combo_var = tk.StringVar()
        self.status_combo = ttk.Combobox(row_rapidos, textvariable=self.status_combo_var, width=18, state='readonly')
        self.status_combo.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(row_rapidos, text='Aplicar filtros à JQL', command=self._on_aplicar_filtros_rapidos).pack(side=tk.LEFT)

        # ---- Tabela ----
        table_frame = ttk.Frame(self.root, padding=(10, 0))
        table_frame.pack(fill=tk.BOTH, expand=True)

        cols = ('Key', 'Reporter', 'Time to resolution', 'Time to first response', 'Assignee', 'Request Type')
        self.tree = ttk.Treeview(table_frame, columns=cols, show='headings', height=20)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120, minwidth=80)

        vsb = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)

        self.tree.bind('<Double-1>', self._on_double_click)

        # ---- Status ----
        self.status_var = tk.StringVar(value='Pronto. Configure JQL e clique em Buscar.')
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=4)

    def _load_credentials(self):
        try:
            self.auth = get_jira_credentials()
            self.status_var.set('Conectado. Digite a JQL e clique em Buscar.')
        except Exception as e:
            self.status_var.set(f'Erro de credenciais: {e}. Configure .env (JIRA_EMAIL, JIRA_API_TOKEN).')
            self.btn_buscar.config(state=tk.DISABLED)

    def _on_carregar_filtros(self):
        if not self.auth:
            messagebox.showwarning('Credenciais', 'Conecte primeiro (configure .env).')
            return
        self.btn_carregar_filtros.config(state=tk.DISABLED)
        self.status_var.set('Carregando filtros, projetos e status do Jira...')
        self.root.update_idletasks()

        def do_load():
            try:
                filters_data = fetch_my_filters(self.auth)
                projects_data = fetch_projects(self.auth)
                statuses_data = fetch_statuses(self.auth)
                self.root.after(0, lambda: self._show_filtros_loaded(filters_data, projects_data, statuses_data))
            except Exception as e:
                err_msg = str(e)
                self.root.after(0, lambda: self._show_filtros_error(err_msg))

        threading.Thread(target=do_load, daemon=True).start()

    def _show_filtros_loaded(self, filters_data, projects_data, statuses_data):
        self.btn_carregar_filtros.config(state=tk.NORMAL)
        self.saved_filters = []
        names = []
        for f in filters_data:
            fid = f.get('id')
            name = f.get('name', '')
            jql = f.get('jql', '')
            self.saved_filters.append((str(fid) if fid is not None else '', name, jql))
            names.append(name)
        self.filter_combo['values'] = names
        if names:
            self.filter_combo.current(0)
            self._on_filter_selected(None)

        self.projects_list = list(projects_data)
        project_keys = [f'{k} - {n}' for k, n in self.projects_list]
        self.project_combo['values'] = [''] + project_keys
        self.project_combo_var.set('')

        self.statuses_list = list(statuses_data)
        self.status_combo['values'] = [''] + self.statuses_list
        self.status_combo_var.set('')

        self.status_var.set(f'Carregados: {len(names)} filtro(s), {len(self.projects_list)} projeto(s), {len(self.statuses_list)} status.')

    def _show_filtros_error(self, msg):
        self.btn_carregar_filtros.config(state=tk.NORMAL)
        self.status_var.set(f'Erro ao carregar filtros: {msg[:60]}...')
        messagebox.showerror('Erro ao carregar filtros', msg)

    def _on_filter_selected(self, event):
        idx = self.filter_combo.current()
        if idx < 0 or idx >= len(self.saved_filters):
            return
        fid, name, jql = self.saved_filters[idx]
        if jql:
            self.jql_entry.delete('1.0', tk.END)
            self.jql_entry.insert('1.0', jql)
            self.status_var.set(f'JQL do filtro "{name}" aplicada.')
            return
        self.status_var.set('Buscando JQL do filtro...')
        self.root.update_idletasks()

        def do_fetch():
            try:
                f = fetch_filter_by_id(self.auth, fid)
                jql = f.get('jql', '')
                self.root.after(0, lambda: self._apply_filter_jql(jql, name))
            except Exception as e:
                err_msg = str(e)
                self.root.after(0, lambda: self._show_filtros_error(err_msg))

        threading.Thread(target=do_fetch, daemon=True).start()

    def _apply_filter_jql(self, jql, name):
        self.jql_entry.delete('1.0', tk.END)
        self.jql_entry.insert('1.0', jql)
        self.status_var.set(f'JQL do filtro "{name}" aplicada.')

    def _on_aplicar_filtros_rapidos(self):
        parts = []
        proj = self.project_combo_var.get().strip()
        if proj and ' - ' in proj:
            key = proj.split(' - ', 1)[0].strip()
            if key:
                parts.append(f'project = "{key}"')
        status = self.status_combo_var.get().strip()
        if status:
            parts.append(f'status = "{status}"')
        if not parts:
            messagebox.showinfo('Filtros rápidos', 'Selecione Projeto e/ou Status e clique em Aplicar.')
            return
        jql_extra = ' AND '.join(parts)
        current = self.jql_entry.get('1.0', tk.END).strip()
        if current:
            new_jql = f'({current}) AND {jql_extra}'
        else:
            new_jql = jql_extra
        self.jql_entry.delete('1.0', tk.END)
        self.jql_entry.insert('1.0', new_jql)
        self.status_var.set('Filtros rápidos aplicados à JQL.')

    def _on_buscar(self):
        jql = self.jql_entry.get('1.0', tk.END).strip()
        if not jql:
            messagebox.showwarning('JQL vazia', 'Digite uma consulta JQL.')
            return
        try:
            limit = int(self.limit_var.get())
        except ValueError:
            limit = 50
        self.btn_buscar.config(state=tk.DISABLED)
        self.btn_export.config(state=tk.DISABLED)
        self.status_var.set('Buscando...')
        self.root.update_idletasks()

        def do_search():
            try:
                field_ids = resolve_custom_fields(self.auth)
                issues = search_jql(self.auth, jql, field_ids, limit=limit)
                self.root.after(0, lambda: self._show_results(issues, field_ids, jql))
            except Exception as e:
                err_msg = str(e)
                self.root.after(0, lambda: self._show_error(err_msg))

        threading.Thread(target=do_search, daemon=True).start()

    def _show_results(self, issues, field_ids, jql):
        self.btn_buscar.config(state=tk.NORMAL)
        self.field_ids = field_ids
        self.current_issues = issues
        self.current_jql = jql

        for item in self.tree.get_children():
            self.tree.delete(item)

        for issue in issues:
            row = get_row_values(issue, field_ids)
            values = (
                row.get('key', ''),
                row.get('Reporter', ''),
                row.get('Time to resolution', ''),
                row.get('Time to first response', ''),
                row.get('Assignee', ''),
                row.get('Request Type', ''),
            )
            self.tree.insert('', tk.END, values=values)

        self.status_var.set(f'{len(issues)} issue(s) encontrado(s). Duplo clique na linha para abrir no Jira.')
        self.btn_export.config(state=tk.NORMAL)

    def _show_error(self, msg):
        self.btn_buscar.config(state=tk.NORMAL)
        self.status_var.set(f'Erro: {msg[:80]}...' if len(msg) > 80 else f'Erro: {msg}')
        messagebox.showerror('Erro na busca', msg)

    def _on_double_click(self, event):
        sel = self.tree.selection()
        if not sel:
            return
        item = self.tree.item(sel[0])
        vals = item.get('values', [])
        if vals:
            key = vals[0]
            if key:
                open_issue_in_browser(key)

    def _on_export_html(self):
        if not self.current_issues:
            messagebox.showinfo('Exportar', 'Nenhum resultado para exportar. Faça uma busca antes.')
            return
        path = filedialog.asksaveasfilename(
            defaultextension='.html',
            filetypes=[('HTML', '*.html'), ('Todos', '*.*')],
            initialfilename='dashboard.html',
        )
        if path:
            try:
                write_html(self.current_issues, self.field_ids, path, self.current_jql)
                self.status_var.set(f'Exportado: {path}')
                messagebox.showinfo('Exportar', f'Arquivo salvo em:\n{path}')
            except Exception as e:
                messagebox.showerror('Exportar', str(e))

    def run(self):
        self.root.mainloop()


def main():
    app = L1DashboardApp()
    app.run()


if __name__ == '__main__':
    main()
