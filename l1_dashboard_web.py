#!/usr/bin/env python3
"""
L1 Dashboard - Versão web (Flask). Roda no navegador, sem Tkinter.
Use quando o app Tkinter falhar no macOS (ex.: "macOS 1507 required").
"""

import os
import sys
import sqlite3
import time
import webbrowser
from threading import Timer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# SQLite: persistência das notas (substitui planilha Google)
def _db_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'l1_notas.db')

def _init_db_notas():
    conn = sqlite3.connect(_db_path())
    conn.execute('''
        CREATE TABLE IF NOT EXISTS notas (
            issue_key TEXT PRIMARY KEY,
            nota INTEGER,
            comentario TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

from dotenv import load_dotenv
load_dotenv()

from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template_string, request, jsonify

from l1_dashboard import (
    JIRA_URL,
    get_jira_credentials,
    resolve_custom_fields,
    search_jql,
    get_row_values,
    get_row_values_for_columns,
    get_field_display_value,
    fetch_issue_sla,
    fetch_issue_sla_raw,
    _sla_name_is_relevant,
    get_issue_note_from_rovo,
    get_issue_note_from_agent,
    get_issue_note_from_ollama,
    get_issue_note_rule_based,
    fetch_issue_comments_text,
    stats_ttr_frt_by_request_type_from_sla,
    stats_other_by_keywords,
    stats_keyword_breakdown_by_request_type,
    stats_keyword_breakdown_by_request_type_ollama,
    stats_csat,
    stats_sla_aggregate,
    stats_sla_pct_by_period,
    stats_sla_by_analyst,
    stats_ttr_frt_by_period,
    stats_nota_temporal,
    stats_nota_by_request_type,
    stats_sla_by_request_type,
    stats_critical_pct_by_period,
    stats_csat_by_period,
    stats_csat_vs_nota,
    stats_csat_by_request_type,
    stats_volume_by_period,
    stats_volume_by_analyst,
    stats_reopened_for_period,
    stats_pontos_melhoria_fortes,
    DEFAULT_JQL,
    fetch_my_filters,
    fetch_filter_by_id,
    fetch_filter_columns,
    fetch_projects,
    fetch_statuses,
    _description_to_plain_text,
    get_issue_summary_and_description,
)

app = Flask(__name__)
_last_result = None  # { 'issues', 'field_ids', 'jql' } para exportar HTML
_last_notas = None   # { issue_key: { 'nota', 'comentario' } } após Calcular notas (Ollama)
_search_cache = None  # (key, expiry_time, data) para cache da busca (melhor desempenho)


def _sla_inline_html(slas, html_escape):
    """Exibe SLAs como na lista do Jira: cada item com ícone (— em andamento, ✓ cumprido), nome e pillbox com tempo/status."""
    if not slas:
        return '<span class="sla-na" title="Chamado pode não ser do Jira Service Management ou SLA não disponível">N/A</span>'
    parts = []
    for s in slas:
        ts = s.get('timestamp') or '—'
        met = s.get('met', True)
        ongoing = s.get('ongoing', False)
        name = s.get('name') or '—'
        # Ícone: — quando em andamento, ✓ quando cumprido, ✗ quando estourado
        if ongoing:
            icon_html = '<span class="sla-icon ongoing" aria-hidden="true">—</span>' + ('<span class="sla-icon met" aria-hidden="true">✓</span>' if met else '')
        else:
            icon_html = '<span class="sla-icon met" aria-hidden="true">✓</span>' if met else '<span class="sla-icon breached" aria-hidden="true">✗</span>'
        ts_pill = f'<span class="sla-time-pill">{html_escape(str(ts))}</span>' if ts and str(ts).strip() != '—' else ''
        parts.append(
            '<div class="sla-list-row">'
            + '<div class="sla-icons">' + icon_html + '</div>'
            + f'<div class="sla-list-body"><span class="sla-name">{html_escape(str(name))}</span>'
            + (f'<div class="sla-time-wrap">{ts_pill}</div>' if ts_pill else '')
            + '</div></div>'
        )
    return '<div class="sla-list-inline">' + ''.join(parts) + '</div>'


def _fetch_slas_for_issues(auth, issues, max_workers=12):
    """Busca SLAs para cada issue em paralelo via API do Jira (GET /rest/servicedeskapi/request/{key}/sla).
    Retorna dict issue_key -> list of sla dicts. É a única fonte dos dados de SLA exibidos no modo lista."""
    result = {}
    keys = [i.get('key') for i in issues if i.get('key')]
    if not keys:
        return result
    def fetch_one(key):
        try:
            return (key, fetch_issue_sla(auth, key))
        except Exception:
            return (key, [])
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, k): k for k in keys}
        for future in as_completed(futures):
            key, slas = future.result()
            result[key] = slas
    return result

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>L1 Dashboard - Jira</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    /* Nubank: roxo #820AD1, roxo claro #c484ec, roxo escuro #6406ac, fundo branco/cinza claro */
    :root {
      --nubank-purple: #820AD1;
      --nubank-purple-light: #c484ec;
      --nubank-purple-dark: #6406ac;
      --nubank-purple-vibrant: #8c1ce4;
      --nubank-bg: #f7f5f9;
      --nubank-card: #ffffff;
      --nubank-text: #443a53;
      --nubank-text-muted: #6b5b73;
      --nubank-border: #e8e0ed;
      --nubank-shadow: 0 2px 8px rgba(130, 10, 209, 0.06);
      --nubank-shadow-lg: 0 12px 40px rgba(130, 10, 209, 0.12);
      --radius: 16px;
      --radius-lg: 24px;
    }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--nubank-bg);
      min-height: 100vh;
      padding: 28px;
      color: var(--nubank-text);
      line-height: 1.5;
    }
    .container { max-width: 1440px; margin: 0 auto; }
    .header {
      background: linear-gradient(135deg, var(--nubank-purple) 0%, var(--nubank-purple-dark) 100%);
      color: #fff;
      border-radius: var(--radius-lg);
      padding: 36px 44px;
      margin-bottom: 28px;
      box-shadow: var(--nubank-shadow-lg);
    }
    .header h1 { font-size: 2.25rem; font-weight: 700; letter-spacing: -0.03em; margin-bottom: 8px; }
    .header p { opacity: 0.92; font-size: 1.05rem; font-weight: 500; }
    .card {
      background: var(--nubank-card);
      border-radius: var(--radius);
      padding: 28px;
      margin-bottom: 24px;
      box-shadow: var(--nubank-shadow);
      border: 1px solid var(--nubank-border);
    }
    label { display: block; margin-bottom: 8px; font-weight: 600; color: var(--nubank-text); font-size: 0.9rem; }
    textarea {
      width: 100%; min-height: 80px; padding: 14px 18px;
      border: 1px solid var(--nubank-border); border-radius: var(--radius);
      font-family: ui-monospace, "SF Mono", monospace; font-size: 13px;
      transition: border-color 0.2s, box-shadow 0.2s;
    }
    textarea:focus { outline: none; border-color: var(--nubank-purple); box-shadow: 0 0 0 3px rgba(130, 10, 209, 0.12); }
    .console-jql {
      background: #2d1b4e;
      border: 1px solid #3d2a5c;
      border-radius: var(--radius-lg);
      padding: 0;
      margin-bottom: 24px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
      overflow: hidden;
    }
    .console-jql .console-title {
      background: #3d2a5c;
      color: var(--nubank-purple-light);
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      padding: 14px 22px;
      border-bottom: 1px solid #4a3568;
      font-family: ui-monospace, monospace;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .console-jql .console-title::before { content: ""; width: 10px; height: 10px; border-radius: 50%; background: #e040fb; }
    .console-jql .console-body { padding: 22px; min-height: 100px; }
    .console-jql .console-prompt { color: var(--nubank-purple-light); font-family: ui-monospace, monospace; font-size: 14px; margin-bottom: 8px; }
    .console-jql .console-prompt span { color: var(--nubank-purple-light); }
    .console-jql #jql {
      background: transparent; border: none; color: var(--nubank-purple-light);
      font-family: ui-monospace, monospace; font-size: 14px; line-height: 1.6;
      min-height: 90px; resize: vertical;
    }
    .console-jql #jql::placeholder { color: rgba(196, 132, 236, 0.5); }
    .console-jql #jql:focus { outline: none; box-shadow: none; }
    .console-jql .row label { color: #9c8cac; }
    .console-jql .row input, .console-jql .row select {
      background: #3d2a5c; border: 1px solid #4a3568; color: #e8e0ed;
      border-radius: 12px;
    }
    .console-jql .row input::placeholder { color: #9c8cac; }
    .console-jql .row button { background: var(--nubank-purple); color: #fff; border: none; }
    .console-jql .row button:hover { background: var(--nubank-purple-vibrant); }
    .console-jql .row button.secondary { background: #5c3d7a; border: 1px solid #6b4d85; }
    .console-jql .row button.secondary:hover { background: #6b4d85; }
    .console-jql .row button:disabled { background: #443a53; color: #9c8cac; border: none; }
    input, select, button {
      padding: 12px 18px;
      border-radius: var(--radius);
      border: 1px solid var(--nubank-border);
      font-size: 14px;
      font-family: inherit;
    }
    input:focus, select:focus { outline: none; border-color: var(--nubank-purple); box-shadow: 0 0 0 2px rgba(130, 10, 209, 0.15); }
    button {
      background: var(--nubank-purple);
      color: #fff;
      border: none;
      cursor: pointer;
      font-weight: 600;
      transition: background 0.2s, transform 0.15s, box-shadow 0.2s;
      border-radius: var(--radius);
    }
    button:hover { background: var(--nubank-purple-vibrant); transform: translateY(-1px); box-shadow: 0 6px 20px rgba(130, 10, 209, 0.35); }
    button:disabled { background: #9c8cac; cursor: not-allowed; transform: none; box-shadow: none; }
    button.secondary { background: var(--nubank-purple-dark); }
    button.secondary:hover { background: var(--nubank-purple); box-shadow: 0 6px 20px rgba(100, 6, 172, 0.35); }
    .view-toggle {
      display: inline-flex;
      border-radius: var(--radius);
      overflow: hidden;
      border: 1px solid var(--nubank-border);
      background: var(--nubank-card);
      box-shadow: var(--nubank-shadow);
    }
    .view-toggle button { border-radius: 0; background: transparent; color: var(--nubank-text-muted); border: none; padding: 12px 22px; }
    .view-toggle button.active { background: var(--nubank-purple); color: white; }
    .view-toggle button:not(:last-child) { border-right: 1px solid var(--nubank-border); }
    .row { display: flex; gap: 14px; flex-wrap: wrap; align-items: center; margin-bottom: 10px; }
    .row label { margin: 0; margin-right: 4px; }
    .filtros { display: flex; flex-wrap: wrap; gap: 14px; align-items: center; margin-top: 12px; }
    .filtros select { min-width: 180px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 18px; align-items: center; margin-bottom: 28px; }
    .content {
      background: var(--nubank-card);
      border-radius: var(--radius-lg);
      padding: 36px;
      box-shadow: var(--nubank-shadow);
      border: 1px solid var(--nubank-border);
      min-height: 400px;
    }
    .table-wrap {
      overflow-x: auto;
      overflow-y: visible;
      margin-bottom: 18px;
      border-radius: var(--radius);
      border: 1px solid var(--nubank-border);
      box-shadow: var(--nubank-shadow);
      -webkit-overflow-scrolling: touch;
    }
    table { width: 100%; border-collapse: collapse; min-width: 1100px; table-layout: auto; }
    thead { background: linear-gradient(135deg, var(--nubank-purple) 0%, var(--nubank-purple-dark) 100%); color: white; }
    th { padding: 16px 20px; text-align: left; font-weight: 600; font-size: 0.8rem; white-space: nowrap; text-transform: uppercase; letter-spacing: 0.04em; }
    td { padding: 16px 20px; border-bottom: 1px solid var(--nubank-border); font-size: 0.9rem; vertical-align: top; }
    tbody tr:nth-child(even) { background: #faf8fc; }
    tbody tr:hover { background: rgba(130, 10, 209, 0.06); }
    tr.hidden-by-filter { display: none; }
    td.summary-cell { max-width: 240px; font-weight: 500; color: var(--nubank-text); }
    td.desc-cell {
      max-width: 320px; line-height: 1.55; color: var(--nubank-text-muted);
      overflow: hidden; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; word-break: break-word;
    }
    td.desc-cell:hover { overflow: visible; -webkit-line-clamp: unset; }
    th.satisfaction-cell, td.satisfaction-cell { min-width: 72px; max-width: 100px; word-break: break-word; white-space: normal; box-sizing: border-box; }
    th.nota-cell, td.nota-cell { min-width: 64px; max-width: 120px; word-break: break-word; white-space: normal; box-sizing: border-box; }
    a { color: var(--nubank-purple); text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; color: var(--nubank-purple-dark); }
    .msg { padding: 18px 22px; border-radius: var(--radius); margin-bottom: 22px; animation: slideIn 0.3s ease; }
    @keyframes slideIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
    .msg.error { background: #fde8e8; color: #9b1b1b; border: 1px solid #f5c2c2; }
    .msg.success { background: #e8f5e9; color: #1b5e20; border: 1px solid #a5d6a7; }
    .msg.info { background: #f3e5f5; color: #6a1b9a; border: 1px solid #ce93d8; }
    #result { margin-top: 28px; }
    .count { margin-top: 18px; color: var(--nubank-text-muted); font-size: 0.82rem; line-height: 1.6; }
    #chartContainer { display: none; margin-top: 28px; }
    #chartContainer.visible { display: block; }
    .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 360px), 1fr)); gap: 28px; margin-bottom: 28px; align-items: start; }
    .chart-section-title { font-size: 1.1rem; margin: 24px 0 12px; color: var(--nubank-text); border-bottom: 1px solid var(--border); padding-bottom: 8px; }
    .chart-sections { margin-top: 16px; }
    .chart-card {
      background: var(--nubank-card);
      padding: 30px;
      border-radius: var(--radius-lg);
      box-shadow: var(--nubank-shadow);
      border: 1px solid var(--nubank-border);
      min-height: 320px;
      width: 100%;
      box-sizing: border-box;
    }
    .chart-card.pie-card { min-height: 380px; }
    .pie-wrapper { display: flex; gap: 22px; align-items: flex-start; flex-wrap: wrap; }
    .pie-canvas-wrap { flex: 0 0 260px; max-height: 320px; }
    .pie-legend { flex: 1; min-width: 220px; max-height: 360px; overflow-y: auto; overflow-x: hidden; font-size: 12px; line-height: 1.6; }
    .pie-legend-item { display: flex; align-items: flex-start; gap: 12px; margin-bottom: 12px; padding: 8px 0; border-radius: 12px; transition: background 0.15s; }
    .pie-legend-item:hover { background: rgba(130, 10, 209, 0.08); }
    .pie-legend-color { width: 14px; height: 14px; border-radius: 6px; flex-shrink: 0; margin-top: 2px; }
    .pie-legend-label { flex: 1; min-width: 0; word-break: break-word; font-size: 12px; font-weight: 600; color: var(--nubank-text); }
    .chart-card h3 { margin: 0 0 22px 0; font-size: 1.1rem; color: var(--nubank-text); font-weight: 700; }
    .chart-card canvas { width: 100% !important; max-width: 100%; max-height: 400px; min-height: 240px; }
    .chart-card.pie-card canvas { max-height: 320px; min-height: 220px; }
    .chart-card #chartTop5Melhoria, .chart-card #chartTop5Fortes { min-height: 280px; max-height: 520px; }
    .chart-card #chartResolucao, .chart-card #chartPrimeiraResposta { min-height: 280px; }
    .chart-card #chartPie { min-height: 260px; }
    #chartPrimeiraRespostaWrapper, #chartResolucaoWrapper { min-height: 280px; position: relative; width: 100%; }
    .chart-card-parallax { margin-top: 24px; position: relative; z-index: 0; box-shadow: 0 4px 24px rgba(130, 10, 209, 0.08); transform: translateZ(0); }
    .chart-card-parallax::before { content: ''; position: absolute; left: 0; top: 0; right: 0; bottom: 0; border-radius: inherit; pointer-events: none; background: linear-gradient(135deg, rgba(130, 10, 209, 0.02) 0%, transparent 50%); }
    .chart-empty { text-align: center; padding: 52px 28px; color: var(--nubank-text-muted); font-size: 0.92rem; }
    .chart-hint { margin: 8px 0 0 0; font-size: 0.8rem; color: var(--nubank-text-muted); }
    .pontos-tickets { display: block; margin-top: 4px; font-size: 0.85rem; color: var(--nubank-text-muted); }
    .pontos-tickets a { color: var(--nubank-purple); text-decoration: none; }
    .pontos-tickets a:hover { text-decoration: underline; }
    .pontos-abnt { font-family: 'Times New Roman', Times, serif; font-size: 12pt; line-height: 1.5; text-align: justify; margin: 0 0 1em 0; }
    .pontos-abnt ul { margin: 0.5em 0; padding-left: 1.5em; }
    .pontos-abnt li { margin-bottom: 0.25em; }
    .pontos-texto-completo { white-space: normal; word-wrap: break-word; overflow-wrap: break-word; max-width: 100%; overflow: visible; }
    #pontosRelatorioPorChamado .pontos-chamado-block ul { overflow: visible; max-height: none; }
    #pontosRelatorioCard { overflow: visible; }
    #pontosRelatorioPorChamado { overflow: visible; }
    .pontos-chamado-block { overflow: visible; }
    .pontos-chamado-block ul, .pontos-chamado-block ul li { white-space: normal; word-wrap: break-word; overflow-wrap: break-word; overflow: visible; max-width: 100%; }
    #pontosRelatorioCard .pontos-chamado-block { font-family: 'Times New Roman', Times, serif; font-size: 12pt; line-height: 1.5; }
    #pontosRelatorioCard .pontos-chamado-block ul { margin: 0.5em 0; padding-left: 1.5em; text-align: justify; }
    .pontos-duas-colunas { display: grid !important; grid-template-columns: minmax(220px, 1fr) minmax(220px, 1fr) !important; gap: 24px; align-items: start; }
    .pontos-col-esquerda { min-width: 0; overflow: visible; }
    .pontos-col-direita { min-width: 0; overflow: visible; }
    #pontosRelatorioMelhoria li, #pontosRelatorioFortes li { white-space: normal; word-wrap: break-word; overflow-wrap: break-word; margin-bottom: 0.5em; }
    .btn-sla { font-size: 12px; padding: 8px 14px; cursor: pointer; background: rgba(130, 10, 209, 0.1); color: var(--nubank-purple); border: 1px solid var(--nubank-purple); border-radius: 10px; font-weight: 600; }
    .btn-sla:hover { background: var(--nubank-purple); color: #fff; }
    #slaOverlay { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(68, 58, 83, 0.5); z-index: 1000; align-items: center; justify-content: center; backdrop-filter: blur(6px); }
    #slaOverlay.show { display: flex; }
    #slaPanel {
      background: var(--nubank-card); border-radius: var(--radius-lg); overflow: hidden;
      max-width: 440px; width: 90%; max-height: 80vh; display: flex; flex-direction: column;
      box-shadow: var(--nubank-shadow-lg); border: 1px solid var(--nubank-border);
    }
    #slaPanel .sla-panel-header { background: linear-gradient(135deg, var(--nubank-purple) 0%, var(--nubank-purple-dark) 100%); color: #fff; padding: 16px 20px; font-size: 1.1rem; font-weight: 700; border-radius: var(--radius-lg) var(--radius-lg) 0 0; display: flex; align-items: center; justify-content: space-between; }
    #slaPanel .sla-close { background: none; border: none; font-size: 22px; cursor: pointer; color: rgba(255,255,255,0.9); line-height: 1; padding: 0 4px; }
    #slaPanel .sla-close:hover { color: #fff; }
    #slaPanel .sla-panel-body { padding: 16px 20px; overflow: auto; }
    #slaPanel h3 { margin: 0 0 18px 0; color: var(--nubank-text); font-size: 1.15rem; font-weight: 700; }
    #slaPanel .sla-list { list-style: none; padding: 0; margin: 0; }
    #slaPanel .sla-item { margin-bottom: 14px; padding: 14px; background: #faf8fc; border-radius: var(--radius); border: 1px solid var(--nubank-border); display: flex; align-items: flex-start; gap: 12px; }
    .tickets-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; z-index: 1100; display: flex; align-items: center; justify-content: center; }
    .tickets-modal-backdrop { position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: rgba(68, 58, 83, 0.5); cursor: pointer; backdrop-filter: blur(6px); }
    .tickets-modal-box {
      position: relative; background: var(--nubank-card); border-radius: var(--radius-lg);
      box-shadow: var(--nubank-shadow-lg); max-width: 500px; width: 92%; max-height: 80vh;
      display: flex; flex-direction: column; border: 1px solid var(--nubank-border);
    }
    .tickets-modal-header {
      display: flex; align-items: center; justify-content: space-between;
      padding: 22px 28px; border-bottom: 1px solid var(--nubank-border);
      background: #faf8fc;
      border-radius: var(--radius-lg) var(--radius-lg) 0 0;
    }
    .tickets-modal-header h4 { margin: 0; font-size: 1.15rem; color: var(--nubank-text); font-weight: 700; }
    .tickets-modal-close { background: none; border: none; font-size: 26px; line-height: 1; cursor: pointer; color: var(--nubank-text-muted); padding: 0 4px; }
    .tickets-modal-close:hover { color: var(--nubank-text); }
    .tickets-modal-body { padding: 22px 28px; overflow-y: auto; max-height: 60vh; }
    .tickets-modal-body a { display: block; padding: 12px 0; color: var(--nubank-purple); text-decoration: none; font-weight: 600; border-bottom: 1px solid var(--nubank-border); }
    .tickets-modal-body a:last-child { border-bottom: none; }
    .tickets-modal-body a:hover { background: rgba(130, 10, 209, 0.08); margin: 0 -28px; padding: 12px 28px; text-decoration: none; }
    #slaPanel .sla-item .sla-time-pill { display: inline-block; background: #e5e7eb; color: #374151; padding: 6px 12px; border-radius: 10px; font-size: 12px; margin-top: 6px; font-weight: 500; border: 1px solid #d1d5db; }
    #slaPanel .sla-item .sla-icons { display: flex; align-items: center; gap: 4px; flex-shrink: 0; }
    #slaPanel .sla-item .sla-icon { width: 22px; height: 22px; min-width: 22px; min-height: 22px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 700; }
    #slaPanel .sla-item .sla-icon.ongoing { background: #d1d5db; color: #fff; }
    #slaPanel .sla-item .sla-icon.met { background: rgba(34, 197, 94, 0.25); color: #16a34a; }
    #slaPanel .sla-item .sla-icon.breached { background: rgba(220, 38, 38, 0.2); color: #dc2626; }
    #slaPanel .sla-item .sla-name { font-size: 13px; color: var(--nubank-text); font-weight: 500; }
    #slaPanel .sla-empty { color: var(--nubank-text-muted); font-size: 13px; }
    .sla-list-inline { font-size: 13px; line-height: 1.4; }
    .sla-list-row { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 12px; }
    .sla-list-row:last-child { margin-bottom: 0; }
    .sla-list-row .sla-icons { display: flex; align-items: center; gap: 4px; flex-shrink: 0; }
    td.sla-cell .sla-list-row .sla-icon { width: 22px; height: 22px; min-width: 22px; min-height: 22px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 700; flex-shrink: 0; line-height: 1; }
    td.sla-cell .sla-list-row .sla-icon.ongoing { background: #d1d5db; color: #fff; }
    td.sla-cell .sla-list-row .sla-icon.met { background: rgba(34, 197, 94, 0.25); color: #16a34a; }
    td.sla-cell .sla-list-row .sla-icon.breached { background: rgba(220, 38, 38, 0.2); color: #dc2626; }
    td.sla-cell .sla-list-body .sla-name { color: var(--nubank-text); font-weight: 500; display: block; }
    td.sla-cell .sla-time-pill { display: inline-block; background: #e5e7eb; color: #374151; padding: 6px 12px; border-radius: 10px; font-size: 12px; margin-top: 6px; font-weight: 500; border: 1px solid #d1d5db; }
    .sla-debug { margin-top: 14px; font-size: 11px; }
    .sla-debug summary { cursor: pointer; color: var(--nubank-purple); }
    .sla-debug pre { background: #f7f5f9; padding: 14px; border-radius: 10px; overflow: auto; max-height: 200px; white-space: pre-wrap; word-break: break-all; font-size: 11px; border: 1px solid var(--nubank-border); }
    td .sla-time { display: inline-block; background: #e8e0ed; color: var(--nubank-text); padding: 6px 10px; border-radius: 10px; font-size: 12px; margin-right: 6px; font-weight: 500; }
    td .sla-icon { font-weight: bold; margin-right: 4px; }
    td .sla-icon.met { color: #2e7d32; }
    td .sla-icon.breached { color: #c62828; }
    td.sla-cell .sla-inline-item { display: block; margin-bottom: 8px; font-size: 12px; }
    td.sla-cell .sla-inline-item:last-child { margin-bottom: 0; }
    td.sla-cell .sla-name { color: var(--nubank-text-muted); margin-left: 2px; }
    td.sla-cell .sla-na { color: var(--nubank-text-muted); font-size: 12px; }
    .sla-tipo-badge { display: inline-block; margin-right: 6px; padding: 2px 6px; border-radius: 6px; font-size: 10px; font-weight: 700; }
    .sla-tipo-badge.sla-tipo-FRT { background: rgba(5, 150, 105, 0.15); color: #059669; }
    .sla-tipo-badge.sla-tipo-FRT.sla-estourado { background: rgba(220, 38, 38, 0.2); color: #dc2626; }
    .sla-tipo-badge.sla-tipo-TTR { background: rgba(130, 10, 209, 0.15); color: var(--nubank-purple); }
    .sla-tipo-badge.sla-tipo-TTR.sla-estourado { background: rgba(220, 38, 38, 0.2); color: #dc2626; }
    .sla-tipo-badge.sla-tipo-outro { background: rgba(100, 100, 100, 0.15); color: var(--nubank-text-muted); }
    .sla-tipo-badge.sla-tipo-outro.sla-estourado { background: rgba(220, 38, 38, 0.2); color: #dc2626; }
    th.sla-header { min-width: 140px; width: 140px; white-space: nowrap; position: sticky; right: 0; background: linear-gradient(135deg, var(--nubank-purple) 0%, var(--nubank-purple-dark) 100%); box-shadow: -4px 0 8px rgba(0,0,0,0.08); z-index: 1; }
    .table-wrap th:last-child { min-width: 180px; white-space: nowrap; }
    td.sla-cell { min-width: 180px; vertical-align: top; position: sticky; right: 0; background: var(--nubank-card); box-shadow: -4px 0 8px rgba(0,0,0,0.06); z-index: 0; }
    tbody tr:nth-child(even) td.sla-cell { background: #faf8fc; }
    tbody tr:hover td.sla-cell { background: rgba(130, 10, 209, 0.06); }
  </style>
</head>
<body>
  <div id="slaOverlay">
    <div id="slaPanel">
      <div class="sla-panel-header">
        <span id="slaTitle">SLAs</span>
        <button type="button" class="sla-close" aria-label="Fechar">&times;</button>
      </div>
      <div class="sla-panel-body" id="slaContent"></div>
    </div>
  </div>
  <div class="container">
    <div class="header">
      <h1>L1 Dashboard — Jira</h1>
      <p>Chamados L1, SLAs e notas por Request Type</p>
    </div>

  <div class="console-jql">
    <div class="console-title">L1 JQL Console</div>
    <div class="console-body">
      <div class="console-prompt"><span>jira@l1&gt;</span></div>
      <textarea id="jql" name="jql" placeholder="(component not in (gadgets-devices) OR component IS EMPTY) AND &quot;Support Level - ITOPS&quot; = &quot;L1&quot; ORDER BY created DESC">{{ default_jql | e }}</textarea>
    </div>
    <div class="card" style="margin: 0; border-radius: 0; border-top: 1px solid #30363d; background: #161b22; padding: 14px 20px;">
    <div class="row" style="margin-top: 0;">
      <label for="limit">Limite</label>
      <input type="number" id="limit" name="limit" value="50" min="1" max="3000" style="width: 70px;">
      <label for="monthSelect">Mês</label>
      <select id="monthSelect" style="width: 100px;">
        <option value="">Todos</option>
        <option value="1">Jan</option><option value="2">Fev</option><option value="3">Mar</option>
        <option value="4">Abr</option><option value="5">Mai</option><option value="6">Jun</option>
        <option value="7">Jul</option><option value="8">Ago</option><option value="9">Set</option>
        <option value="10">Out</option><option value="11">Nov</option><option value="12">Dez</option>
      </select>
      <label for="yearSelect">Ano</label>
      <select id="yearSelect" style="width: 80px;">
        <option value="">—</option>
        {% for y in years_list or [] %}<option value="{{ y }}">{{ y }}</option>{% endfor %}
      </select>
      <button type="button" id="btnBuscar">Buscar</button>
      <button type="button" id="btnExport" class="secondary" disabled>Exportar HTML</button>
      <button type="button" id="btnNotas" class="secondary" disabled title="Usa Ollama (local). Configure OLLAMA_URL no .env. Cenário 2 SLA Crítico (Assignee + Comments).">Calcular notas (Ollama)</button>
      <button type="button" id="btnNotasRestante" class="secondary" disabled title="Calcula nota só dos chamados que ainda não têm nota.">Calcular restante</button>
      <button type="button" id="btnReavaliarNotas" class="secondary" disabled title="Reavalia todas as notas do resultado atual (ignora cache e banco).">Reavaliar notas</button>
    </div>
    </div>
  </div>

  <div class="card">
    <label>Filtros do Jira</label>
    <div class="filtros">
      <button type="button" id="btnCarregarFiltros">Carregar meus filtros</button>
      <label for="filterSelect">Filtro salvo</label>
      <select id="filterSelect" disabled><option value="">—</option></select>
      <label for="projectSelect">Projeto</label>
      <select id="projectSelect" disabled><option value="">—</option></select>
      <label for="statusSelect">Status</label>
      <select id="statusSelect" disabled><option value="">—</option></select>
      <button type="button" id="btnAplicarFiltros" class="secondary" disabled>Aplicar à JQL</button>
    </div>
  </div>

  <div class="content">
  <div id="message"></div>
  <div class="toolbar" id="toolbar" style="display: none;">
    <div class="view-toggle">
      <button type="button" id="btnViewLista" class="active">Lista</button>
      <button type="button" id="btnViewGrafico">Gráfico</button>
    </div>
    <label for="filterRequestType">Request Type</label>
    <select id="filterRequestType" style="min-width: 200px;">
      <option value="">Todos</option>
    </select>
  </div>
  <div id="result"></div>
  <div id="chartContainer">
    <div class="chart-grid">
      <div class="chart-card pie-card"><h3>Chamados por Request Type</h3><div class="pie-wrapper"><div class="pie-canvas-wrap"><canvas id="chartPie"></canvas></div><div id="chartPieLegend" class="pie-legend" title="Legenda completa"></div></div></div>
      <div class="chart-card"><h3>Tempo médio de resolução (horas)</h3><canvas id="chartResolucao"></canvas></div>
      <div class="chart-card"><h3>Tempo médio 1ª resposta (horas)</h3><div id="chartPrimeiraRespostaWrapper"><p id="chartPrimeiraRespostaEmpty" class="chart-empty" style="display: none;">Nenhum dado de 1ª resposta (coluna SLA do modo lista) para este período.</p><canvas id="chartPrimeiraResposta"></canvas></div></div>
      <div class="chart-card pie-card" id="chartSubcategoriaCard"><h3>Subcategorias por palavra-chave na descrição</h3><div class="row" style="margin-bottom: 12px;"><label for="chartSubcategoriaRequestType" style="margin-right: 8px;">Request Type</label><select id="chartSubcategoriaRequestType" style="min-width: 200px;"></select></div><div class="pie-wrapper"><div class="pie-canvas-wrap"><canvas id="chartSubcategoria"></canvas></div><div id="chartSubcategoriaLegend" class="pie-legend"></div></div><p id="chartSubcategoriaEmpty" class="chart-empty" style="display: none;">Nenhum ticket para o Request Type selecionado.</p></div>
      <div class="chart-card" id="chartCsatCard"><h3>CSAT (Satisfaction 1–5 estrelas)</h3><p id="chartCsatEmpty" class="chart-empty" style="display: none;">Nenhum dado na coluna Satisfaction do Jira para este per\u00edodo.</p><div id="chartCsatWrapper"><canvas id="chartCsat"></canvas></div><p id="chartCsatAverage" style="margin-top: 12px; font-weight: 600; color: var(--nubank-text);"></p></div>
    </div>
    <div class="chart-sections" style="margin-top: 24px;">
      <h2 class="chart-section-title">1) SLA e Efici\u00eancia</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>% tickets dentro do SLA (TTR + FRT)</h3><p id="slaPctSummary" style="font-weight: 600; color: var(--nubank-text);"></p><p id="slaPctEmpty" class="chart-empty" style="display: none;">Sem dados de SLA (projeto pode n\u00e3o ser Service Desk).</p></div>
        <div class="chart-card"><h3>TTR e FRT medianos por m\u00eas</h3><canvas id="chartTtrFrtPeriod"></canvas></div>
        <div class="chart-card"><h3>Distribui\u00e7\u00e3o de viola\u00e7\u00f5es de SLA</h3><canvas id="chartSlaViolations"></canvas></div>
      </div>
      <h2 class="chart-section-title">1b) SLA e velocidade (cr\u00edticos)</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>% dentro do SLA por m\u00eas</h3><canvas id="chartSlaPctPeriod"></canvas></div>
        <div class="chart-card"><h3>TTR e FRT medianos (linha temporal)</h3><canvas id="chartTtrFrtLine"></canvas></div>
      </div>
      <h2 class="chart-section-title">2) Qualidade t\u00e9cnica</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>M\u00e9dia Nota Solu\u00e7\u00e3o (Ollama) por m\u00eas</h3><canvas id="chartNotaSolucaoPeriod"></canvas></div>
        <div class="chart-card"><h3>Distribui\u00e7\u00e3o das notas de Solu\u00e7\u00e3o</h3><canvas id="chartNotaSolucaoDist"></canvas></div>
        <div class="chart-card"><p class="chart-empty">Categoriza\u00e7\u00e3o e Preenchimento: requer campos no Jira para m\u00e9dias por per\u00edodo.</p></div>
      </div>
      <h2 class="chart-section-title">3) Resultado final (score composto)</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Nota final m\u00e9dia (linha)</h3><canvas id="chartNotaFinalLine"></canvas></div>
        <div class="chart-card"><h3>Distribui\u00e7\u00e3o da nota final</h3><canvas id="chartNotaFinalDist"></canvas></div>
      </div>
      <h2 class="chart-section-title">4) Analistas (performance e coaching)</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Top 10 analistas por nota final</h3><canvas id="chartTop10Analysts"></canvas></div>
        <div class="chart-card"><h3>Bottom 10 analistas por nota final</h3><canvas id="chartBottom10Analysts"></canvas></div>
        <div class="chart-card"><h3>SLA por analista (% dentro / fora)</h3><canvas id="chartSlaByAnalyst"></canvas></div>
      </div>
      <h2 class="chart-section-title">5) CSAT (cliente)</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>CSAT m\u00e9dio por per\u00edodo</h3><canvas id="chartCsatLine"></canvas></div>
        <div class="chart-card"><h3>CSAT vs Nota Final</h3><canvas id="chartCsatVsNota2"></canvas></div>
      </div>
      <h2 class="chart-section-title">6) Mix e tend\u00eancia de categorias</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Notas por Request Type</h3><canvas id="chartNotaByRt"></canvas></div>
        <div class="chart-card"><h3>SLA por Request Type (% dentro)</h3><canvas id="chartSlaByRt"></canvas></div>
      </div>
      <h2 class="chart-section-title">7) Alertas e risco</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>% tickets cr\u00edticos (Satisfaction 1\u20132) por m\u00eas</h3><canvas id="chartCriticalPctPeriod"></canvas><p class="chart-hint">Baseado na coluna Satisfaction do Jira. Clique em um mês (ponto da linha) para abrir a lista de tickets críticos; cada ticket é clicável para abrir no Jira.</p></div>
        <div class="chart-card"><p class="chart-empty">% com requer_auditoria_humana = true: requer campo no Jira. Configure para habilitar.</p></div>
      </div>
      <h2 class="chart-section-title">2) Qualidade e Resultado Final</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Nota final m\u00e9dia por m\u00eas</h3><canvas id="chartNotaPeriod"></canvas></div>
        <div class="chart-card"><h3>Distribui\u00e7\u00e3o das notas finais</h3><canvas id="chartNotaDist"></canvas></div>
        <div class="chart-card"><h3>Top 10 analistas por nota final</h3><canvas id="chartTopAnalysts"></canvas></div>
      </div>
      <h2 class="chart-section-title">3) CSAT e Experi\u00eancia do Cliente</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>CSAT m\u00e9dio por m\u00eas</h3><canvas id="chartCsatPeriod"></canvas></div>
        <div class="chart-card"><h3>CSAT vs Nota Final</h3><canvas id="chartCsatVsNota"></canvas></div>
        <div class="chart-card"><h3>CSAT por Request Type</h3><canvas id="chartCsatByRt"></canvas></div>
      </div>
      <h2 class="chart-section-title">4) Reabertura</h2>
      <div class="chart-grid">
        <div class="chart-card" id="chartReaberturaCard">
          <h3>Tickets reabertos no per\u00edodo</h3>
          <p id="chartReaberturaEmpty" class="chart-empty">Selecione m\u00eas e ano e busque para ver reaberturas no mesmo per\u00edodo (JQL: status CHANGED FROM Done/Closed/Resolved).</p>
          <div id="chartReaberturaWrapper" style="display: none;">
            <canvas id="chartReabertura"></canvas>
            <p id="chartReaberturaTotal" style="margin-top: 12px; font-weight: 600; color: var(--nubank-text);"></p>
            <p class="chart-hint">Mesmo per\u00edodo aplicado no modo lista. Clique no gr\u00e1fico para ver os tickets.</p>
          </div>
        </div>
      </div>
      <h2 class="chart-section-title">5) Auditoria Humana (efic\u00e1cia da IA)</h2>
      <div class="chart-grid"><div class="chart-card"><p class="chart-empty">Requer campo \u201crequer_auditoria_humana\u201d e notas IA vs humana no Jira ou integra\u00e7\u00e3o. Configure para habilitar.</p></div></div>
      <h2 class="chart-section-title">6) Pontos positivos e pontos negativos</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Top 5 pontos negativos (pontos de melhoria)</h3><p class="chart-hint" style="font-size: 0.85rem; color: var(--nubank-text-muted); margin-top: 0;">SLA fora do prazo; linguagem n\u00e3o corporativa/resolutiva/\u00e1gil; sem @Reporter; Satisfaction baixo</p><p id="pontosMelhoriaEmpty" class="chart-empty">Clique em &quot;Extrair pontos (Ollama)&quot; ap\u00f3s uma busca.</p><canvas id="chartTop5Melhoria"></canvas></div>
        <div class="chart-card"><h3>Top 5 pontos positivos (pontos fortes)</h3><p class="chart-hint" style="font-size: 0.85rem; color: var(--nubank-text-muted); margin-top: 0;">SLA dentro do prazo; linguagem corporativa, resolutiva, \u00e1gil; @Reporter marcado; Satisfaction alto</p><p id="pontosFortesEmpty" class="chart-empty">Clique em &quot;Extrair pontos (Ollama)&quot; ap\u00f3s uma busca.</p><canvas id="chartTop5Fortes"></canvas></div>
        <div class="chart-card"><h3>Regras n\u00e3o seguidas por Request Type</h3><p id="pontosMelhoriaRtEmpty" class="chart-empty">Clique em &quot;Extrair pontos (Ollama)&quot; ap\u00f3s uma busca.</p><div style="margin-bottom: 8px;"><label for="pontosMelhoriaRtSelect">Request Type</label><select id="pontosMelhoriaRtSelect" style="min-width: 180px; margin-left: 8px;"></select></div><canvas id="chartMelhoriaByRt"></canvas></div>
      </div>
      <div class="chart-card pontos-abnt" id="pontosRelatorioCard" style="margin-top: 12px;">
        <h3>Relat\u00f3rio: pontos negativos e pontos positivos por chamado (ABNT NBR 14724)</h3>
        <div id="pontosRelatorioEmpty" class="chart-empty">Clique em &quot;Extrair pontos (Ollama)&quot; ap\u00f3s uma busca. An\u00e1lise por ticket: SLA (Jira), Comments do Assignee, Satisfaction. Um lado: pontos negativos (melhoria); outro: pontos positivos (fortes).</div>
        <div id="pontosRelatorioPorChamado" style="display: none; margin-top: 12px;"></div>
        <div id="pontosRelatorioContent" style="display: none; margin-top: 12px;">
          <div class="pontos-duas-colunas">
            <div class="pontos-col-esquerda"><h4 style="color: #dc2626; margin: 0 0 8px 0;">Pontos negativos (pontos de melhoria)</h4><ul id="pontosRelatorioMelhoria" style="margin: 0; padding-left: 20px;"></ul></div>
            <div class="pontos-col-direita"><h4 style="color: #059669; margin: 0 0 8px 0;">Pontos positivos (pontos fortes)</h4><ul id="pontosRelatorioFortes" style="margin: 0; padding-left: 20px;"></ul></div>
          </div>
        </div>
        <details class="pontos-legenda" style="margin-top: 16px; padding: 12px; background: #f8f7fa; border-radius: 10px; border: 1px solid #e5e2eb;">
          <summary style="cursor: pointer; font-weight: 600; color: var(--nubank-purple);">Legenda dos termos</summary>
          <dl style="margin: 12px 0 0 0; font-size: 0.9rem; line-height: 1.6; color: var(--nubank-text);">
            <dt style="font-weight: 700; margin-top: 8px;">FRT</dt>
            <dd style="margin: 0 0 0 16px;">Time to First Response \u2014 tempo at\u00e9 a primeira resposta do analista ao chamado.</dd>
            <dt style="font-weight: 700; margin-top: 8px;">TTR</dt>
            <dd style="margin: 0 0 0 16px;">Time to Resolution \u2014 tempo total at\u00e9 a resolu\u00e7\u00e3o do chamado (cria\u00e7\u00e3o at\u00e9 fechamento).</dd>
            <dt style="font-weight: 700; margin-top: 8px;">SLA</dt>
            <dd style="margin: 0 0 0 16px;">Service Level Agreement \u2014 acordo de n\u00edvel de servi\u00e7o; metas de prazo (ex.: FRT em 30 min, TTR em 20 h). Dentro do SLA = cumprido; estourado = fora do prazo.</dd>
            <dt style="font-weight: 700; margin-top: 8px;">CSAT</dt>
            <dd style="margin: 0 0 0 16px;">Customer Satisfaction \u2014 satisfa\u00e7\u00e3o do cliente; no dashboard, o campo Satisfaction (escala 1 a 5).</dd>
            <dt style="font-weight: 700; margin-top: 8px;">TAPI</dt>
            <dd style="margin: 0 0 0 16px;">Toler\u00e2ncia \u00e0 Resposta Instant\u00e2nea \u2014 m\u00e9trica relacionada \u00e0 expectativa de resposta r\u00e1pida (escala/avalia\u00e7\u00e3o).</dd>
            <dt style="font-weight: 700; margin-top: 8px;">@Reporter</dt>
            <dd style="margin: 0 0 0 16px;">Men\u00e7\u00e3o ao solicitante do chamado nos coment\u00e1rios (boas pr\u00e1ticas de comunica\u00e7\u00e3o).</dd>
          </dl>
        </details>
      </div>
      <div class="row" style="margin-top: 8px;"><button type="button" id="btnPontosOllama" class="secondary" disabled title="Analisa por ticket: SLA (Jira), Comments do Assignee, Satisfaction. Pontos negativos: SLA fora do prazo, linguagem n\u00e3o corporativa/\u00e1gil, sem @Reporter. Pontos positivos: o inverso (Ollama, at\u00e9 12 chamados).">Extrair pontos (Ollama)</button></div>
      <h2 class="chart-section-title">7) Carga e Volume</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Tickets por m\u00eas</h3><canvas id="chartVolumePeriod"></canvas></div>
        <div class="chart-card"><h3>Volume por analista</h3><canvas id="chartVolumeAnalyst"></canvas></div>
      </div>
      <h2 class="chart-section-title">8) ROI / Impacto estrat\u00e9gico</h2>
      <div class="chart-grid"><div class="chart-card"><p class="chart-empty">Requer m\u00e9tricas de tempo de auditoria e % automatizado (integra\u00e7\u00e3o com processo). Configure para habilitar.</p></div></div>
    </div>
  </div>
  <p class="count" style="margin-top: 12px;">
    <strong>Time to resolution:</strong> calculado (created → resolved) quando o issue está resolvido; <em>Em aberto</em> quando não há data de resolução.<br>
    <strong>Time to first response:</strong> campo do Jira; quando vazio, calculado pelo primeiro comentário.
  </p>
  </div>
  </div>

  <div id="ticketsModal" class="tickets-modal" style="display: none;">
    <div class="tickets-modal-backdrop"></div>
    <div class="tickets-modal-box">
      <div class="tickets-modal-header">
        <h4 id="ticketsModalTitle">Tickets</h4>
        <button type="button" class="tickets-modal-close" id="ticketsModalClose" aria-label="Fechar">&times;</button>
      </div>
      <div class="tickets-modal-body" id="ticketsModalBody"></div>
    </div>
  </div>

  <script>
    const jqlEl = document.getElementById('jql');
    const limitEl = document.getElementById('limit');
    const monthSelect = document.getElementById('monthSelect');
    const yearSelect = document.getElementById('yearSelect');
    if (yearSelect && yearSelect.options.length <= 1) {
      var y = new Date().getFullYear();
      for (var i = y; i >= y - 5; i--) yearSelect.appendChild(new Option(i, i));
    }
    const btnBuscar = document.getElementById('btnBuscar');
    const btnExport = document.getElementById('btnExport');
    const btnCarregarFiltros = document.getElementById('btnCarregarFiltros');
    const filterSelect = document.getElementById('filterSelect');
    const projectSelect = document.getElementById('projectSelect');
    const statusSelect = document.getElementById('statusSelect');
    const btnAplicarFiltros = document.getElementById('btnAplicarFiltros');
    const messageEl = document.getElementById('message');
    const resultEl = document.getElementById('result');
    window.viewMode = 'lista';
    window.lastStats = { byRequestType: {}, requestTypeList: [] };
    window.chartInstances = [];
    window.lastRequestTypeKeys = {};
    window.jiraBaseUrl = '';
    window.lastPontosMelhoria = null;

    if (typeof Chart !== 'undefined') {
      Chart.defaults.layout.padding = 18;
      Chart.defaults.font.size = 12;
      Chart.defaults.responsive = true;
      Chart.defaults.maintainAspectRatio = true;
    }
    var barChartScaleX = { ticks: { maxRotation: 50, minRotation: 25, font: { size: 11 }, autoSkip: false } };
    var barChartScaleY = { beginAtZero: true, ticks: { font: { size: 11 } } };
    var MAX_TTR_FRT_PER_CHART = 12;
    var horBarOptions = function(xSuggestedMax) {
      var xScale = Object.assign({}, barChartScaleY);
      if (xSuggestedMax != null) xScale.suggestedMax = xSuggestedMax;
      return { responsive: true, maintainAspectRatio: true, layout: { padding: 18 }, indexAxis: 'y',
        scales: { x: xScale, y: { ticks: { font: { size: 11 }, autoSkip: false } } },
        plugins: { legend: { display: false } } };
    };

    function showTicketsModal(title, keys) {
      var modal = document.getElementById('ticketsModal');
      var titleEl = document.getElementById('ticketsModalTitle');
      var bodyEl = document.getElementById('ticketsModalBody');
      if (!modal || !bodyEl) return;
      titleEl.textContent = title;
      var base = window.jiraBaseUrl || '';
      var hint = (keys && keys.length > 0 && base) ? '<p class="chart-hint" style="margin-bottom: 12px;">Clique em um chamado para abrir no Jira.</p>' : '';
      bodyEl.innerHTML = hint + (keys || []).map(function(k) {
        var href = base ? (base + '/browse/' + encodeURIComponent(k)) : '#';
        return '<a href="' + href + '" target="_blank" rel="noopener" class="ticket-link">' + (k.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')) + '</a>';
      }).join('') || '<p class="chart-empty">Nenhum ticket.</p>';
      modal.style.display = 'flex';
    }
    function closeTicketsModal() {
      var modal = document.getElementById('ticketsModal');
      if (modal) modal.style.display = 'none';
    }
    document.getElementById('ticketsModalClose').onclick = closeTicketsModal;
    document.querySelector('.tickets-modal-backdrop').onclick = closeTicketsModal;

    function applyRequestTypeFilter() {
      var rt = (document.getElementById('filterRequestType').value || '').trim();
      resultEl.querySelectorAll('tbody tr').forEach(function(tr) {
        var rowRt = (tr.getAttribute('data-request-type') || '').trim();
        if (!rt || rowRt === rt) tr.classList.remove('hidden-by-filter');
        else tr.classList.add('hidden-by-filter');
      });
    }

    function renderCharts() {
      var stats = window.lastStats;
      if (!stats || !stats.byRequestType) return;
      var byRt = stats.byRequestType;
      var list = stats.requestTypeList || Object.keys(byRt);
      var colors = ['#820AD1','#8c1ce4','#6406ac','#c484ec','#ac6b95','#9c8cac','#5c3d7a','#443a53'];
      window.chartInstances.forEach(function(c) { if (c) c.destroy(); });
      window.chartInstances = [];
      var pieCtx = document.getElementById('chartPie');
      if (pieCtx) {
        var counts = list.map(function(rt) { return byRt[rt] ? byRt[rt].count : 0; });
        var pieLabels = list.map(function(rt) { var c = byRt[rt] ? byRt[rt].count : 0; return rt + ' (' + c + ')'; });
        var bgColors = list.map(function(_, i) { return colors[i % colors.length]; });
        var pie = new Chart(pieCtx.getContext('2d'), {
          type: 'pie',
          data: {
            labels: pieLabels,
            datasets: [{ data: counts, backgroundColor: bgColors }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: true,
            layout: { padding: 18 },
            plugins: { legend: { display: false } },
            onClick: function(ev, elements) {
              if (elements.length === 0) return;
              var idx = elements[0].index;
              var rt = list[idx];
              var keys = (window.lastRequestTypeKeys || {})[rt] || [];
              showTicketsModal(rt + ' (' + keys.length + ' chamados)', keys);
            }
          }
        });
        window.chartInstances.push(pie);
        var legendEl = document.getElementById('chartPieLegend');
        if (legendEl) {
          legendEl.innerHTML = pieLabels.map(function(label, i) {
            var safe = (label + '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
            var rt = list[i];
            var keyCount = ((window.lastRequestTypeKeys || {})[rt] || []).length;
            return '<div class="pie-legend-item" style="cursor: pointer;" title="Clique para listar ' + keyCount + ' ticket(s)"><span class="pie-legend-color" style="background:' + bgColors[i] + '"></span><span class="pie-legend-label">' + safe + '</span></div>';
          }).join('');
          legendEl.querySelectorAll('.pie-legend-item').forEach(function(el, i) {
            el.onclick = function() {
              var rt = list[i];
              var keys = (window.lastRequestTypeKeys || {})[rt] || [];
              showTicketsModal(rt + ' (' + keys.length + ' chamados)', keys);
            };
          });
        }
      }
      function destroyContinuation(id) {
        window.chartInstances.forEach(function(c) { if (c && c.canvas && c.canvas.id === id) c.destroy(); });
        window.chartInstances = window.chartInstances.filter(function(c) { return !c || !c.canvas || c.canvas.id !== id; });
        var wrap = document.getElementById(id + 'Wrap');
        if (wrap && wrap.parentNode) wrap.parentNode.removeChild(wrap);
      }
      destroyContinuation('chartResolucaoContinuacao');
      destroyContinuation('chartPrimeiraRespostaContinuacao');
      var list1 = list.length > MAX_TTR_FRT_PER_CHART ? list.slice(0, MAX_TTR_FRT_PER_CHART) : list;
      var list2 = list.length > MAX_TTR_FRT_PER_CHART ? list.slice(MAX_TTR_FRT_PER_CHART) : [];
      var resCtx = document.getElementById('chartResolucao');
      if (resCtx) {
        var resDataFull = list.map(function(rt) { var v = byRt[rt] && byRt[rt].avgResolutionHours; return v != null ? v : 0; });
        var resData1 = list1.map(function(rt) { var v = byRt[rt] && byRt[rt].avgResolutionHours; return v != null ? v : 0; });
        var resMax = Math.max.apply(null, resDataFull) > 0 ? undefined : 10;
        var resOpt = horBarOptions(resMax);
        resOpt.onClick = function(ev, elements) {
          if (elements.length === 0) return;
          var rt = list1[elements[0].index];
          var keys = (window.lastRequestTypeKeys || {})[rt] || [];
          showTicketsModal(rt + ' (' + keys.length + ' chamados)', keys);
        };
        var res = new Chart(resCtx.getContext('2d'), {
          type: 'bar',
          data: { labels: list1, datasets: [{ label: 'Média (h)', data: resData1, backgroundColor: '#820AD1' }] },
          options: resOpt
        });
        window.chartInstances.push(res);
        if (list2.length > 0) {
          var grid = document.querySelector('#chartContainer .chart-grid');
          if (grid) {
            var wrap = document.createElement('div');
            wrap.id = 'chartResolucaoContinuacaoWrap';
            wrap.className = 'chart-card chart-card-parallax';
            wrap.innerHTML = '<h3>Tempo m\u00e9dio de resolu\u00e7\u00e3o (horas) \u2014 continua\u00e7\u00e3o</h3><div style="min-height: 280px;"><canvas id="chartResolucaoContinuacao"></canvas></div>';
            grid.appendChild(wrap);
            var resData2 = list2.map(function(rt) { var v = byRt[rt] && byRt[rt].avgResolutionHours; return v != null ? v : 0; });
            var resOpt2 = horBarOptions();
            resOpt2.onClick = function(ev, elements) {
              if (elements.length === 0) return;
              var rt = list2[elements[0].index];
              var keys = (window.lastRequestTypeKeys || {})[rt] || [];
              showTicketsModal(rt + ' (' + keys.length + ' chamados)', keys);
            };
            var c2 = document.getElementById('chartResolucaoContinuacao');
            if (c2) window.chartInstances.push(new Chart(c2.getContext('2d'), { type: 'bar', data: { labels: list2, datasets: [{ label: 'Média (h)', data: resData2, backgroundColor: '#820AD1' }] }, options: resOpt2 }));
          }
        }
      }
      var frCtx = document.getElementById('chartPrimeiraResposta');
      var frDataFull = list.map(function(rt) { var v = byRt[rt] && byRt[rt].avgFirstResponseHours; return v != null ? v : 0; });
      var hasFrData = frDataFull.some(function(v) { return v > 0; });
      var frEmptyEl = document.getElementById('chartPrimeiraRespostaEmpty');
      if (frEmptyEl) frEmptyEl.style.display = hasFrData ? 'none' : 'block';
      if (frCtx) frCtx.style.display = hasFrData ? 'block' : 'none';
      if (frCtx && hasFrData) {
        var frData1 = list1.map(function(rt) { var v = byRt[rt] && byRt[rt].avgFirstResponseHours; return v != null ? v : 0; });
        var frOpt = horBarOptions();
        frOpt.onClick = function(ev, elements) {
          if (elements.length === 0) return;
          var rt = list1[elements[0].index];
          var keys = (window.lastRequestTypeKeys || {})[rt] || [];
          showTicketsModal(rt + ' (' + keys.length + ' chamados)', keys);
        };
        var fr = new Chart(frCtx.getContext('2d'), {
          type: 'bar',
          data: { labels: list1, datasets: [{ label: 'Média (h)', data: frData1, backgroundColor: '#8c1ce4' }] },
          options: frOpt
        });
        window.chartInstances.push(fr);
        if (list2.length > 0) {
          var grid = document.querySelector('#chartContainer .chart-grid');
          if (grid) {
            var wrap = document.createElement('div');
            wrap.id = 'chartPrimeiraRespostaContinuacaoWrap';
            wrap.className = 'chart-card chart-card-parallax';
            wrap.innerHTML = '<h3>Tempo m\u00e9dio 1\u00aa resposta (horas) \u2014 continua\u00e7\u00e3o</h3><div style="min-height: 280px;"><canvas id="chartPrimeiraRespostaContinuacao"></canvas></div>';
            grid.appendChild(wrap);
            var frData2 = list2.map(function(rt) { var v = byRt[rt] && byRt[rt].avgFirstResponseHours; return v != null ? v : 0; });
            var frOpt2 = horBarOptions();
            frOpt2.onClick = function(ev, elements) {
              if (elements.length === 0) return;
              var rt = list2[elements[0].index];
              var keys = (window.lastRequestTypeKeys || {})[rt] || [];
              showTicketsModal(rt + ' (' + keys.length + ' chamados)', keys);
            };
            var c2 = document.getElementById('chartPrimeiraRespostaContinuacao');
            if (c2) window.chartInstances.push(new Chart(c2.getContext('2d'), { type: 'bar', data: { labels: list2, datasets: [{ label: 'Média (h)', data: frData2, backgroundColor: '#8c1ce4' }] }, options: frOpt2 }));
          }
        }
      }
      var kwByRt = stats.keywordBreakdownByRequestType || {};
      var chartSubCard = document.getElementById('chartSubcategoriaCard');
      var chartSubSelect = document.getElementById('chartSubcategoriaRequestType');
      var chartSubEmpty = document.getElementById('chartSubcategoriaEmpty');
      var chartSubCtx = document.getElementById('chartSubcategoria');
      var chartSubLegend = document.getElementById('chartSubcategoriaLegend');
      if (chartSubCard && chartSubSelect) {
        chartSubSelect.innerHTML = '';
        list.forEach(function(rt) {
          chartSubSelect.appendChild(new Option(rt + ' (' + (byRt[rt] ? byRt[rt].count : 0) + ')', rt));
        });
        function renderSubcategoriaChart() {
          var subStats = (window.lastStats || {}).keywordBreakdownByRequestType || {};
          var sel = (chartSubSelect.value || '').trim();
          var data = sel ? (subStats[sel] || { byKeyword: {}, keywordList: [], keysByKeyword: {} }) : { byKeyword: {}, keywordList: [], keysByKeyword: {} };
          var kwList = data.keywordList || [];
          var byKw = data.byKeyword || {};
          var keysByKw = data.keysByKeyword || {};
          if (kwList.length === 0) {
            chartSubCard.querySelector('.pie-wrapper').style.display = 'none';
            if (chartSubEmpty) chartSubEmpty.style.display = 'block';
          } else {
            if (chartSubEmpty) chartSubEmpty.style.display = 'none';
            chartSubCard.querySelector('.pie-wrapper').style.display = '';
            var subCounts = kwList.map(function(k) { return byKw[k] || 0; });
            var subLabels = kwList.map(function(k) { var n = byKw[k] || 0; return k + ' (' + n + ')'; });
            var subColors = ['#820AD1','#8c1ce4','#6406ac','#c484ec','#ac6b95','#9c8cac','#5c3d7a','#443a53','#6b4d85','#7c5c9c'];
            var subBg = kwList.map(function(_, i) { return subColors[i % subColors.length]; });
            if (chartSubCtx) {
              var subPie = new Chart(chartSubCtx.getContext('2d'), {
                type: 'pie',
                data: { labels: subLabels, datasets: [{ data: subCounts, backgroundColor: subBg }] },
                options: {
                  responsive: true,
                  maintainAspectRatio: true,
                  layout: { padding: 10 },
                  plugins: { legend: { display: false } },
                  onClick: function(ev, elements) {
                    if (elements.length === 0) return;
                    var idx = elements[0].index;
                    var rawLabel = kwList[idx];
                    var keys = keysByKw[rawLabel] || [];
                    var rtLabel = sel ? (sel + ' \u2192 ' + rawLabel) : rawLabel;
                    showTicketsModal(rtLabel + ' (' + keys.length + ' chamados)', keys);
                  }
                }
              });
              window.chartInstances.push(subPie);
            }
            if (chartSubLegend) {
              chartSubLegend.innerHTML = subLabels.map(function(label, i) {
                var safe = (label + '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
                var rawLabel = kwList[i];
                var keys = (keysByKw[rawLabel] || []).length;
                var itemEl = '<div class="pie-legend-item" data-index="' + i + '" style="cursor: pointer;" title="Clique para listar ' + keys + ' ticket(s)"><span class="pie-legend-color" style="background:' + subBg[i] + '"></span><span class="pie-legend-label">' + safe + '</span></div>';
                return itemEl;
              }).join('');
              chartSubLegend.querySelectorAll('.pie-legend-item').forEach(function(el, i) {
                el.onclick = function() {
                  var rawLabel = kwList[i];
                  var keys = (keysByKw[rawLabel] || []);
                  var rtLabel = sel ? (sel + ' \u2192 ' + rawLabel) : rawLabel;
                  showTicketsModal(rtLabel + ' (' + keys.length + ' chamados)', keys);
                };
              });
            }
          }
        }
        chartSubSelect.onchange = function() { window.chartInstances.forEach(function(c) { if (c && c.canvas && c.canvas.id === 'chartSubcategoria') c.destroy(); }); window.chartInstances = window.chartInstances.filter(function(c) { return !c || !c.canvas || c.canvas.id !== 'chartSubcategoria'; }); renderSubcategoriaChart(); };
        if (list.length > 0 && !chartSubSelect.value) chartSubSelect.value = list[0];
        renderSubcategoriaChart();
      }
      var csat = stats.csat || {};
      var byStar = csat.byStar || { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
      var csatTotal = csat.totalWithSatisfaction || 0;
      var totalNoPeriodo = stats.totalIssues != null ? stats.totalIssues : 0;
      var csatAvg = csat.average;
      var chartCsatCtx = document.getElementById('chartCsat');
      var chartCsatEmpty = document.getElementById('chartCsatEmpty');
      var chartCsatWrapper = document.getElementById('chartCsatWrapper');
      var chartCsatAverageEl = document.getElementById('chartCsatAverage');
      if (chartCsatCtx && chartCsatEmpty && chartCsatWrapper && chartCsatAverageEl) {
        window.chartInstances.forEach(function(c) { if (c && c.canvas && c.canvas.id === 'chartCsat') c.destroy(); });
        window.chartInstances = window.chartInstances.filter(function(c) { return !c || !c.canvas || c.canvas.id !== 'chartCsat'; });
        if (csatTotal === 0) {
          chartCsatWrapper.style.display = 'none';
          chartCsatEmpty.style.display = 'block';
          chartCsatAverageEl.textContent = totalNoPeriodo > 0 ? 'Nenhum dado na coluna Satisfaction do Jira para este per\u00edodo. (Total: ' + totalNoPeriodo + ' chamados no per\u00edodo.)' : 'Nenhum dado na coluna Satisfaction do Jira para este per\u00edodo.';
        } else {
          chartCsatEmpty.style.display = 'none';
          chartCsatWrapper.style.display = 'block';
          var csatLabels = ['1 estrela', '2 estrelas', '3 estrelas', '4 estrelas', '5 estrelas'];
          var csatData = [byStar[1] || 0, byStar[2] || 0, byStar[3] || 0, byStar[4] || 0, byStar[5] || 0];
          var csatColors = ['#dc2626', '#ea580c', '#ca8a04', '#65a30d', '#059669'];
          var csatChart = new Chart(chartCsatCtx.getContext('2d'), {
            type: 'bar',
            data: { labels: csatLabels, datasets: [{ label: 'Chamados', data: csatData, backgroundColor: csatColors }] },
            options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }, plugins: { legend: { display: false } } }
          });
          window.chartInstances.push(csatChart);
          var baseText = csatTotal + ' com avalia\u00e7\u00e3o';
          if (totalNoPeriodo > 0 && totalNoPeriodo !== csatTotal) baseText += ' (de ' + totalNoPeriodo + ' no per\u00edodo)';
          chartCsatAverageEl.textContent = csatAvg != null ? 'CSAT m\u00e9dio: ' + csatAvg + ' (base: ' + baseText + ', fonte: coluna Satisfaction)' : 'CSAT m\u00e9dio: \u2014 (base: ' + baseText + ', fonte: coluna Satisfaction)';
        }
      }
    function destroyChart(id) {
      window.chartInstances.forEach(function(c) { if (c && c.canvas && c.canvas.id === id) c.destroy(); });
      window.chartInstances = window.chartInstances.filter(function(c) { return !c || !c.canvas || c.canvas.id !== id; });
    }
    var slaAgg = stats.slaAggregate || {};
    var slaPctEl = document.getElementById('slaPctSummary');
    var slaPctEmpty = document.getElementById('slaPctEmpty');
    if (slaPctEl && slaPctEmpty) {
      if (slaAgg.totalWithSla > 0) {
        slaPctEmpty.style.display = 'none';
        slaPctEl.style.display = '';
        slaPctEl.textContent = slaAgg.pctWithinSla != null ? slaAgg.pctWithinSla + '% dentro do SLA (' + (slaAgg.totalMet || 0) + ' de ' + slaAgg.totalWithSla + ' com SLA)' : '—';
      } else { slaPctEl.style.display = 'none'; slaPctEmpty.style.display = 'block'; }
    }
    var violByName = slaAgg.violationCountByName || {};
    var violLabels = Object.keys(violByName);
    if (violLabels.length > 0) {
      destroyChart('chartSlaViolations');
      var c = document.getElementById('chartSlaViolations');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: violLabels, datasets: [{ label: 'Violações', data: violLabels.map(function(l) { return violByName[l]; }), backgroundColor: '#dc2626' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }, plugins: { legend: { display: false } } } }));
    }
    var ttrFrt = stats.ttrFrtByPeriod || {};
    var byPeriod = ttrFrt.byPeriod || [];
    if (byPeriod.length > 0) {
      destroyChart('chartTtrFrtPeriod');
      var c = document.getElementById('chartTtrFrtPeriod');
      var ttrData = byPeriod.map(function(p) { return p.medianTtrHours != null ? p.medianTtrHours : null; });
      var frtData = byPeriod.map(function(p) { return p.medianFrtHours != null ? p.medianFrtHours : null; });
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: byPeriod.map(function(p) { return p.period; }), datasets: [{ label: 'TTR mediano (h)', data: ttrData, borderColor: '#820AD1', fill: false, spanGaps: false }, { label: 'FRT mediano (h)', data: frtData, borderColor: '#8c1ce4', borderDash: [5, 5], fill: false, spanGaps: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, suggestedMax: 24 } } } }));
    }
    var notaT = stats.notaTemporal || {};
    var notaByP = notaT.byPeriod || [];
    if (notaByP.length > 0) {
      destroyChart('chartNotaPeriod');
      var c = document.getElementById('chartNotaPeriod');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: notaByP.map(function(p) { return p.period; }), datasets: [{ label: 'Nota média', data: notaByP.map(function(p) { return p.avgNota; }), borderColor: '#059669', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } } } }));
    }
    var dist = notaT.distribution || {};
    var distLabels = ['1', '2', '3', '4', '5'];
    var distData = distLabels.map(function(l) { return dist[parseInt(l, 10)] || 0; });
    if (distData.some(function(v) { return v > 0; })) {
      destroyChart('chartNotaDist');
      var c = document.getElementById('chartNotaDist');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: distLabels.map(function(l) { return l + ' estrela(s)'; }), datasets: [{ label: 'Chamados', data: distData, backgroundColor: ['#dc2626','#ea580c','#ca8a04','#65a30d','#059669'] }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }, plugins: { legend: { display: false } } } }));
    }
    var topA = (notaT.topAnalysts || []).slice(0, 10);
    if (topA.length > 0) {
      destroyChart('chartTopAnalysts');
      var c = document.getElementById('chartTopAnalysts');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: topA.map(function(a) { return a.assignee; }), datasets: [{ label: 'Nota média', data: topA.map(function(a) { return a.avgNota; }), backgroundColor: '#820AD1' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, scales: { x: { min: 1, max: 5 } }, plugins: { legend: { display: false } } } }));
    }
    var csatByP = (stats.csatByPeriod || {}).byPeriod || [];
    if (csatByP.length > 0) {
      destroyChart('chartCsatPeriod');
      var c = document.getElementById('chartCsatPeriod');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: csatByP.map(function(p) { return p.period; }), datasets: [{ label: 'CSAT médio', data: csatByP.map(function(p) { return p.average; }), borderColor: '#059669', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } } } }));
    }
    var csatVsNotaPoints = (stats.csatVsNota || {}).points || [];
    if (csatVsNotaPoints.length > 0) {
      destroyChart('chartCsatVsNota');
      var c = document.getElementById('chartCsatVsNota');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'scatter', data: { datasets: [{ label: 'CSAT x Nota', data: csatVsNotaPoints.map(function(p) { return { x: p.csat, y: p.nota }; }), backgroundColor: 'rgba(130, 10, 209, 0.6)' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { x: { min: 0.5, max: 5.5, title: { display: true, text: 'CSAT' } }, y: { min: 0.5, max: 5.5, title: { display: true, text: 'Nota' } } } } }));
    }
    var csatByRt = (stats.csatByRequestType || {}).byRequestType || {};
    var csatRtLabels = Object.keys(csatByRt);
    if (csatRtLabels.length > 0) {
      destroyChart('chartCsatByRt');
      var c = document.getElementById('chartCsatByRt');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: csatRtLabels, datasets: [{ label: 'CSAT médio', data: csatRtLabels.map(function(rt) { return csatByRt[rt].average; }), backgroundColor: '#059669' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } }, plugins: { legend: { display: false } } } }));
    }
    var volByP = (stats.volumeByPeriod || {}).byPeriod || [];
    if (volByP.length > 0) {
      destroyChart('chartVolumePeriod');
      var c = document.getElementById('chartVolumePeriod');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: volByP.map(function(p) { return p.period; }), datasets: [{ label: 'Tickets', data: volByP.map(function(p) { return p.count; }), borderColor: '#820AD1', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }, plugins: { legend: { display: false } } } }));
    }
    var volByA = (stats.volumeByAnalyst || {}).byAnalyst || [];
    if (volByA.length > 0) {
      destroyChart('chartVolumeAnalyst');
      var c = document.getElementById('chartVolumeAnalyst');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: volByA.map(function(a) { return a.assignee; }), datasets: [{ label: 'Tickets', data: volByA.map(function(a) { return a.count; }), backgroundColor: '#8c1ce4' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, scales: { x: { beginAtZero: true } }, plugins: { legend: { display: false } } } }));
    }
    var reopened = stats.reopened || {};
    var reopenedByP = reopened.byPeriod || [];
    var reopenedKeys = reopened.keys || [];
    var reopenedEmptyEl = document.getElementById('chartReaberturaEmpty');
    var reopenedWrapperEl = document.getElementById('chartReaberturaWrapper');
    var reopenedTotalEl = document.getElementById('chartReaberturaTotal');
    if (reopenedByP.length > 0 || reopened.total > 0) {
      if (reopenedEmptyEl) reopenedEmptyEl.style.display = 'none';
      if (reopenedWrapperEl) reopenedWrapperEl.style.display = 'block';
      if (reopenedTotalEl) reopenedTotalEl.textContent = 'Total no per\u00edodo: ' + (reopened.total || 0) + ' ticket(s) reaberto(s).';
      destroyChart('chartReabertura');
      var c = document.getElementById('chartReabertura');
      if (c) {
        var labels = reopenedByP.length > 0 ? reopenedByP.map(function(p) { return p.period; }) : [reopened.total > 0 ? 'Per\u00edodo' : ''];
        var data = reopenedByP.length > 0 ? reopenedByP.map(function(p) { return p.count; }) : [reopened.total || 0];
        var chart = new Chart(c.getContext('2d'), {
          type: 'bar',
          data: { labels: labels, datasets: [{ label: 'Reabertos', data: data, backgroundColor: '#dc2626' }] },
          options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } },
            plugins: { legend: { display: false } },
            onClick: function(ev, elements) {
              if (reopenedKeys.length > 0 && elements.length > 0) showTicketsModal('Tickets reabertos no per\u00edodo', reopenedKeys);
            }
          }
        });
        window.chartInstances.push(chart);
      }
    } else {
      if (reopenedEmptyEl) reopenedEmptyEl.style.display = 'block';
      if (reopenedWrapperEl) reopenedWrapperEl.style.display = 'none';
      if (reopenedTotalEl) reopenedTotalEl.textContent = '';
    }
    var slaPctByP = (stats.slaPctByPeriod || {}).byPeriod || [];
    if (slaPctByP.length > 0) {
      destroyChart('chartSlaPctPeriod');
      var c = document.getElementById('chartSlaPctPeriod');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: slaPctByP.map(function(p) { return p.period; }), datasets: [{ label: '% dentro do SLA', data: slaPctByP.map(function(p) { return p.pctWithinSla; }), borderColor: '#059669', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 0, max: 100 } }, plugins: { legend: { display: false } } } }));
    }
    if (byPeriod.length > 0) {
      destroyChart('chartTtrFrtLine');
      var c = document.getElementById('chartTtrFrtLine');
      var ttrDataLine = byPeriod.map(function(p) { return p.medianTtrHours != null ? p.medianTtrHours : null; });
      var frtDataLine = byPeriod.map(function(p) { return p.medianFrtHours != null ? p.medianFrtHours : null; });
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: byPeriod.map(function(p) { return p.period; }), datasets: [{ label: 'TTR mediano (h)', data: ttrDataLine, borderColor: '#820AD1', fill: false, spanGaps: false }, { label: 'FRT mediano (h)', data: frtDataLine, borderColor: '#8c1ce4', borderDash: [5, 5], fill: false, spanGaps: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, suggestedMax: 24 } } } }));
    }
    if (notaByP.length > 0) {
      destroyChart('chartNotaSolucaoPeriod');
      var c = document.getElementById('chartNotaSolucaoPeriod');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: notaByP.map(function(p) { return p.period; }), datasets: [{ label: 'Nota m\u00e9dia', data: notaByP.map(function(p) { return p.avgNota; }), borderColor: '#059669', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } } } }));
    }
    if (distData.some(function(v) { return v > 0; })) {
      destroyChart('chartNotaSolucaoDist');
      var c = document.getElementById('chartNotaSolucaoDist');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: distLabels.map(function(l) { return l + ' estrela(s)'; }), datasets: [{ label: 'Chamados', data: distData, backgroundColor: ['#dc2626','#ea580c','#ca8a04','#65a30d','#059669'] }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }, plugins: { legend: { display: false } } } }));
    }
    if (notaByP.length > 0) {
      destroyChart('chartNotaFinalLine');
      var c = document.getElementById('chartNotaFinalLine');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: notaByP.map(function(p) { return p.period; }), datasets: [{ label: 'Nota final m\u00e9dia', data: notaByP.map(function(p) { return p.avgNota; }), borderColor: '#820AD1', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } } } }));
    }
    if (distData.some(function(v) { return v > 0; })) {
      destroyChart('chartNotaFinalDist');
      var c = document.getElementById('chartNotaFinalDist');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: distLabels.map(function(l) { return l + ' estrela(s)'; }), datasets: [{ label: 'Chamados', data: distData, backgroundColor: ['#dc2626','#ea580c','#ca8a04','#65a30d','#059669'] }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }, plugins: { legend: { display: false } } } }));
    }
    var top10 = (notaT.topAnalysts || []).slice(0, 10);
    if (top10.length > 0) {
      destroyChart('chartTop10Analysts');
      var c = document.getElementById('chartTop10Analysts');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: top10.map(function(a) { return a.assignee; }), datasets: [{ label: 'Nota m\u00e9dia', data: top10.map(function(a) { return a.avgNota; }), backgroundColor: '#059669' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, scales: { x: { min: 1, max: 5 } }, plugins: { legend: { display: false } } } }));
    }
    var bottom10 = notaT.bottomAnalysts || [];
    if (bottom10.length > 0) {
      destroyChart('chartBottom10Analysts');
      var c = document.getElementById('chartBottom10Analysts');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: bottom10.map(function(a) { return a.assignee; }), datasets: [{ label: 'Nota m\u00e9dia', data: bottom10.map(function(a) { return a.avgNota; }), backgroundColor: '#dc2626' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, scales: { x: { min: 1, max: 5 } }, plugins: { legend: { display: false } } } }));
    }
    var slaByA = (stats.slaByAnalyst || {}).byAnalyst || [];
    if (slaByA.length > 0) {
      destroyChart('chartSlaByAnalyst');
      var c = document.getElementById('chartSlaByAnalyst');
      var labels = slaByA.map(function(a) { return a.assignee; });
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: labels, datasets: [{ label: 'Dentro SLA', data: slaByA.map(function(a) { return a.met; }), backgroundColor: '#059669' }, { label: 'Fora SLA', data: slaByA.map(function(a) { return a.total - a.met; }), backgroundColor: '#dc2626' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } }, plugins: { legend: { display: true } } } }));
    }
    if (csatByP.length > 0) {
      destroyChart('chartCsatLine');
      var c = document.getElementById('chartCsatLine');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'line', data: { labels: csatByP.map(function(p) { return p.period; }), datasets: [{ label: 'CSAT m\u00e9dio', data: csatByP.map(function(p) { return p.average; }), borderColor: '#059669', fill: false }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } } } }));
    }
    if (csatVsNotaPoints.length > 0) {
      destroyChart('chartCsatVsNota2');
      var c = document.getElementById('chartCsatVsNota2');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'scatter', data: { datasets: [{ label: 'CSAT x Nota', data: csatVsNotaPoints.map(function(p) { return { x: p.csat, y: p.nota }; }), backgroundColor: 'rgba(130, 10, 209, 0.6)' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { x: { min: 0.5, max: 5.5, title: { display: true, text: 'CSAT' } }, y: { min: 0.5, max: 5.5, title: { display: true, text: 'Nota' } } } } }));
    }
    var notaByRt = (stats.notaByRequestType || {}).byRequestType || {};
    var notaRtLabels = Object.keys(notaByRt);
    if (notaRtLabels.length > 0) {
      destroyChart('chartNotaByRt');
      var c = document.getElementById('chartNotaByRt');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: notaRtLabels, datasets: [{ label: 'Nota m\u00e9dia', data: notaRtLabels.map(function(rt) { return notaByRt[rt].avgNota; }), backgroundColor: '#820AD1' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 1, max: 5 } }, plugins: { legend: { display: false } } } }));
    }
    var slaByRt = (stats.slaByRequestType || {}).byRequestType || {};
    var slaRtLabels = Object.keys(slaByRt);
    if (slaRtLabels.length > 0) {
      destroyChart('chartSlaByRt');
      var c = document.getElementById('chartSlaByRt');
      if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: slaRtLabels, datasets: [{ label: '% dentro SLA', data: slaRtLabels.map(function(rt) { return slaByRt[rt].pct; }), backgroundColor: '#059669' }] }, options: { responsive: true, maintainAspectRatio: true, scales: { y: { min: 0, max: 100 } }, plugins: { legend: { display: false } } } }));
    }
    var criticalByP = (stats.criticalPctByPeriod || {}).byPeriod || [];
    if (criticalByP.length > 0) {
      destroyChart('chartCriticalPctPeriod');
      var c = document.getElementById('chartCriticalPctPeriod');
      if (c) {
        var keysByPeriod = criticalByP.map(function(p) { return p.keys || []; });
        window.chartInstances.push(new Chart(c.getContext('2d'), {
          type: 'line',
          data: {
            labels: criticalByP.map(function(p) { return p.period; }),
            datasets: [{
              label: '% cr\u00edticos (Satisfaction 1-2)',
              data: criticalByP.map(function(p) { return p.pctCritical; }),
              borderColor: '#dc2626',
              backgroundColor: 'rgba(220, 38, 38, 0.1)',
              fill: false,
              pointRadius: 8,
              pointHoverRadius: 12,
              pointBackgroundColor: '#dc2626',
              pointBorderColor: '#fff',
              pointBorderWidth: 2
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: { y: { min: 0, max: 100 } },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  afterLabel: function(context) {
                    var idx = context.dataIndex;
                    var keys = keysByPeriod[idx] || [];
                    return keys.length > 0 ? 'Clique para ver ' + keys.length + ' ticket(s)' : '';
                  }
                }
              }
            },
            onClick: function(ev, elements) {
              if (elements.length > 0) {
                var idx = elements[0].index;
                var keys = keysByPeriod[idx] || [];
                var period = criticalByP[idx] ? criticalByP[idx].period : '';
                showTicketsModal('Tickets cr\u00edticos (Satisfaction 1-2) - ' + period, keys);
              }
            }
          }
        }));
      }
    }
    }

    document.getElementById('filterRequestType').onchange = applyRequestTypeFilter;
    document.getElementById('btnViewLista').onclick = function() {
      window.viewMode = 'lista';
      document.getElementById('btnViewLista').classList.add('active');
      document.getElementById('btnViewGrafico').classList.remove('active');
      document.getElementById('result').style.display = '';
      document.getElementById('chartContainer').classList.remove('visible');
    };
    document.getElementById('btnViewGrafico').onclick = function() {
      window.viewMode = 'grafico';
      document.getElementById('btnViewGrafico').classList.add('active');
      document.getElementById('btnViewLista').classList.remove('active');
      document.getElementById('result').style.display = 'none';
      document.getElementById('chartContainer').classList.add('visible');
      renderCharts();
    };

    function msg(text, type) {
      messageEl.innerHTML = '<div class="msg ' + (type || 'info') + '">' + text + '</div>';
    }

    function jqlWithPeriod(jql, month, year) {
      if (!jql || !month || !year) return jql;
      var lastDay = new Date(parseInt(year, 10), parseInt(month, 10), 0).getDate();
      var first = year + '-' + (month < 10 ? '0' + month : month) + '-01';
      var last = year + '-' + (month < 10 ? '0' + month : month) + '-' + (lastDay < 10 ? '0' + lastDay : lastDay);
      var condition = 'created >= "' + first + '" AND created <= "' + last + '"';
      var orderIdx = jql.toUpperCase().lastIndexOf(' ORDER BY ');
      if (orderIdx !== -1) return jql.slice(0, orderIdx).trim() + ' AND ' + condition + ' ' + jql.slice(orderIdx).trim();
      return jql.trim() + ' AND ' + condition;
    }

    btnBuscar.onclick = async () => {
      var jql = jqlEl.value.trim();
      if (!jql) { msg('Digite uma JQL.', 'error'); return; }
      var m = monthSelect.value;
      var y = yearSelect.value;
      if (m && y) {
        jql = jqlWithPeriod(jql, parseInt(m, 10), parseInt(y, 10));
        jqlEl.value = jql;
      }
      var payload = { jql: jql, limit: (m && y) ? 3000 : Math.min(parseInt(limitEl.value, 10) || 50, 3000) };
      if (window.selectedFilterId) payload.filter_id = window.selectedFilterId;
      if (m && y) { payload.month = parseInt(m, 10); payload.year = parseInt(y, 10); }
      btnBuscar.disabled = true;
      msg((m && y) ? 'Buscando todos os chamados do mês...' : 'Buscando...', 'info');
      try {
        var r = await fetch('/buscar', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        var data = await r.json();
        if (data.error) { msg(data.error, 'error'); resultEl.innerHTML = ''; document.getElementById('toolbar').style.display = 'none'; }
        else {
          msg(data.count + ' issue(s) encontrado(s).' + ((m && y) ? ' Busca mensal (todos do período).' : '') + ' Clique na Key para abrir no Jira.', 'success');
          window.lastRequestTypeKeys = data.requestTypeKeys || {};
          window.jiraBaseUrl = data.jiraBaseUrl || '';
          resultEl.innerHTML = data.html;
          if (data.notas) {
            document.querySelectorAll('.nota-cell').forEach(function(td) {
              var key = td.getAttribute('data-nota-key');
              if (!key) return;
              var entry = data.notas[key];
              var n = entry ? entry.nota : null;
              var c = (entry && entry.comentario) ? entry.comentario : '';
              td.textContent = n != null ? String(n) : '—';
              td.title = c ? (String(n) + ' – ' + c) : (n != null ? String(n) : '');
            });
          }
          btnExport.disabled = false;
          if (document.getElementById('btnNotasRestante')) document.getElementById('btnNotasRestante').disabled = false;
          document.getElementById('btnNotas').disabled = false;
          if (document.getElementById('btnReavaliarNotas')) document.getElementById('btnReavaliarNotas').disabled = false;
          if (document.getElementById('btnPontosOllama')) document.getElementById('btnPontosOllama').disabled = false;
          window.lastStats = data.stats || { byRequestType: {}, requestTypeList: [] };
          var toolbar = document.getElementById('toolbar');
          toolbar.style.display = 'flex';
          var filterRt = document.getElementById('filterRequestType');
          filterRt.innerHTML = '<option value="">Todos</option>';
          (window.lastStats.requestTypeList || []).forEach(function(rt) {
            filterRt.appendChild(new Option(rt, rt));
          });
          applyRequestTypeFilter();
          if (window.viewMode === 'grafico') {
            document.getElementById('result').style.display = 'none';
            document.getElementById('chartContainer').classList.add('visible');
            renderCharts();
          } else {
            document.getElementById('result').style.display = '';
            document.getElementById('chartContainer').classList.remove('visible');
          }
        }
      } catch (e) {
        msg('Erro: ' + e.message, 'error');
      }
      btnBuscar.disabled = false;
    };

    document.getElementById('btnNotas').onclick = async () => {
      var btnNotas = document.getElementById('btnNotas');
      btnNotas.disabled = true;
      msg('Calculando notas (Ollama). Só termina quando todos tiverem nota — pode demorar bastante.', 'info');
      var timeoutMs = 60 * 60 * 1000;
      var controller = new AbortController();
      var timeoutId = setTimeout(function() { controller.abort(); }, timeoutMs);
      try {
        var r = await fetch('/api/notas', { method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: controller.signal });
        clearTimeout(timeoutId);
        var data = await r.json();
        if (data.error) { msg(data.error, 'error'); }
        else {
          document.querySelectorAll('.nota-cell').forEach(function(td) {
            var key = td.getAttribute('data-nota-key');
            if (!key) return;
            var entry = data[key];
            var n = entry ? entry.nota : null;
            var c = (entry && entry.comentario) ? entry.comentario : '';
            td.textContent = n != null ? String(n) : '—';
            td.title = c ? (String(n) + ' – ' + c) : (n != null ? String(n) : '');
          });
          msg('Notas calculadas.', 'success');
        }
      } catch (e) {
        clearTimeout(timeoutId);
        if (e.name === 'AbortError') {
          msg('Demorou muito (timeout 60 min). Tente menos tickets ou calcule notas de novo (Ollama).', 'error');
        } else {
          msg('Erro: ' + e.message, 'error');
        }
      }
      btnNotas.disabled = false;
    };

    var btnNotasRestante = document.getElementById('btnNotasRestante');
    if (btnNotasRestante) {
      btnNotasRestante.onclick = async () => {
        btnNotasRestante.disabled = true;
        msg('Calculando apenas os chamados que ainda não têm nota (pode demorar; timeout 60 min)...', 'info');
        var timeoutMs = 60 * 60 * 1000;
        var controller = new AbortController();
        var timeoutId = setTimeout(function() { controller.abort(); }, timeoutMs);
        try {
          var r = await fetch('/api/notas-restante', { method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: controller.signal });
          clearTimeout(timeoutId);
          var data = await r.json();
          if (data.error) { msg(data.error, 'error'); }
          else {
            document.querySelectorAll('.nota-cell').forEach(function(td) {
              var key = td.getAttribute('data-nota-key');
              if (!key) return;
              var entry = data[key];
              var n = entry ? entry.nota : null;
              var c = (entry && entry.comentario) ? entry.comentario : '';
              td.textContent = n != null ? String(n) : '—';
              td.title = c ? (String(n) + ' – ' + c) : (n != null ? String(n) : '');
            });
            msg('Notas do restante calculadas.', 'success');
          }
        } catch (e) {
          clearTimeout(timeoutId);
          if (e.name === 'AbortError') {
            msg('Demorou muito (timeout 60 min). Clique em Calcular restante de novo para continuar.', 'error');
          } else {
            msg('Erro: ' + e.message, 'error');
          }
        }
        btnNotasRestante.disabled = false;
      };
    }

    var btnReavaliarNotas = document.getElementById('btnReavaliarNotas');
    if (btnReavaliarNotas) {
      btnReavaliarNotas.onclick = async () => {
        btnReavaliarNotas.disabled = true;
        msg('Reavaliando todas as notas (Ollama). Pode demorar bastante.', 'info');
        var timeoutMs = 60 * 60 * 1000;
        var controller = new AbortController();
        var timeoutId = setTimeout(function() { controller.abort(); }, timeoutMs);
        try {
          var r = await fetch('/api/notas-reavaliar', { method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: controller.signal });
          clearTimeout(timeoutId);
          var data = await r.json();
          if (data.error) { msg(data.error, 'error'); }
          else {
            document.querySelectorAll('.nota-cell').forEach(function(td) {
              var key = td.getAttribute('data-nota-key');
              if (!key) return;
              var entry = data[key];
              var n = entry ? entry.nota : null;
              var c = (entry && entry.comentario) ? entry.comentario : '';
              td.textContent = n != null ? String(n) : '—';
              td.title = c ? (String(n) + ' – ' + c) : (n != null ? String(n) : '');
            });
            msg('Notas reavaliadas.', 'success');
          }
        } catch (e) {
          clearTimeout(timeoutId);
          if (e.name === 'AbortError') {
            msg('Demorou muito (timeout 60 min). Tente menos tickets ou reavalie de novo.', 'error');
          } else {
            msg('Erro: ' + e.message, 'error');
          }
        }
        btnReavaliarNotas.disabled = false;
      };
    }

    btnExport.onclick = async () => {
      try {
        const r = await fetch('/export', { method: 'POST' });
        if (!r.ok) { const d = await r.json(); msg(d.error || 'Erro ao exportar', 'error'); return; }
        const blob = await r.blob();
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'dashboard.html';
        a.click();
        msg('Arquivo exportado.', 'success');
      } catch (e) {
        msg('Erro ao exportar: ' + e.message, 'error');
      }
    };

    async function loadFilters() {
      btnCarregarFiltros.disabled = true;
      msg('Carregando filtros, projetos e status...', 'info');
      try {
        const r = await fetch('/filtros');
        var data;
        try { data = await r.json(); } catch (e) { data = { error: 'Resposta inv\u00e1lida do servidor (verifique credenciais Jira no .env)' }; }
        if (!r.ok) { msg(data.error || ('Erro ' + r.status), 'error'); btnCarregarFiltros.disabled = false; return null; }
        if (data.error) { msg(data.error, 'error'); btnCarregarFiltros.disabled = false; return null; }
        var filters = Array.isArray(data.filters) ? data.filters : [];
        var projects = Array.isArray(data.projects) ? data.projects : [];
        var statuses = Array.isArray(data.statuses) ? data.statuses : [];
        filterSelect.innerHTML = '<option value="">—</option>' + filters.map(function(f, i) { return '<option value="' + i + '">' + (f.name || '').replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</option>'; }).join('');
        projectSelect.innerHTML = '<option value="">—</option>' + projects.map(function(p) { return '<option value="' + (p.key || '').replace(/"/g, '&quot;') + '">' + (p.key || '') + ' - ' + (p.name || '').replace(/</g, '&lt;') + '</option>'; }).join('');
        statusSelect.innerHTML = '<option value="">—</option>' + statuses.map(function(s) { return '<option value="' + String(s).replace(/"/g, '&quot;') + '">' + String(s).replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</option>'; }).join('');
        filterSelect.disabled = false;
        projectSelect.disabled = false;
        statusSelect.disabled = false;
        btnAplicarFiltros.disabled = false;
        window.filtrosData = { filters: filters, projects: projects, statuses: statuses };
        msg('Carregados: ' + filters.length + ' filtro(s), ' + projects.length + ' projeto(s), ' + statuses.length + ' status.', 'success');
        return window.filtrosData;
      } catch (e) {
        msg('Erro: ' + e.message, 'error');
      }
      btnCarregarFiltros.disabled = false;
      return null;
    }

    function applyDefaultsAfterLoad(data) {
      if (!data || !data.filters) return;
      var projOpt = Array.from(projectSelect.options).find(function(o) { return o.value === 'IT'; });
      if (projOpt) projectSelect.value = 'IT';
      var techIdx = data.filters.findIndex(function(f) { return (f.name || '').toLowerCase().indexOf('techcenter') !== -1; });
      if (techIdx !== -1) {
        filterSelect.value = String(techIdx);
        var f = data.filters[techIdx];
        window.selectedFilterId = f.id || null;
        if (f.jql) { jqlEl.value = f.jql; msg('Padrão: projeto IT e filtro Techcenter aplicados.', 'success'); }
        else {
          fetch('/filtro/' + f.id).then(function(r) { return r.json(); }).then(function(d) {
            if (d.jql) { jqlEl.value = d.jql; msg('Padrão: projeto IT e filtro Techcenter aplicados.', 'success'); }
          }).catch(function() {});
        }
      } else { msg('Padrão: projeto IT selecionado. Filtro "Techcenter" não encontrado.', 'info'); }
      btnCarregarFiltros.disabled = false;
    }

    function loadFiltersOnStart() {
      loadFilters().then(function(data) {
        if (data) { applyDefaultsAfterLoad(data); return; }
        msg('Filtros não carregados. Tentando novamente em 2s...', 'info');
        setTimeout(function() {
          loadFilters().then(function(data2) {
            if (data2) { applyDefaultsAfterLoad(data2); return; }
            msg('Clique em "Carregar meus filtros" para tentar novamente. Verifique JIRA_EMAIL e JIRA_API_TOKEN no .env.', 'error');
          });
        }, 2000);
      });
    }
    loadFiltersOnStart();

    btnCarregarFiltros.onclick = function() { loadFilters().then(applyDefaultsAfterLoad); };

    function renderPontosCharts() {
      var data = window.lastPontosMelhoria;
      if (!data) return;
      function destroyChart(id) {
        window.chartInstances.forEach(function(c) { if (c && c.canvas && c.canvas.id === id) c.destroy(); });
        window.chartInstances = window.chartInstances.filter(function(c) { return !c || !c.canvas || c.canvas.id !== id; });
      }
      var top5M = data.top5Melhoria || [];
      var top5F = data.top5Fortes || [];
      var byRt = data.melhoriaByRequestType || {};
      var emptyMelhoria = document.getElementById('pontosMelhoriaEmpty');
      var emptyFortes = document.getElementById('pontosFortesEmpty');
      var emptyRt = document.getElementById('pontosMelhoriaRtEmpty');
      var selRt = document.getElementById('pontosMelhoriaRtSelect');
      if (top5M.length > 0) {
        if (emptyMelhoria) emptyMelhoria.style.display = 'none';
        destroyChart('chartTop5Melhoria');
        var c = document.getElementById('chartTop5Melhoria');
        if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: top5M.map(function(x) { return x.label; }), datasets: [{ label: 'Men\u00e7\u00f5es', data: top5M.map(function(x) { return x.count; }), backgroundColor: '#dc2626' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, layout: { padding: { left: 420 } }, scales: { x: { beginAtZero: true, ticks: { stepSize: 1 } }, y: { ticks: { autoSkip: false, maxRotation: 0, font: { size: 11 } } } }, plugins: { legend: { display: false } } } }));
      } else if (emptyMelhoria) emptyMelhoria.style.display = 'block';
      if (top5F.length > 0) {
        if (emptyFortes) emptyFortes.style.display = 'none';
        destroyChart('chartTop5Fortes');
        var c = document.getElementById('chartTop5Fortes');
        if (c) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: top5F.map(function(x) { return x.label; }), datasets: [{ label: 'Men\u00e7\u00f5es', data: top5F.map(function(x) { return x.count; }), backgroundColor: '#059669' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, layout: { padding: { left: 420 } }, scales: { x: { beginAtZero: true, ticks: { stepSize: 1 } }, y: { ticks: { autoSkip: false, maxRotation: 0, font: { size: 11 } } } }, plugins: { legend: { display: false } } } }));
      } else if (emptyFortes) emptyFortes.style.display = 'block';
      var rtKeys = Object.keys(byRt);
      if (rtKeys.length > 0 && selRt) {
        selRt.innerHTML = rtKeys.map(function(rt, i) { return '<option value="' + i + '">' + String(rt).replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</option>'; }).join('');
        selRt.value = '0';
        if (emptyRt) emptyRt.style.display = 'none';
        function drawMelhoriaByRt() {
          destroyChart('chartMelhoriaByRt');
          var idx = parseInt(selRt.value, 10);
          var rt = rtKeys[idx];
          var items = (rt && byRt[rt]) ? Object.entries(byRt[rt]).map(function(kv) { return { label: kv[0], count: kv[1] }; }).sort(function(a, b) { return b.count - a.count; }).slice(0, 8) : [];
          var c = document.getElementById('chartMelhoriaByRt');
          if (c && items.length > 0) window.chartInstances.push(new Chart(c.getContext('2d'), { type: 'bar', data: { labels: items.map(function(x) { return x.label; }), datasets: [{ label: 'Men\u00e7\u00f5es', data: items.map(function(x) { return x.count; }), backgroundColor: '#820AD1' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: true, scales: { x: { beginAtZero: true } }, plugins: { legend: { display: false } } } }));
        }
        drawMelhoriaByRt();
        selRt.onchange = drawMelhoriaByRt;
      } else if (emptyRt) emptyRt.style.display = 'block';
      var relatorioEmpty = document.getElementById('pontosRelatorioEmpty');
      var relatorioPorChamado = document.getElementById('pontosRelatorioPorChamado');
      var relatorioContent = document.getElementById('pontosRelatorioContent');
      var ulMelhoria = document.getElementById('pontosRelatorioMelhoria');
      var ulFortes = document.getElementById('pontosRelatorioFortes');
      var pontosPorIssue = data.pontosPorIssue || [];
      var jiraBase = window.jiraBaseUrl || '';
      function escapeHtml(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
      if (pontosPorIssue.length > 0) {
        if (relatorioEmpty) relatorioEmpty.style.display = 'none';
        if (relatorioPorChamado) {
          relatorioPorChamado.style.display = 'block';
          var hrefBase = jiraBase ? (jiraBase + '/browse/') : '#';
          relatorioPorChamado.innerHTML = pontosPorIssue.map(function(item) {
            var key = item.key || '';
            var summary = escapeHtml((item.summary || '').slice(0, 120));
            var melhoriaList = (item.melhorias || []).map(function(m) { return '<li class="pontos-texto-completo">' + escapeHtml(String(m)) + '</li>'; }).join('');
            var fortesList = (item.fortes || []).map(function(f) { return '<li class="pontos-texto-completo">' + escapeHtml(String(f)) + '</li>'; }).join('');
            return '<div class="pontos-chamado-block" style="margin-bottom: 20px; padding: 14px; background: #f8f7fa; border-radius: 10px; border: 1px solid #e5e2eb;">' +
              '<div style="margin-bottom: 8px;"><a href="' + hrefBase + encodeURIComponent(key) + '" target="_blank" rel="noopener" style="font-weight: 700; color: var(--nubank-purple);">' + escapeHtml(key) + '</a>' +
              (summary ? ' &mdash; ' + summary : '') + '</div>' +
              '<div class="pontos-duas-colunas" style="gap: 20px;">' +
              '<div class="pontos-col-esquerda"><h4 style="color: #dc2626; margin: 0 0 6px 0; font-size: 12pt;">Pontos negativos (melhoria)</h4><ul style="margin: 0; padding-left: 20px; font-size: 12pt; line-height: 1.5;">' + (melhoriaList || '<li>—</li>') + '</ul></div>' +
              '<div class="pontos-col-direita"><h4 style="color: #059669; margin: 0 0 6px 0; font-size: 12pt;">Pontos positivos (fortes)</h4><ul style="margin: 0; padding-left: 20px; font-size: 12pt; line-height: 1.5;">' + (fortesList || '<li>—</li>') + '</ul></div>' +
              '</div></div>';
          }).join('');
        }
        if (relatorioContent) relatorioContent.style.display = 'block';
        if (ulMelhoria) ulMelhoria.innerHTML = top5M.map(function(x) {
          var t = escapeHtml(x.label || '');
          var keys = x.keys || [];
          var keysHtml = keys.length && jiraBase ? ' <span class="pontos-tickets">' + keys.map(function(k) { return '<a href="' + jiraBase + '/browse/' + encodeURIComponent(k) + '" target="_blank" rel="noopener">' + escapeHtml(k) + '</a>'; }).join(', ') + '</span>' : '';
          return '<li>' + t + ' <span style="color:#666">(' + (x.count || 0) + ')</span>' + keysHtml + '</li>';
        }).join('');
        if (ulFortes) ulFortes.innerHTML = top5F.map(function(x) {
          var t = escapeHtml(x.label || '');
          var keys = x.keys || [];
          var keysHtml = keys.length && jiraBase ? ' <span class="pontos-tickets">' + keys.map(function(k) { return '<a href="' + jiraBase + '/browse/' + encodeURIComponent(k) + '" target="_blank" rel="noopener">' + escapeHtml(k) + '</a>'; }).join(', ') + '</span>' : '';
          return '<li>' + t + ' <span style="color:#666">(' + (x.count || 0) + ')</span>' + keysHtml + '</li>';
        }).join('');
      } else {
        if (relatorioEmpty) relatorioEmpty.style.display = 'block';
        if (relatorioPorChamado) { relatorioPorChamado.style.display = 'none'; relatorioPorChamado.innerHTML = ''; }
        if (relatorioContent) relatorioContent.style.display = 'none';
      }
    }
    var btnPontosOllama = document.getElementById('btnPontosOllama');
    if (btnPontosOllama) {
      btnPontosOllama.onclick = async function() {
        btnPontosOllama.disabled = true;
        msg('Extraindo pontos de melhoria e fortes (Ollama). At\u00e9 12 chamados. Aguarde.', 'info');
        try {
          var r = await fetch('/api/pontos-melhoria', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
          var data = await r.json();
          if (data.error) { msg(data.error, 'error'); }
          else { window.lastPontosMelhoria = data; renderPontosCharts(); msg('Pontos extra\u00eddos. Veja os gr\u00e1ficos na se\u00e7\u00e3o 6.', 'success'); }
        } catch (e) { msg('Erro: ' + e.message, 'error'); }
        btnPontosOllama.disabled = false;
      };
    }

    filterSelect.onchange = async () => {
      if (filterSelect.value === '') { window.selectedFilterId = null; return; }
      const i = parseInt(filterSelect.value);
      if (i < 0 || !window.filtrosData || !window.filtrosData.filters[i]) { window.selectedFilterId = null; return; }
      const f = window.filtrosData.filters[i];
      window.selectedFilterId = f.id || null;
      if (f.jql) { jqlEl.value = f.jql; msg('JQL do filtro aplicada. Ao buscar, serão usadas as mesmas colunas do filtro no Jira.', 'success'); return; }
      msg('Buscando JQL do filtro...', 'info');
      try {
        const r = await fetch('/filtro/' + f.id);
        const data = await r.json();
        if (data.jql) { jqlEl.value = data.jql; msg('JQL do filtro aplicada.', 'success'); }
        else msg(data.error || 'JQL não encontrada.', 'error');
      } catch (e) {
        msg('Erro: ' + e.message, 'error');
      }
    };

    btnAplicarFiltros.onclick = () => {
      const proj = projectSelect.value;
      const status = statusSelect.value;
      if (!proj && !status) { msg('Selecione Projeto e/ou Status.', 'error'); return; }
      const parts = [];
      if (proj) parts.push('project = "' + proj + '"');
      if (status) parts.push('status = "' + status + '"');
      const extra = parts.join(' AND ');
      const current = jqlEl.value.trim();
      if (!current) { jqlEl.value = extra; msg('Filtros aplicados à JQL.', 'success'); return; }
      const orderByIdx = current.toUpperCase().lastIndexOf(' ORDER BY ');
      let condition = current;
      let orderBy = '';
      if (orderByIdx !== -1) {
        condition = current.slice(0, orderByIdx).trim();
        orderBy = current.slice(orderByIdx).trim();
      }
      jqlEl.value = '(' + condition + ') AND ' + extra + (orderBy ? ' ' + orderBy : '');
      msg('Filtros aplicados à JQL.', 'success');
    };

    const slaOverlay = document.getElementById('slaOverlay');
    const slaPanel = document.getElementById('slaPanel');
    const slaTitle = document.getElementById('slaTitle');
    const slaContent = document.getElementById('slaContent');
    document.querySelector('.sla-close').onclick = () => { slaOverlay.classList.remove('show'); };
    slaOverlay.onclick = (e) => { if (e.target === slaOverlay) slaOverlay.classList.remove('show'); };
    resultEl.addEventListener('click', async (e) => {
      if (!e.target.classList.contains('btn-sla')) return;
      const key = e.target.getAttribute('data-key');
      if (!key) return;
      slaTitle.textContent = 'SLAs – ' + key;
      slaContent.innerHTML = '<p class="sla-empty">Carregando...</p>';
      slaOverlay.classList.add('show');
      try {
        const esc = (t) => { const d = document.createElement('div'); d.textContent = t == null ? '' : t; return d.innerHTML; };
        const r = await fetch('/sla/' + encodeURIComponent(key) + '?debug=1');
        const data = await r.json();
        if (data.error) {
          slaContent.innerHTML = '<p class="sla-empty">' + (data.error || 'Erro ao carregar SLAs.') + '</p>' +
            (data.debug ? '<details class="sla-debug"><summary>Resposta bruta da API</summary><pre>' + esc(JSON.stringify(data.debug, null, 2)) + '</pre></details>' : '');
          return;
        }
        const slas = data.slas || [];
        let html = '';
        if (slas.length === 0) {
          html = '<p class="sla-empty">Nenhum SLA configurado para este chamado ou projeto não é Service Desk.</p>';
        } else {
          html = '<ul class="sla-list">' + slas.map(s => {
            const ongoing = !!s.ongoing;
            const met = !!s.met;
            let icons = '';
            if (ongoing) icons = '<span class="sla-icon ongoing" aria-hidden="true">—</span>' + (met ? '<span class="sla-icon met" aria-hidden="true">✓</span>' : '');
            else icons = met ? '<span class="sla-icon met" aria-hidden="true">✓</span>' : '<span class="sla-icon breached" aria-hidden="true">✗</span>';
            const ts = (s.timestamp || '').trim();
            const timePill = ts && ts !== '—' ? '<div class="sla-time-wrap"><span class="sla-time-pill">' + esc(ts) + '</span></div>' : '';
            return '<li class="sla-item"><div class="sla-icons">' + icons + '</div><div class="sla-list-body"><span class="sla-name">' + esc(s.name || '—') + '</span>' + timePill + '</div></li>';
          }).join('') + '</ul>';
        }
        if (data.debug) {
          html += '<details class="sla-debug"><summary>Resposta bruta da API (debug)</summary><pre>' + esc(JSON.stringify(data.debug, null, 2)) + '</pre></details>';
        }
        slaContent.innerHTML = html;
      } catch (err) {
        slaContent.innerHTML = '<p class="sla-empty">Erro: ' + err.message + '</p>';
      }
    });
  </script>
</body>
</html>
'''


def get_auth():
    try:
        return get_jira_credentials()
    except Exception as e:
        raise RuntimeError(f'Credenciais: {e}. Configure .env (JIRA_EMAIL, JIRA_API_TOKEN).')


@app.route('/')
def index():
    from datetime import datetime
    current_year = datetime.now().year
    years_list = list(range(current_year, current_year - 6, -1))
    return render_template_string(HTML_TEMPLATE, default_jql=DEFAULT_JQL, years_list=years_list)


def _jql_with_month_year(jql, month, year):
    """Insere na JQL existente o filtro de data (mês/ano): created no intervalo.
    Não substitui a JQL: adiciona AND created >= "YYYY-MM-01" AND created <= "YYYY-MM-DD"
    antes do ORDER BY, se existir, para manter a ordem.
    """
    import calendar
    try:
        last_day = calendar.monthrange(int(year), int(month))[1]
        first = f'{year}-{month:02d}-01'
        last = f'{year}-{month:02d}-{last_day}'
        condition = f'created >= "{first}" AND created <= "{last}"'
        order_idx = jql.upper().rfind(' ORDER BY ')
        if order_idx != -1:
            jql = jql[:order_idx].strip() + ' AND ' + condition + ' ' + jql[order_idx:].strip()
        else:
            jql = jql.strip() + ' AND ' + condition
        return jql
    except Exception:
        return jql


@app.route('/buscar', methods=['POST'])
def buscar():
    try:
        auth = get_auth()
        data = request.get_json() or {}
        jql = (data.get('jql') or '').strip()
        limit = data.get('limit') or 50
        limit = min(int(limit), 3000)
        filter_id = data.get('filter_id') or None
        month = data.get('month')
        year = data.get('year')
        if month is not None and year is not None and int(month) and int(year):
            # Período selecionado: buscar todos os chamados (sem limite) para CSAT e listagem analisarem todos
            limit = None
            if 'created >= "' not in jql or 'created <= "' not in jql:
                jql = _jql_with_month_year(jql, int(month), int(year))
        if not jql:
            return jsonify({'error': 'JQL vazia.'})
        # Cache da busca (60s) para melhor desempenho em buscas repetidas
        import time as _time
        cache_key = (jql, limit, filter_id or '')
        if getattr(app, '_search_cache', None):
            ck, expiry, cached = app._search_cache
            if ck == cache_key and _time.time() < expiry:
                issues, field_ids, columns = cached
            else:
                app._search_cache = None
        if not getattr(app, '_search_cache', None) or (getattr(app, '_search_cache', None) and app._search_cache[0] != cache_key):
            field_ids = resolve_custom_fields(auth)
            columns = None
            if filter_id:
                try:
                    columns = fetch_filter_columns(auth, filter_id)
                except Exception:
                    columns = None
            issues = search_jql(auth, jql, field_ids, limit=limit, columns=columns)
            app._search_cache = (cache_key, _time.time() + 60, (issues, field_ids, columns))
        else:
            issues, field_ids, columns = app._search_cache[2]
        base_url = JIRA_URL.rstrip('/')
        from html import escape as html_escape
        # SLA: origem única no Jira. sla_by_key alimenta a coluna SLAs do modo lista; gráficos e análise Ollama (pontos fortes/melhoria) usam esses mesmos dados (sla_by_key_relevant para agregações).
        sla_by_key = _fetch_slas_for_issues(auth, issues)
        sla_by_key_relevant = {k: [s for s in v if _sla_name_is_relevant(s.get('name'))] for k, v in sla_by_key.items()}
        if columns:
            # Colunas do filtro, exceto Time to resolution e Time to first response (ficam só na coluna única SLAs); Status no lugar se faltar
            skip_idx = set()
            has_status = False
            for i, c in enumerate(columns):
                lbl = (c.get('label') or '').lower()
                if ('resolution' in lbl or 'resolução' in lbl or 'resolucao' in lbl) and 'first response' not in lbl and 'primeira resposta' not in lbl:
                    skip_idx.add(i)
                if 'first response' in lbl or 'primeira resposta' in lbl:
                    skip_idx.add(i)
                if lbl.strip() == 'status':
                    has_status = True
            header_parts = []
            for i, c in enumerate(columns):
                if i in skip_idx:
                    continue
                header_parts.append(f'<th>{html_escape(str(c["label"]))}</th>')
                if not has_status and len(header_parts) == 2:
                    header_parts.append('<th>Status</th>')
            if not has_status and len(header_parts) < 3:
                header_parts.append('<th>Status</th>')
            headers = ''.join(header_parts) + '<th class="satisfaction-cell">Satisfaction</th><th>Created</th><th>Updated</th><th class="nota-cell">Nota</th><th class="sla-header">SLAs</th>'
            html_rows = []
            for issue in issues:
                values = get_row_values_for_columns(issue, columns, field_ids)
                status_obj = (issue.get('fields') or {}).get('status')
                status_txt = status_obj.get('name', '') if isinstance(status_obj, dict) else (str(status_obj) if status_obj else '')
                created_txt = get_field_display_value(issue, 'created', field_ids)
                updated_txt = get_field_display_value(issue, 'updated', field_ids)
                key = issue.get('key', '')
                slas = sla_by_key.get(key, [])
                cells = []
                cell_count = 0
                for i, col in enumerate(columns):
                    if i in skip_idx:
                        continue
                    val = values[i] if i < len(values) else ''
                    val_str = str(val)
                    if col.get('id') in ('key', 'issuekey') and val_str:
                        val = f'<a href="{base_url}/browse/{html_escape(val_str)}" target="_blank">{html_escape(val_str)}</a>'
                    else:
                        val = html_escape(val_str)
                    cells.append(f'<td>{val}</td>')
                    cell_count += 1
                    if not has_status and cell_count == 2:
                        cells.append(f'<td>{html_escape(status_txt)}</td>')
                if not has_status and cell_count < 3:
                    cells.append(f'<td>{html_escape(status_txt)}</td>')
                _sum, desc_plain = get_issue_summary_and_description(issue, field_ids)
                desc_display = (desc_plain[:250] + '…') if len(desc_plain) > 250 else (desc_plain or '—')
                desc_title = desc_plain.replace('"', '&quot;')[:800] if desc_plain else ''
                cells.append(f'<td class="desc-cell" title="{desc_title}">{html_escape(desc_display)}</td>')
                sat_txt = get_row_values(issue, field_ids).get('Satisfaction') or '—'
                cells.append(f'<td class="satisfaction-cell">{html_escape(str(sat_txt))}</td>')
                cells.append(f'<td>{html_escape(created_txt)}</td>')
                cells.append(f'<td>{html_escape(updated_txt)}</td>')
                cells.append(f'<td class="nota-cell" data-nota-key="{html_escape(key)}">—</td>')
                cells.append(f'<td class="sla-cell">{_sla_inline_html(slas, html_escape)}</td>')
                rt_attr = html_escape((get_row_values(issue, field_ids).get('Request Type') or '').strip() or '(sem tipo)')
                html_rows.append('<tr data-request-type="' + rt_attr + '">' + ''.join(cells) + '</tr>')
            headers = headers.replace('<th class="satisfaction-cell">Satisfaction</th><th>Created</th>', '<th>Descrição</th><th class="satisfaction-cell">Satisfaction</th><th>Created</th>', 1)
            table = f'<div class="table-wrap"><table><thead><tr>{headers}</tr></thead><tbody>' + ''.join(html_rows) + '</tbody></table></div><p class="count">Total: {len(issues)} issues</p>'
        else:
            rows = [get_row_values(issue, field_ids) for issue in issues]
            html_rows = []
            for idx, (r, issue) in enumerate(zip(rows, issues)):
                key = r.get('key', '')
                link = f'<a href="{base_url}/browse/{key}" target="_blank">{key}</a>' if key else ''
                summary, desc_plain = get_issue_summary_and_description(issue, field_ids)
                summary = (summary or '')[:200]
                desc_display = (desc_plain[:250] + '…') if len(desc_plain) > 250 else (desc_plain or '—')
                desc_title = desc_plain.replace('"', '&quot;')[:800] if desc_plain else ''
                fields = issue.get('fields') or {}
                status_obj = fields.get('status')
                status_txt = status_obj.get('name', '') if isinstance(status_obj, dict) else (str(status_obj) if status_obj else '')
                created_txt = get_field_display_value(issue, 'created', field_ids)
                updated_txt = get_field_display_value(issue, 'updated', field_ids)
                slas = sla_by_key.get(key, [])
                sla_html = _sla_inline_html(slas, html_escape)
                rt_val = (r.get('Request Type') or '').strip() or '(sem tipo)'
                sat_val = r.get('Satisfaction') or '—'
                html_rows.append(
                    '<tr data-request-type="' + html_escape(rt_val) + '"><td>' + link + '</td><td class="summary-cell">' + html_escape(summary or '—') + '</td><td class="desc-cell" title="' + desc_title + '">' + html_escape(desc_display) + '</td><td>' + (html_escape(str(r.get('Reporter') or ''))) + '</td><td>' + html_escape(status_txt) + '</td><td>' + (html_escape(str(r.get('Assignee') or ''))) + '</td><td>' + html_escape(rt_val) + '</td><td>' + html_escape(created_txt) + '</td><td>' + html_escape(updated_txt) + '</td><td class="satisfaction-cell">' + html_escape(str(sat_val)) + '</td><td class="nota-cell" data-nota-key="' + html_escape(key) + '">—</td><td class="sla-cell">' + sla_html + '</td></tr>'
                )
            table = '<div class="table-wrap"><table><thead><tr><th>Key</th><th>Título</th><th>Descrição</th><th>Reporter</th><th>Status</th><th>Assignee</th><th>Request Type</th><th>Created</th><th>Updated</th><th class="satisfaction-cell">Satisfaction</th><th class="nota-cell">Nota</th><th class="sla-header">SLAs</th></tr></thead><tbody>' + ''.join(html_rows) + '</tbody></table></div><p class="count">Total: ' + str(len(issues)) + ' issues</p>'
        global _last_result, _last_notas
        # sla_by_key: mesma fonte da coluna SLAs do modo lista; usada para gráficos (sla_by_key_relevant) e análise Ollama (pontos fortes/melhoria)
        _last_result = {'issues': issues, 'field_ids': field_ids, 'jql': jql, 'sla_by_key': sla_by_key}
        keys = [i.get('key') for i in issues if i.get('key')]
        loaded = _load_notas_from_db()
        merged = (_last_notas or {}).copy()
        for k in keys:
            if k in loaded:
                merged[k] = loaded[k]
        _last_notas = {k: merged.get(k) or {'nota': None, 'comentario': '—'} for k in keys}
        # Gráficos de SLA (TTR/FRT) usam APENAS a coluna SLA do modo lista (todos os SLAs da lista, classificados por tipo FRT/TTR); sem fallback.
        stats = stats_ttr_frt_by_request_type_from_sla(issues, sla_by_key, field_ids)
        other_stats = stats_other_by_keywords(issues, field_ids)
        stats['otherByKeyword'] = other_stats.get('otherByKeyword', {})
        stats['otherKeywordList'] = other_stats.get('otherKeywordList', [])
        # Subcategorias sempre por análise da coluna Description (palavras-chave), sem Ollama
        kw_breakdown = stats_keyword_breakdown_by_request_type(issues, field_ids)
        stats['keywordBreakdownByRequestType'] = kw_breakdown.get('keywordBreakdownByRequestType', {})
        stats['csat'] = stats_csat(issues, field_ids)
        stats['totalIssues'] = len(issues)  # total no período (para comparar com chamados com Satisfaction)
        stats['slaAggregate'] = stats_sla_aggregate(sla_by_key_relevant)
        stats['slaPctByPeriod'] = stats_sla_pct_by_period(issues, sla_by_key_relevant, by_month=True)
        stats['slaByAnalyst'] = stats_sla_by_analyst(issues, sla_by_key_relevant)
        stats['ttrFrtByPeriod'] = stats_ttr_frt_by_period(issues, field_ids, by_month=True)
        stats['notaTemporal'] = stats_nota_temporal(issues, _last_notas, by_month=True)
        stats['notaByRequestType'] = stats_nota_by_request_type(issues, _last_notas, field_ids)
        stats['slaByRequestType'] = stats_sla_by_request_type(issues, sla_by_key_relevant, field_ids)
        stats['criticalPctByPeriod'] = stats_critical_pct_by_period(issues, field_ids, by_month=True)
        stats['csatByPeriod'] = stats_csat_by_period(issues, field_ids, by_month=True)
        stats['csatVsNota'] = stats_csat_vs_nota(issues, field_ids, _last_notas)
        stats['csatByRequestType'] = stats_csat_by_request_type(issues, field_ids)
        stats['volumeByPeriod'] = stats_volume_by_period(issues, by_month=True)
        stats['volumeByAnalyst'] = stats_volume_by_analyst(issues)
        # Gráfico de reabertura: mesmo período do modo lista (mês/ano selecionado)
        if month is not None and year is not None and int(month) and int(year):
            stats['reopened'] = stats_reopened_for_period(auth, field_ids, int(month), int(year))
        else:
            stats['reopened'] = {'total': 0, 'byPeriod': [], 'keys': []}
        request_type_keys = {}
        for issue in issues:
            key = issue.get('key')
            if not key:
                continue
            row = get_row_values(issue, field_ids)
            rt = (row.get('Request Type') or '').strip() or '(sem tipo)'
            request_type_keys.setdefault(rt, []).append(key)
        return jsonify({
            'count': len(issues),
            'html': table,
            'stats': stats,
            'notas': _last_notas,
            'requestTypeKeys': request_type_keys,
            'jiraBaseUrl': JIRA_URL.rstrip('/'),
        })
    except Exception as e:
        return jsonify({'error': str(e)})


def _nota_is_evaluated(entry):
    """True se o chamado foi avaliado de verdade (nota 1–5)."""
    if not entry:
        return False
    n = entry.get('nota')
    return n is not None and isinstance(n, (int, float)) and 1 <= int(n) <= 5


def _evaluate_one_issue(key, issue, field_ids, auth, ollama_url, ollama_model):
    """Avalia um único chamado usando apenas Ollama. Várias tentativas; fallback para regras só se Ollama não responder após todas."""
    comments_text = fetch_issue_comments_text(auth, issue.get('key')) if auth else ''
    max_tentativas = int(os.environ.get('OLLAMA_NOTAS_MAX_RETRIES', '5'))
    for tentativa in range(max_tentativas):
        r = get_issue_note_from_ollama(
            issue,
            ollama_url,
            model=ollama_model,
            field_ids=field_ids,
            comments_text=comments_text or None,
            auth=auth,
        )
        if _nota_is_evaluated(r):
            return r
        if tentativa < max_tentativas - 1:
            time.sleep(2)
    r = get_issue_note_rule_based(issue, field_ids)
    return {'nota': r.get('nota'), 'comentario': ('(regras, Ollama sem resposta) ' + (r.get('comentario') or ''))[:200]}


def _run_evaluate_all_until_complete(force_reavaliar=False):
    """
    Usa cache + SQLite para saber quem já tem nota (exceto se force_reavaliar=True).
    Avalia chamado por chamado (um por vez). Só encerra quando todas as linhas do resultado atual tiverem nota (1–5).
    Persiste no SQLite após cada avaliação.
    Se force_reavaliar=True, ignora cache e banco e reavalia todos os chamados do resultado atual.
    """
    global _last_result, _last_notas
    if not _last_result or not _last_result.get('issues'):
        return None, 'Faça uma busca antes de calcular notas.'
    ollama_url = os.environ.get('OLLAMA_URL', '').strip() or None
    if not ollama_url:
        return None, 'Configure OLLAMA_URL no .env (ex.: http://localhost:11434).'
    ollama_model = os.environ.get('OLLAMA_MODEL', '').strip() or None
    field_ids = _last_result.get('field_ids') or {}
    try:
        auth = get_auth()
    except Exception:
        auth = None

    issues = _last_result['issues']
    task_list = [(issue.get('key'), issue) for issue in issues if issue.get('key')]
    if not task_list:
        return {}, None
    keys = [t[0] for t in task_list]
    issue_by_key = {k: issue for k, issue in task_list}

    loaded = _load_notas_from_db() if not force_reavaliar else {}
    out = {}
    cache = {} if force_reavaliar else (_last_notas or {})
    for k in keys:
        if not force_reavaliar and k in loaded and _nota_is_evaluated(loaded[k]):
            out[k] = loaded[k]
        elif not force_reavaliar and k in cache and _nota_is_evaluated(cache.get(k)):
            out[k] = cache[k]
        else:
            out[k] = loaded.get(k) or cache.get(k) or {'nota': None, 'comentario': '—'}
    keys_sem_nota = [k for k in keys if not _nota_is_evaluated(out[k])]

    while keys_sem_nota:
        for key in keys_sem_nota:
            issue = issue_by_key.get(key)
            if not issue:
                continue
            result = _evaluate_one_issue(key, issue, field_ids, auth, ollama_url, ollama_model)
            out[key] = result
            _last_notas = dict(out)
            _save_notas_to_db(out)
        keys_sem_nota = [k for k in keys if not _nota_is_evaluated(out[k])]
        if keys_sem_nota:
            time.sleep(1)

    _last_notas = out
    _save_notas_to_db(out)
    return out, None


def _load_notas_from_db():
    """Carrega notas do SQLite. Quem já está no banco não será reavaliado. Cache interno (_last_notas) é usado na sessão."""
    _init_db_notas()
    try:
        conn = sqlite3.connect(_db_path())
        cur = conn.execute('SELECT issue_key, nota, comentario FROM notas')
        out = {}
        for row in cur.fetchall():
            key = (row[0] or '').strip()
            if not key:
                continue
            nota = row[1]
            if nota is not None:
                try:
                    n = int(nota)
                    if n < 1 or n > 5:
                        nota = None
                except (TypeError, ValueError):
                    nota = None
            out[key] = {'nota': nota, 'comentario': (row[2] or '')[:500]}
        conn.close()
        return out
    except Exception:
        return {}


def _save_notas_to_db(notas):
    """Persiste notas no SQLite e mantém cache (_last_notas) em memória."""
    if not notas:
        return
    _init_db_notas()
    try:
        conn = sqlite3.connect(_db_path())
        for key, v in notas.items():
            if not (key and isinstance(key, str) and key.strip()):
                continue
            nota = v.get('nota') if isinstance(v, dict) else None
            comentario = (v.get('comentario') or '')[:500] if isinstance(v, dict) else ''
            conn.execute(
                'INSERT OR REPLACE INTO notas (issue_key, nota, comentario, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
                (key.strip(), nota, comentario)
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


@app.route('/api/notas', methods=['POST'])
def api_notas():
    """Avalia todos os chamados do resultado atual, um por um. Usa cache + SQLite; só encerra quando toda linha tiver nota (1–5)."""
    try:
        out, err = _run_evaluate_all_until_complete()
        if err:
            return jsonify({'error': err}), 400
        return jsonify(out)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/notas-restante', methods=['POST'])
def api_notas_restante():
    """Igual a Calcular notas: avalia chamado por chamado até todos terem nota; usa cache e SQLite para pular os já avaliados."""
    try:
        out, err = _run_evaluate_all_until_complete()
        if err:
            return jsonify({'error': err}), 400
        return jsonify(out)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/notas-reavaliar', methods=['POST'])
def api_notas_reavaliar():
    """Reavalia todas as notas do resultado atual: ignora cache e banco, recalcula todas via Ollama (valida todas as notas)."""
    try:
        out, err = _run_evaluate_all_until_complete(force_reavaliar=True)
        if err:
            return jsonify({'error': err}), 400
        return jsonify(out)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/subcategorias-ollama', methods=['POST'])
def api_subcategorias_ollama():
    """Reclassifica subcategorias com Ollama (reflete melhor o caso real que palavras-chave). Retorna novo keywordBreakdownByRequestType."""
    try:
        global _last_result
        if not _last_result or not _last_result.get('issues'):
            return jsonify({'error': 'Faça uma busca antes.'}), 400
        ollama_url = os.environ.get('OLLAMA_URL', '').strip() or None
        if not ollama_url:
            return jsonify({'error': 'Configure OLLAMA_URL no .env.'}), 400
        issues = _last_result['issues']
        field_ids = _last_result.get('field_ids') or {}
        ollama_model = os.environ.get('OLLAMA_MODEL', '').strip() or None
        kw_breakdown = stats_keyword_breakdown_by_request_type_ollama(issues, field_ids, ollama_url, ollama_model)
        return jsonify({
            'keywordBreakdownByRequestType': kw_breakdown.get('keywordBreakdownByRequestType', {}),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pontos-melhoria', methods=['POST'])
def api_pontos_melhoria():
    """Extrai pontos de melhoria e fortes via Ollama (até 40 issues do resultado atual). Inclui comentários do Jira na análise quando existirem."""
    try:
        global _last_result
        if not _last_result or not _last_result.get('issues'):
            return jsonify({'error': 'Faça uma busca antes.'}), 400
        ollama_url = os.environ.get('OLLAMA_URL', '').strip() or None
        if not ollama_url:
            return jsonify({'error': 'Configure OLLAMA_URL no .env.'}), 400
        auth = get_auth()
        issues = _last_result['issues']
        field_ids = _last_result.get('field_ids') or {}
        sla_by_key = _last_result.get('sla_by_key') or {}
        data = stats_pontos_melhoria_fortes(issues, ollama_url, field_ids, limit=12, auth=auth, sla_by_key=sla_by_key)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/export', methods=['POST'])
def export():
    try:
        global _last_result
        if not _last_result:
            return jsonify({'error': 'Nenhum resultado para exportar. Faça uma busca antes.'}), 400
        issues = _last_result['issues']
        field_ids = _last_result['field_ids']
        jql = _last_result.get('jql', '')
        from l1_dashboard import write_html
        import tempfile
        path = os.path.join(tempfile.gettempdir(), 'jira_dashboard_export.html')
        write_html(issues, field_ids, path, jql)
        with open(path, 'rb') as f:
            body = f.read()
        os.remove(path)
        from flask import Response
        return Response(body, mimetype='text/html', headers={'Content-Disposition': 'attachment; filename=dashboard.html'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _normalize_jira_list(data, item_key=None):
    """Se a API retornar lista, usa; se for dict com 'values' ou 'results', extrai a lista."""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        lst = data.get('values') or data.get('results') or data.get('items') or data.get('data') or []
        if isinstance(lst, list):
            return lst
    return []


@app.route('/filtros')
def filtros():
    try:
        auth = get_auth()
        filters_data = fetch_my_filters(auth)
        filters_raw = filters_data if isinstance(filters_data, list) else _normalize_jira_list(filters_data)
        if not filters_raw and isinstance(filters_data, dict) and 'id' in filters_data:
            filters_raw = [filters_data]
        filters = []
        for f in (filters_raw or []):
            if not isinstance(f, dict):
                continue
            filters.append({
                'id': str(f.get('id', '')),
                'name': f.get('name', '') or f.get('label', ''),
                'jql': f.get('jql', ''),
            })
        projects_data = fetch_projects(auth)
        projects_list = projects_data if isinstance(projects_data, list) else _normalize_jira_list(projects_data)
        projects = []
        for p in (projects_list or []):
            if isinstance(p, dict):
                projects.append({'key': p.get('key', ''), 'name': p.get('name', '')})
            elif isinstance(p, (list, tuple)) and len(p) >= 2:
                projects.append({'key': str(p[0]), 'name': str(p[1])})
        statuses_data = fetch_statuses(auth)
        statuses = statuses_data if isinstance(statuses_data, list) else _normalize_jira_list(statuses_data)
        if statuses and isinstance(statuses[0], dict):
            statuses = [s.get('name', '') or s.get('id', '') for s in statuses if s.get('name') or s.get('id')]
        return jsonify({'filters': filters, 'projects': projects, 'statuses': statuses or []})
    except Exception as e:
        import traceback
        err_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                err_msg = 'Jira: {} - {}'.format(getattr(e.response, 'status_code', ''), err_msg)
            except Exception:
                pass
        return jsonify({'error': err_msg})


@app.route('/filtro/<filter_id>')
def filtro(filter_id):
    try:
        auth = get_auth()
        f = fetch_filter_by_id(auth, filter_id)
        return jsonify({'jql': f.get('jql', '')})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/sla/<issue_key>')
def sla(issue_key):
    """Retorna SLAs do chamado (Jira Service Management). ?debug=1 inclui a resposta bruta da API."""
    try:
        auth = get_auth()
        debug = request.args.get('debug', '').lower() in ('1', 'true', 'yes')
        slas = fetch_issue_sla(auth, issue_key)
        out = {'issue_key': issue_key, 'slas': slas}
        if debug:
            status, raw, err = fetch_issue_sla_raw(auth, issue_key)
            out['debug'] = {'status_code': status, 'raw_response': raw, 'fetch_error': err}
        return jsonify(out)
    except Exception as e:
        return jsonify({'error': str(e), 'slas': []})


def open_browser(port):
    webbrowser.open(f'http://127.0.0.1:{port}/')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f'L1 Dashboard (web) em http://127.0.0.1:{port}/')
    print('Feche o servidor com Ctrl+C.')
    Timer(1, lambda: open_browser(port)).start()
    app.run(host='127.0.0.1', port=port, debug=False, use_reloader=False)
