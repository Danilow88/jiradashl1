#!/usr/bin/env python3
"""
Simula: busca Jira -> extração de pontos (Ollama) -> geração de notas (Ollama).
Requer .env com JIRA_EMAIL, JIRA_API_TOKEN e OLLAMA_URL (para pontos e notas).
Rode: python test_busca_pontos_notas.py
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

def main():
    from l1_dashboard_web import app, DEFAULT_JQL

    client = app.test_client()
    base = 'http://test/'

    # 1) Busca (JQL padrão, limit 3 para ser rápido)
    print('1) POST /buscar (JQL padrão, limit=3)...')
    r = client.post('/buscar', json={'jql': DEFAULT_JQL, 'limit': 3}, content_type='application/json')
    if r.status_code != 200:
        try:
            err = r.get_json() or {}
            msg = err.get('error', r.text or f'status {r.status_code}')
        except Exception:
            msg = r.text or f'status {r.status_code}'
        print(f'   Falha: {msg}')
        return 1
    data = r.get_json() or {}
    count = data.get('count', 0)
    print(f'   OK -> {count} issue(s) retornado(s)')

    if count == 0:
        print('   Nenhum issue na busca; injetando 1 issue fake para testar extração e notas (Ollama).')
        # Injeta resultado mínimo para testar /api/pontos-melhoria e /api/notas
        import l1_dashboard_web as web
        fake_issue = {
            'key': 'TEST-1',
            'fields': {
                'summary': 'Teste de extração e nota',
                'description': 'Chamado de teste para validar Ollama.',
                'status': {'name': 'Done'},
                'created': '2025-01-15T10:00:00.000-0300',
                'updated': '2025-01-15T12:00:00.000-0300',
            },
        }
        web._last_result = {
            'issues': [fake_issue],
            'field_ids': {},
            'jql': DEFAULT_JQL,
        }
        web._last_notas = {'TEST-1': {'nota': None, 'comentario': '—'}}
        count = 1

    # 2) Extração de pontos de melhoria/fortes (Ollama)
    print('\n2) POST /api/pontos-melhoria...')
    r2 = client.post('/api/pontos-melhoria', json={}, content_type='application/json')
    if r2.status_code == 200:
        out = r2.get_json() or {}
        top_m = out.get('top5Melhoria') or []
        top_f = out.get('top5Fortes') or []
        print(f'   OK -> top5Melhoria: {len(top_m)} itens, top5Fortes: {len(top_f)} itens')
        for i, item in enumerate(top_m[:2]):
            lbl = item.get('label', item) if isinstance(item, dict) else item
            print(f'      Melhoria {i+1}: {str(lbl)[:70]}')
        for i, item in enumerate(top_f[:2]):
            lbl = item.get('label', item) if isinstance(item, dict) else item
            print(f'      Forte {i+1}: {str(lbl)[:70]}')
    else:
        err = (r2.get_json() or {}).get('error', r2.text or f'status {r2.status_code}')
        print(f'   Resposta: {r2.status_code} -> {err}')

    # 3) Geração de notas (Ollama) — avalia até todos do resultado atual terem nota
    print('\n3) POST /api/notas (pode demorar se Ollama estiver rodando)...')
    r3 = client.post('/api/notas', json={}, content_type='application/json')
    if r3.status_code == 200:
        # API retorna o dicionário direto: { issue_key: { nota, comentario } }
        out = r3.get_json() or {}
        if isinstance(out, dict) and out and not any(k in out for k in ('error', 'notas')):
            notas = out
        else:
            notas = out.get('notas') or {}
        n_avaliados = sum(1 for v in notas.values() if v and v.get('nota') is not None)
        print(f'   OK -> {len(notas)} chamados, {n_avaliados} com nota (1-5)')
        for k, v in list(notas.items())[:5]:
            n = v.get('nota') if v else None
            c = (v.get('comentario') or '')[:60] if v else ''
            print(f'      {k}: nota={n} | {c}')
    else:
        err = (r3.get_json() or {}).get('error', r3.text or f'status {r3.status_code}')
        print(f'   Resposta: {r3.status_code} -> {err}')

    print('\nConcluído.')
    return 0

if __name__ == '__main__':
    sys.exit(main())
