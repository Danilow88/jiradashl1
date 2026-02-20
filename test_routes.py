#!/usr/bin/env python3
"""
Teste rápido das rotas do L1 Dashboard (Flask).
Verifica se as rotas existem e respondem (não 404).
Rode: python test_routes.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Carrega .env antes de importar o app
from dotenv import load_dotenv
load_dotenv()

def test_routes():
    from l1_dashboard_web import app
    client = app.test_client()
    ok = 0
    fail = 0

    # GET /
    r = client.get('/')
    if r.status_code == 200:
        print('  OK  GET /')
        ok += 1
    else:
        print(f'  FAIL GET / -> {r.status_code}')
        fail += 1

    # POST /buscar (sem dados ou JQL vazio -> 400 ou 500 por credenciais)
    r = client.post('/buscar', json={}, content_type='application/json')
    if r.status_code != 404:
        print(f'  OK  POST /buscar -> {r.status_code}')
        ok += 1
    else:
        print(f'  FAIL POST /buscar -> 404')
        fail += 1

    # POST /api/pontos-melhoria (sem busca prévia -> 400 "Faça uma busca antes")
    r = client.post('/api/pontos-melhoria', json={}, content_type='application/json')
    if r.status_code == 400:
        data = r.get_json() or {}
        if 'Faça uma busca' in (data.get('error') or ''):
            print('  OK  POST /api/pontos-melhoria -> 400 (esperado: faça uma busca)')
        else:
            print(f'  OK  POST /api/pontos-melhoria -> 400')
        ok += 1
    elif r.status_code == 500:
        # Pode dar 500 se get_auth() rodar antes do check de _last_result em algum fluxo
        print(f'  OK  POST /api/pontos-melhoria -> 500 (rota existe)')
        ok += 1
    elif r.status_code == 404:
        print('  FAIL POST /api/pontos-melhoria -> 404')
        fail += 1
    else:
        print(f'  OK  POST /api/pontos-melhoria -> {r.status_code}')
        ok += 1

    # POST /api/notas
    r = client.post('/api/notas', json={}, content_type='application/json')
    if r.status_code != 404:
        print(f'  OK  POST /api/notas -> {r.status_code}')
        ok += 1
    else:
        print('  FAIL POST /api/notas -> 404')
        fail += 1

    # GET /filtros (pode 500 sem credenciais)
    r = client.get('/filtros')
    if r.status_code != 404:
        print(f'  OK  GET /filtros -> {r.status_code}')
        ok += 1
    else:
        print('  FAIL GET /filtros -> 404')
        fail += 1

    # POST /export (sem resultado -> 400)
    r = client.post('/export')
    if r.status_code in (400, 302, 200):
        print(f'  OK  POST /export -> {r.status_code}')
        ok += 1
    elif r.status_code != 404:
        print(f'  OK  POST /export -> {r.status_code}')
        ok += 1
    else:
        print('  FAIL POST /export -> 404')
        fail += 1

    print('')
    print(f'Resultado: {ok} ok, {fail} falha(s).')
    return fail == 0

if __name__ == '__main__':
    print('Testando rotas do L1 Dashboard...\n')
    success = test_routes()
    sys.exit(0 if success else 1)
