#!/usr/bin/env python3
"""
Teste: garante que os dados de SLA no modo lista são alimentados do Jira.
Simula a resposta da API do Jira e verifica que o HTML da lista contém esses dados.
Rode: python test_sla_list_from_jira.py
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Carrega .env antes de importar o app
from dotenv import load_dotenv
load_dotenv()


def test_sla_list_fed_from_jira():
    """Verifica que o modo lista usa _fetch_slas_for_issues (que chama fetch_issue_sla = API Jira)."""
    from unittest.mock import patch, MagicMock
    from l1_dashboard_web import app, _fetch_slas_for_issues, _sla_inline_html
    from html import escape as html_escape

    # Dados simulados da API do Jira (como retornados por fetch_issue_sla)
    jira_sla_response = [
        {'name': 'Time to first response', 'timestamp': 'Today 7:59 AM', 'met': True, 'tipo': 'FRT', 'ongoing': False},
        {'name': 'Time to Resolution (TTR)', 'timestamp': '0m (em andamento)', 'met': True, 'tipo': 'TTR', 'ongoing': True},
    ]

    with patch('l1_dashboard_web.fetch_issue_sla', return_value=jira_sla_response) as mock_fetch:
        auth = MagicMock()
        issues = [{'key': 'TEST-123', 'fields': {}}]
        sla_by_key = _fetch_slas_for_issues(auth, issues)
        mock_fetch.assert_called_once_with(auth, 'TEST-123')
        assert 'TEST-123' in sla_by_key
        assert len(sla_by_key['TEST-123']) == 2
        assert sla_by_key['TEST-123'][0]['name'] == 'Time to first response'
        assert sla_by_key['TEST-123'][1]['name'] == 'Time to Resolution (TTR)'

    # HTML da célula SLA deve conter os nomes vindos do Jira
    html = _sla_inline_html(jira_sla_response, html_escape)
    assert 'Time to first response' in html
    assert 'Time to Resolution (TTR)' in html
    assert 'Today 7:59 AM' in html or '7:59' in html
    assert 'em andamento' in html
    assert 'sla-list-inline' in html
    assert 'sla-name' in html

    print('  OK  SLA do modo lista é alimentado pelo Jira (_fetch_slas_for_issues -> fetch_issue_sla).')
    print('  OK  HTML da lista contém os dados retornados pela API (nomes e timestamps).')
    return True


if __name__ == '__main__':
    print('Testando: dados de SLA no modo lista vêm do Jira...\n')
    try:
        test_sla_list_fed_from_jira()
        print('\nResultado: teste passou.')
        sys.exit(0)
    except Exception as e:
        print(f'\nFalha: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
