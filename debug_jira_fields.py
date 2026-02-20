#!/usr/bin/env python3
"""Debug: imprime o que a API do Jira retorna para um issue (campos de tempo)."""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from l1_dashboard import (
    JIRA_URL,
    get_jira_credentials,
    resolve_custom_fields,
    search_jql,
    NUBANK_TIME_TO_RESOLUTION_FID,
    NUBANK_TIME_TO_FIRST_RESPONSE_FID,
)

def main():
    key = sys.argv[1] if len(sys.argv) > 1 else None
    if not key:
        print("Uso: python debug_jira_fields.py [ISSUE_KEY]")
        print("Ex.: python debug_jira_fields.py IT-6441")
        print("Se não passar key, busca 1 issue pela JQL 'order by created DESC' e inspeciona.")
        key = None
    auth = get_jira_credentials()
    field_ids = resolve_custom_fields(auth)
    jql = f'key = {key}' if key else 'order by created DESC'
    issues = search_jql(auth, jql, field_ids, limit=1)
    if not issues:
        print("Nenhum issue encontrado.")
        return
    issue = issues[0]
    fields = issue.get('fields', {})
    print("Issue:", issue.get('key'))
    print("Campos presentes (keys):", sorted(fields.keys()))
    print()
    for label, fid in [
        ("Time to resolution (customfield_10886)", NUBANK_TIME_TO_RESOLUTION_FID),
        ("Time to first response (customfield_10884)", NUBANK_TIME_TO_FIRST_RESPONSE_FID),
        ("created", "created"),
        ("resolutiondate", "resolutiondate"),
    ]:
        val = fields.get(fid)
        print(f"{label}:")
        print(f"  tipo={type(val).__name__!r}  valor={val!r}")
        if isinstance(val, dict):
            print(f"  (dict keys: {list(val.keys())})")
        print()
    # Qualquer outro customfield que contenha 10886 ou 10884
    for k, v in fields.items():
        if '10886' in k or '10884' in k or ('time' in k.lower() and ('resolution' in k.lower() or 'response' in k.lower())):
            if k not in (NUBANK_TIME_TO_RESOLUTION_FID, NUBANK_TIME_TO_FIRST_RESPONSE_FID):
                print(f"Outro campo relevante: {k} = {v!r}")

if __name__ == "__main__":
    main()
