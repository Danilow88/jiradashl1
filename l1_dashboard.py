#!/usr/bin/env python3
"""
L1 Support Dashboard - Jira issues filtered by Support Level L1.

Uses the same Jira connection as jira_utils.py. Displays:
  Reporter | Time to resolution | Time to first response | Assignee | Request Type

JQL: (component not in (gadgets-devices) OR component IS EMPTY) AND "Support Level - ITOPS" = "L1"
     ORDER BY "Time to resolution" ASC
"""

import argparse
import os
import sys
import time
import unicodedata

# Add parent so we can import jira_utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    from jira_utils import (
        JIRA_URL,
        get_jira_credentials,
    )
except ImportError:
    JIRA_URL = os.environ.get('JIRA_URL', 'https://cornelisnetworks.atlassian.net')

    def get_jira_credentials():
        email = os.environ.get('JIRA_EMAIL')
        api_token = os.environ.get('JIRA_API_TOKEN')
        if not email:
            raise RuntimeError('JIRA_EMAIL environment variable not set')
        if not api_token:
            raise RuntimeError('JIRA_API_TOKEN environment variable not set')
        return email, api_token

# Default JQL for L1 dashboard (as requested)
DEFAULT_JQL = (
    '(component not in (gadgets-devices) OR component IS EMPTY) '
    'AND "Support Level - ITOPS" = "L1" '
    'ORDER BY "Time to resolution" ASC'
)

# Display column names (from the user's image)
COLUMNS = [
    'Reporter',
    'Time to resolution',
    'Time to first response',
    'Assignee',
    'Request Type',
]


def get_field_id_by_name(fields_list, name):
    """Return field id for a given field name (exact, then case-insensitive, then partial)."""
    if not name:
        return None
    name_clean = name.strip().lower()
    # 1) Exact match
    for f in fields_list:
        if f.get('name') == name:
            return f.get('id')
    # 2) Case-insensitive
    for f in fields_list:
        if (f.get('name') or '').strip().lower() == name_clean:
            return f.get('id')
    # 3) Partial: search name inside field name (e.g. "Time to resolution" in "Time to resolution (days)")
    for f in fields_list:
        fn = (f.get('name') or '').strip().lower()
        if name_clean in fn or fn in name_clean:
            return f.get('id')
    return None


def fetch_jira_fields(auth):
    """GET /rest/api/3/field and return list of fields."""
    url = f'{JIRA_URL}/rest/api/3/field'
    r = requests.get(
        url,
        auth=auth,
        headers={'Accept': 'application/json'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# Nomes alternativos para tentar no Jira (ex.: Nubank, JSM)
# Nubank: "Nubank - Time for Resolution" (customfield_10886), "Nubank - First time to Response" (customfield_10884)
TIME_TO_RESOLUTION_NAMES = [
    'Nubank - Time for Resolution', 'Time to resolution', 'Resolution time', 'Resolution Time',
    '[RA] Time to resolution', 'Time to resolution (CA)', 'Time to Resolution (Max Time)',
    'Tempo para resolução', 'Time to resolution (seconds)',
]
TIME_TO_FIRST_RESPONSE_NAMES = [
    'Nubank - First time to Response', 'Time to first response', 'First response time', 'First Response Time',
    'First Response Time (FRT)', 'CCRC Incident Time to First Response',
    'Tempo para primeira resposta', 'Time to first response (seconds)',
]


def resolve_custom_fields(auth):
    """
    Resolve display names to Jira field IDs.
    Returns dict: { 'Time to resolution': 'customfield_xxxx', ... }
    """
    fields_list = fetch_jira_fields(auth)
    resolved = {}
    for name in ['Request Type', 'Support Level - ITOPS', 'Satisfaction']:
        fid = get_field_id_by_name(fields_list, name)
        resolved[name] = fid if fid else None
    fid = None
    for name in TIME_TO_RESOLUTION_NAMES:
        fid = get_field_id_by_name(fields_list, name)
        if fid:
            break
    resolved['Time to resolution'] = fid
    if not resolved['Time to resolution']:
        for f in fields_list:
            n = (f.get('name') or '').lower()
            if 'resolution' in n and ('time' in n or 'tempo' in n):
                resolved['Time to resolution'] = f.get('id')
                break
    fid = None
    for name in TIME_TO_FIRST_RESPONSE_NAMES:
        fid = get_field_id_by_name(fields_list, name)
        if fid:
            break
    resolved['Time to first response'] = fid
    if not resolved['Time to first response']:
        for f in fields_list:
            n = (f.get('name') or '').lower()
            if ('first' in n and 'response' in n) or ('primeira' in n and 'resposta' in n):
                resolved['Time to first response'] = f.get('id')
                break
    return resolved


def list_all_fields(auth, search=None):
    """Print all Jira fields (id, name). Optionally filter by search substring."""
    fields_list = fetch_jira_fields(auth)
    search_lower = (search or '').strip().lower()
    out = []
    for f in sorted(fields_list, key=lambda x: (x.get('name') or '').lower()):
        fid = f.get('id', '')
        fname = f.get('name') or ''
        if search_lower and search_lower not in fname.lower():
            continue
        out.append((fid, fname))
    return out


def fetch_my_filters(auth):
    """GET /rest/api/3/filter/my - lista filtros do usuário."""
    r = requests.get(
        f'{JIRA_URL}/rest/api/3/filter/my',
        auth=auth,
        headers={'Accept': 'application/json'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def fetch_filter_by_id(auth, filter_id):
    """GET /rest/api/3/filter/{id} - retorna filtro com JQL."""
    r = requests.get(
        f'{JIRA_URL}/rest/api/3/filter/{filter_id}',
        auth=auth,
        headers={'Accept': 'application/json'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def fetch_filter_columns(auth, filter_id):
    """GET /rest/api/3/filter/{id}/columns - retorna colunas configuradas do filtro (id, label)."""
    r = requests.get(
        f'{JIRA_URL}/rest/api/3/filter/{filter_id}/columns',
        auth=auth,
        headers={'Accept': 'application/json'},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        data = data.get('columns') or data.get('items') or []
    if not isinstance(data, list):
        return []
    out = []
    for col in data:
        fid = col.get('id') or col.get('key')
        label = col.get('label') or col.get('name') or fid or ''
        if fid:
            out.append({'id': fid, 'label': label})
    return out


def fetch_projects(auth):
    """GET /rest/api/3/project - lista projetos (key, name)."""
    r = requests.get(
        f'{JIRA_URL}/rest/api/3/project',
        auth=auth,
        headers={'Accept': 'application/json'},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return [(p.get('key', ''), p.get('name', '')) for p in data]


def fetch_statuses(auth):
    """GET /rest/api/3/status - lista status (name)."""
    r = requests.get(
        f'{JIRA_URL}/rest/api/3/status',
        auth=auth,
        headers={'Accept': 'application/json'},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return [s.get('name', '') for s in data if s.get('name')]


_SLA_MONTHS = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')


def _sla_format_datetime(dt):
    """Formata datetime como no Jira: 05/Jan/26 5:05 PM."""
    from datetime import datetime
    if not isinstance(dt, datetime):
        return ''
    h = dt.hour
    h12 = (h % 12) or 12
    am_pm = 'AM' if h < 12 else 'PM'
    return f'{dt.day:02d}/{_SLA_MONTHS[dt.month - 1]}/{str(dt.year)[2:]} {h12}:{dt.minute:02d} {am_pm}'


def _sla_elapsed_to_seconds(obj):
    """Extrai duração em segundos de um objeto da API de SLA (elapsedTime: int segundos ou ms, ou dict com friendly/elapsed).
    Retorna int ou None se não for possível obter segundos."""
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        n = int(obj)
        if n < 0:
            return None
        if n > 86400 * 365:  # provavelmente milissegundos
            n = n // 1000
        return n if n < 86400 * 365 else None
    if isinstance(obj, dict):
        elapsed = obj.get('elapsedTime') or obj.get('seconds') or obj.get('duration')
        if elapsed is not None:
            return _sla_elapsed_to_seconds(elapsed)
        if 'seconds' in obj:
            return int(obj['seconds']) if obj['seconds'] is not None else None
    return None


def _sla_time_to_epoch_seconds(obj):
    """Converte objeto de data da API de SLA (epochMillis, iso8601, etc.) em segundos desde epoch. Só dados Jira."""
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        ms = int(obj)
        if ms > 1e12:
            return ms // 1000
        if ms > 86400 * 365:
            return ms // 1000
        return ms if ms > 0 else None
    if isinstance(obj, dict):
        ms = obj.get('epochMillis')
        if ms is not None:
            return _sla_time_to_epoch_seconds(ms)
        iso = obj.get('iso8601') or obj.get('jira')
        if iso and isinstance(iso, str):
            dt = _parse_iso_date(iso.strip())
            if dt:
                return int(dt.timestamp())
    if isinstance(obj, str) and obj.strip():
        dt = _parse_iso_date(obj.strip())
        if dt:
            return int(dt.timestamp())
    return None


def _sla_time_display(obj):
    """Extrai texto de data/hora ou duração de um objeto da API de SLA (friendly, iso8601, epochMillis, elapsedTime).
    Formato de data: 05/Jan/26 5:05 PM (como no dashboard do Jira).
    """
    if not obj:
        return ''
    if isinstance(obj, (int, float)):
        if obj > 1e12:
            from datetime import datetime
            try:
                dt = datetime.utcfromtimestamp(obj / 1000.0)
                return _sla_format_datetime(dt)
            except Exception:
                return str(int(obj))
        return _format_seconds_hhmm(int(obj)) if obj < 86400 * 365 else ''
    if isinstance(obj, dict):
        s = obj.get('friendly') or obj.get('jira') or obj.get('iso8601') or ''
        if s:
            return str(s).strip()
        ms = obj.get('epochMillis')
        if ms is not None:
            from datetime import datetime
            try:
                dt = datetime.utcfromtimestamp(int(ms) / 1000.0)
                return _sla_format_datetime(dt)
            except Exception:
                pass
        elapsed = obj.get('elapsedTime')
        if elapsed is not None:
            if isinstance(elapsed, dict) and 'friendly' in elapsed:
                return str(elapsed.get('friendly', '')).strip()
            if isinstance(elapsed, (int, float)):
                n = int(elapsed)
                if n > 86400 * 365:
                    n = n // 1000
                return _format_seconds_hhmm(n)
    return str(obj).strip() if obj else ''


def fetch_issue_sla(auth, issue_key):
    """
    GET /rest/servicedeskapi/request/{issueIdOrKey}/sla - SLAs do chamado (Jira Service Management).
    Retorna lista de dicts: [{'name': '...', 'timestamp': '...', 'met': bool, 'ongoing': bool}, ...]
    Se o projeto não for Service Desk ou API retornar 404/403, tenta GET request/{key}?expand=sla (Jira Cloud).
    """
    if not issue_key or not issue_key.strip():
        return []
    key = issue_key.strip()
    data = None
    try:
        r = requests.get(
            f'{JIRA_URL}/rest/servicedeskapi/request/{key}/sla',
            auth=auth,
            headers={'Accept': 'application/json'},
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
        elif r.status_code in (404, 403, 400):
            # Fallback: Jira Cloud pode expor SLA via expand=sla no request
            r2 = requests.get(
                f'{JIRA_URL}/rest/servicedeskapi/request/{key}',
                auth=auth,
                headers={'Accept': 'application/json'},
                params={'expand': 'sla'},
                timeout=15,
            )
            if r2.status_code == 200:
                data = r2.json()
                # SLA pode estar em _expands.sla ou sla ou slaMetrics
                expanded = (data.get('_expands') or [])
                if 'sla' in expanded and isinstance(data.get('sla'), list):
                    data = {'values': data['sla']}
                elif isinstance(data.get('sla'), list):
                    data = {'values': data['sla']}
                else:
                    data = None
            if data is None:
                return []
        else:
            r.raise_for_status()
    except Exception:
        return []
    if data is None:
        return []
    try:
        # Estrutura pode ser {"values": [...]}, {"slaMetrics": [...]}, {"_embedded": {"slaMetrics": [...]}}, ou lista direta
        items = (
            data.get('values') or data.get('slaMetrics') or data.get('items') or data.get('results')
            or (data.get('_embedded') or {}).get('slaMetrics')
            or (data.get('_embedded') or {}).get('values')
        )
        if items is None and isinstance(data, list):
            items = data
        if items is None:
            items = []
        out = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = (item.get('name') or item.get('goalName') or item.get('slaName') or item.get('label')
                    or item.get('metricName') or '').strip()
            # Tempo: múltiplas fontes (top-level, completedCycles, ongoingCycle; aceita friendly "Today 7:59 AM")
            ts = ''
            if isinstance(item.get('friendly'), str) and item['friendly'].strip():
                ts = item['friendly'].strip()
            if not ts:
                for time_key in ('completedTime', 'stopTime', 'dueTime', 'breachedDate', 'completedDate', 'startTime', 'targetTime'):
                    t = item.get(time_key)
                    if t:
                        ts = _sla_time_display(t)
                        break
            if not ts and item.get('completedCycles'):
                cy = item['completedCycles']
                last_cycle = cy[-1] if isinstance(cy, list) and cy else (cy if isinstance(cy, dict) else None)
                first_cycle = cy[0] if isinstance(cy, list) and cy else None
                for c in (last_cycle, first_cycle):
                    if isinstance(c, dict):
                        if isinstance(c.get('friendly'), str) and c['friendly'].strip():
                            ts = c['friendly'].strip()
                            break
                        ts = _sla_time_display(c.get('stopTime') or c.get('completedTime') or c.get('elapsedTime'))
                        if ts:
                            break
            ongoing = False
            if not ts and item.get('ongoingCycle'):
                oc = item['ongoingCycle']
                if isinstance(oc, dict):
                    if isinstance(oc.get('friendly'), str) and oc['friendly'].strip():
                        ts = oc['friendly'].strip() + ' (em andamento)'
                    elif oc.get('elapsedTime') is not None:
                        ts = _sla_time_display(oc['elapsedTime']) + ' (em andamento)'
                    ongoing = True
            # Breached: ler do ciclo correto (API Jira). completedCycles = ciclos já encerrados; último ciclo define se estourou. ongoingCycle = ciclo em andamento.
            breached = False
            completed_cycles = item.get('completedCycles')
            if isinstance(completed_cycles, list) and len(completed_cycles) > 0:
                last_cycle = completed_cycles[-1]
                if isinstance(last_cycle, dict):
                    b = last_cycle.get('breached')
                    if isinstance(b, bool):
                        breached = b
            if not breached and isinstance(item.get('ongoingCycle'), dict):
                oc = item['ongoingCycle']
                ongoing = True
                b = oc.get('breached')
                if isinstance(b, bool):
                    breached = b
                elif oc.get('hasFailed') is not None:
                    breached = bool(oc.get('hasFailed'))
            if not isinstance(breached, bool):
                b = item.get('breached')
                breached = bool(b) if isinstance(b, bool) else bool(item.get('hasFailed', False))
            # Duração em segundos (para gráficos TTR/FRT a partir da coluna SLA): API (elapsedTime/duration/timeSpent) ou parsing do texto exibido na lista (ts)
            duration_seconds = None
            def _try_duration(obj, *keys):
                for k in keys:
                    v = obj.get(k) if isinstance(obj, dict) else None
                    if v is not None:
                        s = _sla_elapsed_to_seconds(v)
                        if s is not None:
                            return s
                return None
            if item.get('completedCycles') and isinstance(item['completedCycles'], list):
                last_cycle = item['completedCycles'][-1]
                if isinstance(last_cycle, dict):
                    duration_seconds = _try_duration(last_cycle, 'elapsedTime', 'duration', 'timeSpent', 'elapsedSeconds') or _sla_elapsed_to_seconds(last_cycle)
            if duration_seconds is None and item.get('ongoingCycle') and isinstance(item['ongoingCycle'], dict):
                oc = item['ongoingCycle']
                duration_seconds = _try_duration(oc, 'elapsedTime', 'duration', 'timeSpent', 'elapsedSeconds')
            if duration_seconds is None:
                duration_seconds = _try_duration(item, 'elapsedTime', 'duration', 'timeSpent', 'elapsedSeconds')
            if duration_seconds is None and item.get('completedCycles') and isinstance(item['completedCycles'], list):
                last_cycle = item['completedCycles'][-1]
                if isinstance(last_cycle, dict):
                    start_sec = _sla_time_to_epoch_seconds(last_cycle.get('startTime') or last_cycle.get('startDate'))
                    end_sec = _sla_time_to_epoch_seconds(last_cycle.get('stopTime') or last_cycle.get('completedTime') or last_cycle.get('completedDate'))
                    if start_sec is not None and end_sec is not None and end_sec >= start_sec:
                        duration_seconds = end_sec - start_sec
            if duration_seconds is None and item.get('ongoingCycle') and isinstance(item['ongoingCycle'], dict):
                oc = item['ongoingCycle']
                start_sec = _sla_time_to_epoch_seconds(oc.get('startTime') or oc.get('startDate'))
                end_sec = _sla_time_to_epoch_seconds(oc.get('stopTime') or oc.get('completedTime') or oc.get('completedDate'))
                if start_sec is not None and end_sec is not None and end_sec >= start_sec:
                    duration_seconds = end_sec - start_sec
            if duration_seconds is None and ts:
                duration_seconds = _parse_sla_timestamp_to_seconds(ts)
            # Incluir todos os SLAs retornados pelo Jira para exibição no modo lista; gráficos/pontos usam apenas os relevantes (Time to close, first response, time to resolution).
            if name or ts:
                tipo = _sla_tipo(name)
                out.append({'name': name or '—', 'timestamp': ts or '—', 'met': not breached, 'tipo': tipo, 'ongoing': ongoing, 'duration_seconds': duration_seconds})
        return out
    except Exception:
        return []


# SLAs que contam para "fora do SLA" (análises). Inclui variações em inglês e português para primeira resposta e resolução.
_SLA_NAMES_RELEVANT = (
    'time to close after resolution',
    'time to first response',
    'time to resolution',
    'first response',
    'primeira resposta',
    'time to close',
    'resolução',
    'resolution',
)


def _sla_name_is_relevant(name):
    """True apenas para os SLAs que definem se o chamado está fora do SLA (Time to close after resolution, Time to first response, Time to resolution)."""
    if not name or not isinstance(name, str):
        return False
    n = name.lower().strip()
    return any(r in n for r in _SLA_NAMES_RELEVANT)


def _sla_tipo(sla_name):
    """Classifica o SLA como FRT (First Response Time) ou TTR (Time to Resolution) pelo nome. Retorna 'FRT', 'TTR' ou None."""
    if not sla_name or not isinstance(sla_name, str):
        return None
    n = sla_name.lower().strip()
    # FRT: primeira resposta tem prioridade para não confundir com "time to close after resolution"
    if any(x in n for x in ('first response', 'primeira resposta', '1ª resposta', 'tempo para primeira', 'first time to response', 'first reply', 'tempo até primeira', 'response time', 'tempo de resposta', 'tempo de primeira')):
        return 'FRT'
    if any(x in n for x in ('resolution', 'resolução', 'resolucao', 'resolver', 'time to close', 'tempo para resolução', 'time for resolution')):
        return 'TTR'
    return None


def _format_sla_list_for_ollama(sla_list, only_relevant=True):
    """Formata lista de SLAs para o Ollama: nome, data/hora, [FRT]/[TTR], e status. Se only_relevant=True, inclui só Time to close/first response/time to resolution (para pontos fortes/melhoria)."""
    if not sla_list:
        return ''
    if only_relevant:
        sla_list = [s for s in sla_list if _sla_name_is_relevant(s.get('name'))]
    parts = []
    for s in sla_list:
        name = (s.get('name') or '—').strip()
        ts = (s.get('timestamp') or '—').strip()
        met = s.get('met', True)
        tipo = s.get('tipo')
        if tipo == 'FRT':
            status = 'Cumprido' if met else 'FRT Estourado'
            parts.append(f'[FRT] {name}: {ts} - {status}')
        elif tipo == 'TTR':
            status = 'Cumprido' if met else 'TTR Estourado'
            parts.append(f'[TTR] {name}: {ts} - {status}')
        else:
            status = 'Cumprido' if met else 'Estourado'
            parts.append(f'{name}: {ts} - {status}')
    return '; '.join(parts) if parts else ''


def fetch_issue_sla_raw(auth, issue_key):
    """
    GET /rest/servicedeskapi/request/{issueKey}/sla - retorna a resposta bruta da API (para debug).
    Retorna (status_code, data_dict ou None, error_str ou None).
    """
    if not issue_key or not issue_key.strip():
        return 0, None, 'issue_key vazio'
    try:
        r = requests.get(
            f'{JIRA_URL}/rest/servicedeskapi/request/{issue_key.strip()}/sla',
            auth=auth,
            headers={'Accept': 'application/json'},
            timeout=15,
        )
        try:
            data = r.json()
        except Exception:
            data = {'_raw_text': r.text[:2000] if r.text else ''}
        return r.status_code, data, None
    except Exception as e:
        return 0, None, str(e)


# IDs dos campos Nubank (sempre solicitados na busca para garantir que venham na resposta)
# Time to first response: usado no gráfico "Tempo médio 1ª resposta"; override via JIRA_TIME_TO_FIRST_RESPONSE_FIELD_ID no .env
NUBANK_TIME_TO_RESOLUTION_FID = 'customfield_10886'   # Nubank - Time for Resolution
NUBANK_TIME_TO_FIRST_RESPONSE_FID = os.environ.get('JIRA_TIME_TO_FIRST_RESPONSE_FIELD_ID', '').strip() or 'customfield_10884'  # Nubank - First time to Response

# Confluence: mesma base do Jira (ex.: https://nubank.atlassian.net -> https://nubank.atlassian.net/wiki). Mesmas credenciais Jira.
CONFLUENCE_URL = os.environ.get('CONFLUENCE_URL', '').strip() or (JIRA_URL.rstrip('/') + '/wiki' if JIRA_URL else '')


def fetch_confluence_for_issue(issue, auth, field_ids):
    """
    Busca no Confluence páginas relacionadas ao tema do chamado (Request Type / resumo).
    Usa apenas dados Jira + Confluence; sem fallback.
    Retorna texto das páginas encontradas (excerpt) ou string vazia se não houver Confluence URL/auth ou nenhum resultado.
    """
    if not CONFLUENCE_URL or not auth:
        return ''
    row = get_row_values(issue, field_ids or {})
    request_type = (row.get('Request Type') or '').strip() or ''
    summary, _ = get_issue_summary_and_description(issue, field_ids)
    summary = (summary or '')[:200]
    # Termos de busca: Request Type e primeiras palavras do summary (tema do chamado)
    import re
    import urllib.parse
    terms = []
    if request_type and request_type != '(sem tipo)':
        terms.append(re.sub(r'[^\w\s-]', ' ', request_type).strip()[:80])
    for w in (summary or '').split()[:5]:
        if len(w) > 2:
            terms.append(w)
    if not terms:
        return ''
    query = ' '.join(terms[:3])
    query_clean = re.sub(r'["\\]', ' ', query).strip()
    if not query_clean:
        return ''
    try:
        cql = f'type=page and text~"{query_clean}"'
        cql_encoded = urllib.parse.quote(cql, safe='')
        r = requests.get(
            f'{CONFLUENCE_URL.rstrip("/")}/rest/api/search',
            auth=auth,
            headers={'Accept': 'application/json'},
            params={'cql': cql, 'limit': 3},
            timeout=15,
        )
        if r.status_code != 200:
            return ''
        data = r.json()
        results = data.get('results') or data.get('content') or []
        if not results:
            return ''
        excerpts = []
        for item in (results[:2]):
            content_id = item.get('content', {}).get('id') if isinstance(item.get('content'), dict) else item.get('id')
            if not content_id:
                title = (item.get('content') or item).get('title') or item.get('title') or ''
                if title:
                    excerpts.append(f'[Confluence] Título: {title}')
                continue
            r2 = requests.get(
                f'{CONFLUENCE_URL.rstrip("/")}/rest/api/content/{content_id}',
                auth=auth,
                headers={'Accept': 'application/json'},
                params={'expand': 'body.view'},
                timeout=10,
            )
            if r2.status_code != 200:
                continue
            body_data = r2.json()
            title = (body_data.get('title') or '')[:200]
            body_obj = (body_data.get('body') or {}).get('view') or body_data.get('body') or {}
            body_html = body_obj.get('value') if isinstance(body_obj, dict) else ''
            body_plain = _description_to_plain_text(body_html) if body_html else ''
            if body_plain:
                body_plain = body_plain.strip()[:1500]
            if title or body_plain:
                excerpts.append(f'[Confluence] Título: {title}\n{body_plain}')
        return '\n\n---\n\n'.join(excerpts).strip()[:3000] if excerpts else ''
    except Exception:
        return ''


def _description_to_plain_text(description):
    """Extrai texto puro da descrição do Jira (ADF, HTML ou string)."""
    if description is None:
        return ''
    if isinstance(description, str):
        import re
        return re.sub(r'<[^>]+>', ' ', description).strip()[:2000]
    if isinstance(description, dict):
        # Atlassian Document Format (ADF)
        texts = []
        def walk(node):
            if isinstance(node, dict):
                if node.get('type') == 'text' and 'text' in node:
                    texts.append(node['text'])
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for item in node:
                    walk(item)
        walk(description)
        return ' '.join(texts).strip()[:2000]
    return str(description)[:2000]


def get_issue_summary_and_description(issue, field_ids=None):
    """
    Retorna (summary, description_plain) do issue.
    Tenta campos padrão 'summary' e 'description'; se field_ids tiver Summary/Description (custom), usa também.
    """
    fields = issue.get('fields') or {}
    fid = field_ids or {}
    summary_raw = (
        fields.get('summary')
        or fields.get(fid.get('Summary'))
        or fields.get(fid.get('summary'))
    )
    if isinstance(summary_raw, dict):
        summary = (summary_raw.get('value') or summary_raw.get('name') or str(summary_raw)).strip()[:2000]
    else:
        summary = (str(summary_raw or '').strip())[:2000]
    desc_raw = (
        fields.get('description')
        or fields.get(fid.get('Description'))
        or fields.get(fid.get('description'))
    )
    description_plain = _description_to_plain_text(desc_raw)
    return summary, description_plain


# Critérios de avaliação padrão para o agente Rovo (nota 1–5 nos chamados)
ROVO_DEFAULT_CRITERIA = (
    'Critérios de avaliação: (1) Urgência e impacto no negócio; '
    '(2) Clareza do título e da descrição; (3) Completude das informações; '
    '(4) Prioridade sugerida (baixa/média/alta/crítica). '
    'Responda com JSON: {"nota": N, "comentario": "frase curta em português"}, N de 1 a 5.'
)


def get_issue_note_rule_based(issue, field_ids=None):
    """
    Nota 1–5 por regras fixas, sem IA. Critérios: CSTAT, ITSM, IT Support, atendimento ao cliente.
    Considera: análise textual (completude do título/descrição), tempo de solução e tempo de 1ª resposta.
    Retorna {"nota": int, "comentario": str}.
    """
    fields = issue.get('fields', {})
    summary = (fields.get('summary') or '').strip() or ''
    description = _description_to_plain_text(fields.get('description'))
    created = _parse_iso_date(fields.get('created'))
    resolved = _parse_iso_date(fields.get('resolutiondate'))
    fid_first = (field_ids or {}).get('Time to first response') or NUBANK_TIME_TO_FIRST_RESPONSE_FID
    val_first = fields.get(fid_first)
    sec_first = _duration_to_seconds(val_first)

    # --- Pontuação textual (CSTAT / atendimento: clareza e completude) ---
    len_title = len(summary)
    len_desc = len(description)
    if len_title >= 20 and len_desc >= 200:
        pts_texto = 2.0
    elif len_title >= 10 and len_desc >= 50:
        pts_texto = 1.3
    elif len_title >= 5 or len_desc >= 20:
        pts_texto = 0.7
    else:
        pts_texto = 0.2

    # --- Tempo até 1ª resposta (ITSM / atendimento ao cliente) ---
    if sec_first is not None and sec_first >= 0:
        h_first = sec_first / 3600
        if h_first <= 1:
            pts_resposta = 1.5
        elif h_first <= 4:
            pts_resposta = 1.2
        elif h_first <= 24:
            pts_resposta = 0.8
        else:
            pts_resposta = 0.4
    else:
        pts_resposta = 0.3  # sem dado

    # --- Tempo até solução (ITSM / IT Support) ---
    if created and resolved:
        sec_resol = int((resolved - created).total_seconds())
        h_resol = sec_resol / 3600
        if h_resol <= 24:
            pts_solucao = 1.5
        elif h_resol <= 72:
            pts_solucao = 1.2
        elif h_resol <= 168:  # 1 semana
            pts_solucao = 0.8
        else:
            pts_solucao = 0.5
    else:
        pts_solucao = 0.3  # em aberto

    total = pts_texto + pts_resposta + pts_solucao
    # Mapear 0–4 para nota 1–5
    if total >= 3.5:
        nota = 5
        comentario = 'Alto: texto completo, resposta e solução rápidas.'
    elif total >= 2.8:
        nota = 4
        comentario = 'Bom: atendimento e solução dentro do esperado.'
    elif total >= 2.0:
        nota = 3
        comentario = 'Médio: algum critério (texto, resposta ou solução) pode melhorar.'
    elif total >= 1.2:
        nota = 2
        comentario = 'Abaixo: texto incompleto ou tempos de resposta/solução altos.'
    else:
        nota = 1
        comentario = 'Baixo: ticket vago ou sem dados de resposta/solução.'
    return {'nota': nota, 'comentario': comentario[:200]}


def _issue_context_for_ollama(issue, field_ids=None):
    """Monta contexto do ticket para o Ollama: Assignee, texto, resolução e tempo de 1ª resposta."""
    fields = issue.get('fields', {})
    summary = (fields.get('summary') or '').strip() or '(sem título)'
    description = _description_to_plain_text(fields.get('description'))[:2000]
    created_iso = fields.get('created')
    resolved_iso = fields.get('resolutiondate')
    time_to_resolution = ''
    if created_iso and resolved_iso:
        time_to_resolution = _format_duration(created_iso, resolved_iso) or ''
    if not time_to_resolution:
        time_to_resolution = 'Em aberto (sem data de resolução)'
    fid_first = (field_ids or {}).get('Time to first response') or NUBANK_TIME_TO_FIRST_RESPONSE_FID
    val_first = fields.get(fid_first)
    sec_first = _duration_to_seconds(val_first)
    time_to_first_response = _format_seconds_hhmm(sec_first) if sec_first is not None and sec_first >= 0 else 'N/A'
    row = get_row_values(issue, field_ids or {})
    assignee = row.get('Assignee', '—')
    satisfaction = row.get('Satisfaction', '—')
    return (
        f'Assignee: {assignee}\n'
        f'Título: {summary}\n'
        f'Descrição: {description}\n'
        f'Time to resolution (TTR - modo lista): {time_to_resolution}\n'
        f'Time to first response (FRT - modo lista): {time_to_first_response}\n'
        f'Satisfaction (CSAT 1 a 5 - modo lista): {satisfaction}'
    )


def _issue_context_full_for_pontos(issue, field_ids, comments_text=None):
    """Contexto completo do ticket para análise de pontos: todas as colunas do modo lista + resposta do Assignee."""
    row = get_row_values(issue, field_ids or {})
    key = row.get('key', '') or (issue.get('key') or '')
    summary, desc_plain = get_issue_summary_and_description(issue, field_ids)
    summary = (summary or '').strip() or '(sem título)'
    desc_plain = (desc_plain or '')[:1500]
    fields = issue.get('fields', {})
    status_obj = fields.get('status') or {}
    status = status_obj.get('name', '') if isinstance(status_obj, dict) else str(status_obj or '')
    created = fields.get('created') or '—'
    updated = fields.get('updated') or '—'
    parts = [
        f'Chamado: {key}',
        f'Título: {summary}',
        f'Descrição: {desc_plain}',
        f'Reporter: {row.get("Reporter", "—")}',
        f'Status: {status}',
        f'Assignee: {row.get("Assignee", "—")}',
        f'Request Type: {row.get("Request Type", "—")}',
        f'Time to resolution: {row.get("Time to resolution", "—")}',
        f'Time to first response: {row.get("Time to first response", "—")}',
        f'Created: {created}',
        f'Updated: {updated}',
        f'Satisfaction: {row.get("Satisfaction", "—")}',
    ]
    if comments_text and comments_text.strip():
        parts.append('Resposta do Assignee (comentários no ticket):')
        parts.append(comments_text.strip()[:1800])
    return '\n'.join(parts)


def _parse_ollama_nota_response(content):
    """Extrai nota 1-5 e comentário resumido do texto. Retorna dict ou None. Comentário limitado a 60 chars."""
    import json
    import re
    if not content or not isinstance(content, str):
        return None
    content = content.strip()
    # Formato "N - resumo" ou "N. resumo" ou só "N" (menos processamento)
    m = re.match(r'^\s*([1-5])\s*[-.:]\s*(.+)$', content, re.DOTALL)
    if m:
        n = int(m.group(1))
        com = (m.group(2).strip() or '')[:60]
        return {'nota': n, 'comentario': com or 'OK'}
    m = re.match(r'^\s*([1-5])\s*$', content)
    if m:
        return {'nota': int(m.group(1)), 'comentario': 'OK'}
    # Se o modelo colocou "N - resumo" em qualquer linha (ex.: após texto introdutório)
    for line in content.split('\n'):
        line = line.strip()
        m = re.match(r'^\s*([1-5])\s*[-.:]\s*(.+)$', line, re.DOTALL)
        if m:
            n = int(m.group(1))
            com = (m.group(2).strip() or '')[:60]
            return {'nota': n, 'comentario': com or 'OK'}
        m = re.match(r'^\s*([1-5])\s*$', line)
        if m:
            return {'nota': int(m.group(1)), 'comentario': 'OK'}
    # Remove markdown code block
    if '```' in content:
        parts = content.split('```')
        for p in parts:
            p = p.strip()
            if p.lower().startswith('json'):
                p = p[4:].strip()
            if p.startswith('{'):
                content = p
                break
    # Extrai primeiro {...} balanceado
    start = content.find('{')
    if start >= 0:
        depth = 0
        in_string = None
        escape = False
        i = start
        while i < len(content):
            c = content[i]
            if escape:
                escape = False
                i += 1
                continue
            if c == '\\' and in_string:
                escape = True
                i += 1
                continue
            if in_string:
                if c == in_string:
                    in_string = None
                i += 1
                continue
            if c in ('"', "'"):
                in_string = c
                i += 1
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    content = content[start:i + 1]
                    break
            i += 1
    try:
        data = json.loads(content)
        nota = data.get('nota')
        if nota is not None:
            try:
                nota = max(1, min(5, int(nota)))
            except (TypeError, ValueError):
                nota = None
        if nota is None or nota < 1 or nota > 5:
            nota = None  # não considerar 0 ou inválido como avaliado
        com = (data.get('comentario') or '')[:60]
        return {'nota': nota if nota is not None else 0, 'comentario': com or 'OK'}
    except (json.JSONDecodeError, TypeError):
        pass
    # Fallback: procurar "nota": N no texto (aceita string ou número)
    m = re.search(r'"nota"\s*:\s*["\']?(\d+)["\']?', content, re.I)
    if m:
        nota = max(1, min(5, int(m.group(1))))
        com = ''
        mc = re.search(r'"comentario"\s*:\s*"((?:[^"\\]|\\.)*)"', content, re.I)
        if mc:
            com = (mc.group(1).replace('\\"', '"') or '')[:60]
        if 1 <= nota <= 5:
            return {'nota': nota, 'comentario': (com or 'OK')[:60]}
    # Último recurso: qualquer menção a nota 1–5
    for pattern in (r'nota\s*[=:]\s*["\']?([1-5])["\']?', r'["\']nota["\']\s*:\s*([1-5])\b', r'\b([1-5])\s*/\s*5', r'\bnota\s+([1-5])\b'):
        m = re.search(pattern, content, re.I)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 5:
                return {'nota': n, 'comentario': 'OK'}
    # Qualquer dígito 1–5 na resposta (garantir que nenhum chamado fique sem nota)
    for chunk in (content[-300:], content):
        m = re.search(r'\b([1-5])\b', chunk)
        if m:
            return {'nota': int(m.group(1)), 'comentario': 'OK'}
    return None


def get_issue_note_from_ollama(issue, ollama_url, model=None, field_ids=None, comments_text=None, auth=None):
    """
    Usa Ollama (modelo local) para dar nota de 1 a 5 ao chamado. Em falha retorna nota None.
    Se auth for passado, inclui no contexto os SLAs do Jira (seção SLAs: nome + cumprido/estourado).

    Por que o Ollama pode falhar em avaliar um ticket:
    - Timeout/conexão: Ollama lento ou inacessível (5 tentativas por endpoint, timeout 120s).
    - Resposta vazia: modelo não retornou texto (response/message/content vazio).
    - Resposta sem 1–5: modelo devolveu texto que o parser não extrai (tentamos várias linhas e regex).
    - Modelo não encontrado: 200 com {"error": "model not found"} (tentamos próximo modelo).
    - /api 404: só /v1/chat/completions existe (já tentamos os três endpoints).
    O backend reavalia em loop até todos terem nota; não há fallback.
    """
    def _fail(comentario):
        return {'nota': None, 'comentario': comentario}

    if not ollama_url or not ollama_url.strip():
        return _fail('Não avaliado (OLLAMA_URL não configurado)')
    base = ollama_url.strip().rstrip('/')
    model = (model or 'llama3.2').strip()
    # Verificação rápida e lista de modelos instalados (tentar primeiro os que existem)
    models_installed = []
    try:
        r = requests.get(f'{base}/api/tags', timeout=5)
        if r.status_code == 404:
            return _fail('Não avaliado (Ollama: URL não é o daemon Ollama — verifique OLLAMA_URL, ex: http://127.0.0.1:11434)')
        if r.status_code == 200:
            data = r.json()
            for item in (data.get('models') or []):
                name = (item.get('name') or item.get('model') or '').strip()
                if name and name not in models_installed:
                    models_installed.append(name)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return _fail('Não avaliado (Ollama indisponível ou timeout)')
    except Exception:
        pass
    context = _issue_context_for_ollama(issue, field_ids)
    if comments_text and comments_text.strip():
        context += '\n\nComments (respostas do Assignee):\n' + (comments_text.strip()[:2500])
    if auth and issue.get('key'):
        sla_list = fetch_issue_sla(auth, issue.get('key'))
        sla_text = _format_sla_list_for_ollama(sla_list)
        if sla_text:
            context += '\n\nSLAs (Jira): [FRT]=First Response Time, [TTR]=Time to Resolution. O status (Cumprido/Estourado) é o do Jira para ESTE ticket — não use valor fixo, respeite apenas o que consta abaixo por análise individual.\n' + sla_text
    context = context[:3500]

    # Cenário 2: SLA CRÍTICO. Foco no atendimento e nas ações do Assignee; evitar linguagem muito técnica.
    prompt = (
        'Você é um QA que avalia o atendimento prestado pelo analista (Assignee). '
        'Reporter = quem foi atendido; Assignee = quem atende. O resumo deve descrever as AÇÕES do Assignee (verbo no ativo), não do Reporter.\n\n'
        'LÍNGUA E GRAMÁTICA: Responda sempre em português do Brasil (pt-BR), com gramática e conjugação verbal corretas, de forma natural e clara. '
        'O Assignee é quem EXECUTA a ação. Use sempre: "X atendeu", "X prestou atendimento", "X respondeu", "X resolveu". '
        'NUNCA escreva "X atendido" (errado: significaria que X foi atendido). NUNCA "atendido o assignee" ou "atended" (inglês). '
        'Exemplos corretos: "Vini Reis atendeu bem ao usuário Aaron Thornton"; "Jilcimar atendeu o chamado"; "Gabriel Silva prestou atendimento ao Nubank Team". '
        'Concordância, acentuação e pontuação em pt-BR.\n\n'
        'Foque no que o Assignee fez: respondeu no prazo? Foi claro? Resolveu? Marcou o Reporter (@Reporter)? '
        'Use os dados: Satisfaction (1-5), SLA (Cumprido/Estourado), Comments do Assignee.\n\n'
        'Resposta em UMA LINHA: N - resumo em até 10 palavras. Ex: 4 - Atendeu bem, solução clara, marcou o Reporter.\n\n'
        'Dados do chamado (Assignee, Comments, TTR, FRT, Satisfaction):\n' + context
    )
    max_retries = 4  # 5 tentativas por endpoint para obter nota 1–5 (sem fallback)
    fallback_models = [model, 'llama3.2', 'llama3.1', 'llama3', 'qwen2.5:0.5b', 'qwen2.5', 'mistral', 'gemma2:2b']
    seen = set()
    models_to_try = []
    for name in models_installed + fallback_models:
        n = (name or '').strip()
        if n and n not in seen:
            seen.add(n)
            models_to_try.append(n)
    got_404 = False
    for m in models_to_try:
        if not m:
            continue
        m = m.strip()
        # 1) /api/generate (mais estável em muitas instalações)
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(
                    f'{base}/api/generate',
                    json={'model': m, 'prompt': prompt, 'stream': False},
                    timeout=120,
                )
                if r.status_code == 404:
                    got_404 = True
                    break
                if r.status_code == 200:
                    data = r.json()
                    content = (data.get('response') or '').strip()
                    if content:
                        result = _parse_ollama_nota_response(content)
                        if result and result.get('nota') and 1 <= result.get('nota') <= 5:
                            return result
                if attempt < max_retries:
                    time.sleep(1.5)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt >= max_retries:
                    return _fail('Não avaliado (Ollama indisponível ou timeout)')
                time.sleep(1.5)
            except Exception:
                if attempt < max_retries:
                    time.sleep(1.5)
                break
        # 2) /api/chat
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(
                    f'{base}/api/chat',
                    json={
                        'model': m,
                        'messages': [{'role': 'user', 'content': prompt}],
                        'stream': False,
                        'options': {'temperature': 0},
                    },
                    timeout=120,
                )
                if r.status_code == 404:
                    got_404 = True
                    break
                if r.status_code == 200:
                    data = r.json()
                    msg = data.get('message') or {}
                    content = (msg.get('content') or '').strip()
                    if content:
                        result = _parse_ollama_nota_response(content)
                        if result and result.get('nota') and 1 <= result.get('nota') <= 5:
                            return result
                if attempt < max_retries:
                    time.sleep(1.5)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt >= max_retries:
                    return _fail('Não avaliado (Ollama indisponível ou timeout)')
                time.sleep(1.5)
            except Exception:
                if attempt < max_retries:
                    time.sleep(1.5)
                break
        # 3) /v1/chat/completions (API compatível com OpenAI; alguns proxies/Ollama só expõem isso)
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(
                    f'{base}/v1/chat/completions',
                    json={
                        'model': m,
                        'messages': [{'role': 'user', 'content': prompt}],
                        'stream': False,
                        'temperature': 0,
                    },
                    timeout=120,
                )
                if r.status_code == 404:
                    got_404 = True
                    break
                if r.status_code == 200:
                    data = r.json()
                    if data.get('error'):
                        if attempt < max_retries:
                            time.sleep(1.5)
                        break
                    choices = data.get('choices') or []
                    if choices:
                        content = (choices[0].get('message') or {}).get('content') or ''
                        content = content.strip()
                        if content:
                            result = _parse_ollama_nota_response(content)
                            if result and result.get('nota') and 1 <= result.get('nota') <= 5:
                                return result
                if attempt < max_retries:
                    time.sleep(1.5)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt >= max_retries:
                    return _fail('Não avaliado (Ollama indisponível ou timeout)')
                time.sleep(1.5)
            except Exception:
                if attempt < max_retries:
                    time.sleep(1.5)
                break
        # Não interromper o loop por 404: /v1/chat/completions pode existir mesmo quando /api/generate e /api/chat retornam 404; tentar todos os modelos.
    if got_404:
        return _fail('Não avaliado (Ollama: /api/generate, /api/chat e /v1/chat/completions não encontrados — verifique OLLAMA_URL, ex: http://127.0.0.1:11434)')
    return _fail('Não avaliado (Ollama sem modelo compatível ou resposta inválida)')


def _is_nonsense_or_metadata_ponto(text):
    """Retorna True se o texto for chave Jira, metadata (assignee/reporter + [at]/[resolved]/[acao]/[argumento]) ou placeholder sem sentido."""
    import re
    if not text or not isinstance(text, str):
        return True
    t = text.strip()
    t_lower = t.lower()
    if re.match(r'^[A-Za-z]{2,}-\d+$', t.strip()):
        return True
    if '(assignee)' in t_lower or '(reporter)' in t_lower:
        if '[at]' in t_lower or '[resolved]' in t_lower or '[acao]' in t_lower or '[argumento]' in t_lower:
            return True
        if re.search(r'\s*\(\d+\)\s*$', t):
            return True
    if '[acao]' in t_lower and '[argumento]' in t_lower:
        return True
    if '[at]' in t_lower and 'resolved' in t_lower and ('assignee' in t_lower or 'reporter' in t_lower):
        return True
    if re.match(r'^[\w\s]+\((assignee|reporter)\)\s*\[[\w.]+\]', t_lower):
        return True
    return False


def _is_sensible_ponto(text):
    """Retorna False se o texto for genérico/placeholder sem valor analítico (n/a, nenhuma, melhoria:, etc.)."""
    if not text or not isinstance(text, str):
        return False
    if _is_nonsense_or_metadata_ponto(text):
        return False
    import unicodedata
    t = text.strip().lower()
    if len(t) < 8:
        return False
    t_norm = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
    blocklist = {
        'n/a', 'na', 'nenhuma', 'nenhum', 'nenhuma.', 'nenhum.',
        'nao mencionado', 'não mencionado', 'not mentioned', 'none', 'n.a.', 'n.a',
        'melhoria', 'melhoria:', 'forte', 'forte:',
        'sem melhoria', 'sem forte', 'nao aplicavel', 'não aplicável', 'n/a.',
        'nao especificado', 'não especificado', 'nao informado', 'não informado',
        'no', 'nao', 'não', 'nada', 'nada.', 'nao ha', 'não há', 'sem conteudo',
        'no specific', 'not specified', 'not applicable',
        'no especificamente', 'não especificamente', 'nao especificamente',
        '### melhoria', '### forte', '# melhoria', '# forte', 'melhoria:', 'forte:',
    }
    t_norm = t_norm.lstrip('#').strip()
    if t_norm in blocklist:
        return False
    for b in blocklist:
        if len(b) > 4 and t_norm == b:
            return False
        if t_norm.startswith(b + ' ') or t_norm == b or t_norm.rstrip('.') == b:
            return False
    if t_norm in ('melhoria:', 'forte:', 'melhoria', 'forte'):
        return False
    return True


def _is_rule_or_wrong_category(text, for_melhoria=True):
    """Rejeita texto que é cabeçalho de regra ou categoria trocada (ex.: 'Regra: Pontos de Melhoria = 5 pontos Positivos')."""
    if not text or not isinstance(text, str):
        return True
    t = text.strip().lower()
    if 'regra:' in t or 'regra :' in t or 'pontos de melhoria =' in t or 'pontos fortes =' in t:
        return True
    if for_melhoria and ('regras seguidas' in t or 'pontos positivos' in t or '5 pontos positivos' in t):
        return True
    if not for_melhoria and ('regras não seguidas' in t or 'regras nao seguidas' in t or 'pontos negativos' in t):
        return True
    return False


def _looks_like_melhoria(text):
    """True se o texto parece ponto negativo / regra não seguida (não deve ir em pontos fortes)."""
    if not text or not isinstance(text, str):
        return False
    t = text.strip().lower()
    t = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
    keywords = (
        'estourado', 'estourou', 'fora do sla', 'extrapolou', 'atraso', 'atrasou',
        'nao seguida', 'não seguida', 'regra nao', 'regra não', 'violou', 'violacao',
        'baixo', 'insatisfatorio', 'insatisfatório', 'incompleto', 'incompleta',
        'inadequad', 'vago', 'vagos', 'inadequado', 'negativo', 'negativos',
        'pontos de melhoria', 'melhoria:', 'frt estourado', 'ttr estourado',
        'satisfaction 1', 'satisfaction 2', 'satisfaction baixo', 'sem @reporter',
        'nao alcanca', 'não alcança', 'satisfacao nao alcanca', 'satisfação não alcança',
    )
    return any(k in t for k in keywords)


def _looks_like_forte(text):
    """True se o texto parece ponto positivo / regra seguida (não deve ir em pontos de melhoria)."""
    if not text or not isinstance(text, str):
        return False
    t = text.strip().lower()
    t = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
    keywords = (
        'dentro do sla', 'cumprido', 'cumprida', 'no prazo', 'no tempo',
        'alto', 'satisfaction alto', 'satisfaction 4', 'satisfaction 5',
        'clara', 'claro', 'resolutivo', 'resolutiva', 'marcou @', 'corporativa',
        'educado', 'regras seguidas', 'forte:', 'pontos fortes',
    )
    return any(k in t for k in keywords)


def _normalize_for_contradiction(text):
    """Normaliza texto para checagem de contradição (minúsculo, sem acentos)."""
    if not text or not isinstance(text, str):
        return ''
    t = text.strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')


def _remove_contradictions(melhorias, fortes):
    """
    Remove itens contraditórios: mesma métrica não pode estar nos dois lados.
    Ex.: se melhoria tem "TTR estourado", remove de fortes "TTR/FRT dentro do SLA"; se fortes tem "dentro do SLA", remove de melhoria "estourado".
    """
    if not melhorias and not fortes:
        return list(melhorias), list(fortes)
    mel = list(melhorias)
    fort = list(fortes)

    # SLA: remover de fortes apenas o SLA que está estourado em melhoria. Ex.: se só FRT estourou, manter "TTR dentro do SLA" em fortes; nunca "TTR/FRT dentro do SLA" numa linha.
    nf = _normalize_for_contradiction
    mel_has_frt_estourado = any(
        'frt estourado' in nf(m) or ('first response' in nf(m) and ('estourado' in nf(m) or 'fora' in nf(m)))
        or 'time to first response' in nf(m) and 'estourado' in nf(m)
        for m in mel
    )
    mel_has_ttr_estourado = any(
        'ttr estourado' in nf(m) or ('time to resolution' in nf(m) or 'time to close' in nf(m)) and 'estourado' in nf(m)
        or 'resolution' in nf(m) and 'estourado' in nf(m)
        for m in mel
    )
    def _forte_contradiz_sla(f):
        n = nf(f)
        if 'fora do sla' in n or 'fora sla' in n:
            return True
        if 'ttr' in n and 'frt' in n:
            return True
        if mel_has_frt_estourado and ('frt' in n or 'first response' in n) and ('dentro' in n or 'cumprido' in n or 'sla' in n):
            return True
        if mel_has_ttr_estourado and ('ttr' in n or 'resolution' in n or 'time to close' in n) and ('dentro' in n or 'cumprido' in n or 'sla' in n):
            return True
        return False
    fort = [f for f in fort if not _forte_contradiz_sla(f)]

    # SLA: se em fortes há "FRT dentro do SLA", remover de melhoria "FRT estourado"; se fortes há "TTR dentro do SLA", remover de melhoria "TTR estourado"
    fort_has_frt_dentro = any(
        ('frt' in nf(f) or 'first response' in nf(f)) and ('dentro' in nf(f) or 'cumprido' in nf(f) or 'sla' in nf(f))
        for f in fort
    )
    fort_has_ttr_dentro = any(
        ('ttr' in nf(f) or 'resolution' in nf(f) or 'time to close' in nf(f)) and ('dentro' in nf(f) or 'cumprido' in nf(f) or 'sla' in nf(f))
        for f in fort
    )
    if fort_has_frt_dentro:
        mel = [m for m in mel if not ('frt estourado' in nf(m) or ('first response' in nf(m) and 'estourado' in nf(m)))]
    if fort_has_ttr_dentro:
        mel = [m for m in mel if not ('ttr estourado' in nf(m) or ('time to resolution' in nf(m) or 'time to close' in nf(m)) and 'estourado' in nf(m))]

    # CSAT: se melhoria tem CSAT baixo (Satisfaction 1-2), remover de fortes CSAT alto / entre 4 e 6
    mel_has_csat_baixo = any(
        'csat baix' in _normalize_for_contradiction(m) or 'satisfaction 1' in _normalize_for_contradiction(m) or 'satisfaction 2' in _normalize_for_contradiction(m)
        for m in mel
    )
    if mel_has_csat_baixo:
        fort = [f for f in fort if not (
            'csat' in _normalize_for_contradiction(f) and ('entre 4' in _normalize_for_contradiction(f) or 'entre 5' in _normalize_for_contradiction(f) or 'alto' in _normalize_for_contradiction(f) or 'satisfaction 4' in _normalize_for_contradiction(f) or 'satisfaction 5' in _normalize_for_contradiction(f))
        )]

    # CSAT: se fortes tem CSAT alto, remover de melhoria CSAT baixo
    fort_has_csat_alto = any(
        'csat' in _normalize_for_contradiction(f) and ('entre 4' in _normalize_for_contradiction(f) or 'entre 5' in _normalize_for_contradiction(f) or 'entre 6' in _normalize_for_contradiction(f) or 'alto' in _normalize_for_contradiction(f) or 'satisfaction 4' in _normalize_for_contradiction(f) or 'satisfaction 5' in _normalize_for_contradiction(f))
        for f in fort
    )
    if fort_has_csat_alto:
        mel = [m for m in mel if not (
            'csat baix' in _normalize_for_contradiction(m) or 'satisfaction 1' in _normalize_for_contradiction(m) or 'satisfaction 2' in _normalize_for_contradiction(m)
        )]

    # Tags/categorias: se melhoria tem uso de tags negativo, remover de fortes "uso adequado das tags"
    mel_has_tags_neg = any(
        'tags' in _normalize_for_contradiction(m) or 'categorias' in _normalize_for_contradiction(m)
        for m in mel
    )
    if mel_has_tags_neg:
        fort = [f for f in fort if not (
            'uso adequado' in _normalize_for_contradiction(f) and ('tags' in _normalize_for_contradiction(f) or 'categorias' in _normalize_for_contradiction(f))
        )]

    # TAPI: se melhoria menciona TAPI (tolerância), remover de fortes "TAPI está entre" (evitar mesmo indicador nos dois lados)
    mel_has_tapi = any('tapi' in _normalize_for_contradiction(m) or 'tolerancia' in _normalize_for_contradiction(m) or 'tolerância' in _normalize_for_contradiction(m) for m in mel)
    if mel_has_tapi:
        fort = [f for f in fort if not ('tapi' in _normalize_for_contradiction(f) and ('entre' in _normalize_for_contradiction(f) or 'esta entre' in _normalize_for_contradiction(f)))]

    return mel, fort


def _is_intro_line(text):
    """True se a linha for só introdutória (ex.: 'O analista X poderia ter feito... para o Reporter Y:') sem conteúdo de ponto."""
    import re
    if not text or len(text) < 20:
        return False
    t = text.strip().lower()
    if re.match(r'^melhoria\s*[:\-]', t) or re.match(r'^forte\s*[:\-]', t):
        return False
    if ('poderia ter feito' in t or 'poderia ter' in t) and ('reporter' in t or 'seguinte' in t):
        return True
    if t.endswith(':') and ('analista' in t or 'assignee' in t) and ('reporter' in t or 'atendido' in t):
        return True
    return False


def _parse_pontos_ollama_response(content):
    """Extrai listas de pontos de melhoria e fortes do texto. Filtra itens genéricos e linhas de regra. Retorna {'melhorias': [...], 'fortes': [...]}."""
    import re
    if not content or not isinstance(content, str):
        return {'melhorias': [], 'fortes': []}
    content = content.strip()
    melhorias = []
    fortes = []
    for line in content.split('\n'):
        line = line.strip()
        line = re.sub(r'^#+\s*', '', line).strip()
        if not line:
            continue
        if _is_intro_line(line):
            continue
        if re.match(r'^\s*melhoria\s*[:\-]', line, re.I):
            m = re.sub(r'^\s*melhoria\s*[:\-]\s*', '', line, flags=re.I).strip()
            if m and len(m) > 2 and _is_sensible_ponto(m) and not _is_rule_or_wrong_category(m, for_melhoria=True) and not _looks_like_forte(m):
                melhorias.append(m[:450])
        elif re.match(r'^\s*forte\s*[:\-]', line, re.I):
            m = re.sub(r'^\s*forte\s*[:\-]\s*', '', line, flags=re.I).strip()
            if m and len(m) > 2 and _is_sensible_ponto(m) and not _is_rule_or_wrong_category(m, for_melhoria=False) and not _looks_like_melhoria(m):
                fortes.append(m[:450])
        elif re.match(r'^\s*[\-\*•]\s+', line) or re.match(r'^\s*\d+[.)]\s+', line):
            bullet = re.sub(r'^\s*[\-\*•]\s+', '', line)
            bullet = re.sub(r'^\s*\d+[.)]\s+', '', bullet).strip()
            if bullet and len(bullet) > 15 and _is_sensible_ponto(bullet):
                if not _looks_like_forte(bullet) and not _is_rule_or_wrong_category(bullet, for_melhoria=True):
                    melhorias.append(bullet[:450])
                elif not _looks_like_melhoria(bullet) and not _is_rule_or_wrong_category(bullet, for_melhoria=False):
                    fortes.append(bullet[:450])
    if not melhorias and not fortes:
        for part in re.split(r'[;\n]', content):
            part = part.strip()
            if _is_intro_line(part):
                continue
            if 10 <= len(part) < 300 and _is_sensible_ponto(part) and not _is_rule_or_wrong_category(part, for_melhoria=True) and not _looks_like_forte(part):
                melhorias.append(part[:450])
    # Garantir: nenhum item de regra; nenhum nonsense/metadata; nenhum ponto de melhoria em fortes; nenhum ponto forte em melhorias
    melhorias = [x for x in melhorias[:5] if not _is_nonsense_or_metadata_ponto(x) and not _is_rule_or_wrong_category(x, for_melhoria=True) and not _looks_like_forte(x)]
    fortes = [x for x in fortes[:5] if not _is_nonsense_or_metadata_ponto(x) and not _is_rule_or_wrong_category(x, for_melhoria=False) and not _looks_like_melhoria(x)]
    # Nunca permitir em fortes: "estourado", "não alcança", ou linha que junta TTR e FRT ("TTR/FRT dentro do SLA")
    nf = _normalize_for_contradiction
    fortes = [f for f in fortes if 'estourado' not in nf(f) and 'nao alcanca' not in nf(f) and 'não alcança' not in nf(f)]
    fortes = [f for f in fortes if not ('ttr' in nf(f) and 'frt' in nf(f))]  # nunca "TTR/FRT" numa única linha
    # Remover contradições: mesma métrica não pode aparecer nos dois lados (ex.: SLA estourado em melhoria e "dentro do SLA" em fortes)
    melhorias, fortes = _remove_contradictions(melhorias, fortes)
    return {'melhorias': melhorias[:5], 'fortes': fortes[:5]}


def get_issue_pontos_ollama(issue, ollama_url, field_ids=None, model=None, comments_text=None, auth=None, sla_by_key=None, mode=None, confluence_text=None):
    """
    Chama Ollama para extrair pontos de melhoria e/ou pontos fortes do atendimento.
    mode='melhoria': só pontos de melhoria (boas práticas para melhorar resultados negativos).
    mode='fortes': só pontos fortes (o que foi feito de bom).
    mode=None: ambos (comportamento anterior).
    confluence_text: quando mode='melhoria', texto do Confluence sobre o tema do chamado; usado para sugerir o que o analista poderia ter feito.
    SLA: usa APENAS a coluna SLA do modo lista (sla_by_key). Sem fallback para fetch no Jira.
    Retorna {'melhorias': [...], 'fortes': [...]} ou {'melhorias': [], 'fortes': []} em falha.
    """
    if not ollama_url or not ollama_url.strip():
        return {'melhorias': [], 'fortes': []}
    base = ollama_url.strip().rstrip('/')
    context = _issue_context_full_for_pontos(issue, field_ids or {}, comments_text)
    key = issue.get('key')
    if key and sla_by_key is not None:
        sla_list = sla_by_key.get(key)
        if sla_list:
            sla_text = _format_sla_list_for_ollama(sla_list)
            if sla_text:
                context += '\n\nSLAs (Jira): [FRT]=First Response Time, [TTR]=Time to Resolution. Status (Cumprido/Estourado) é do Jira para ESTE ticket.\n' + sla_text
    if confluence_text and confluence_text.strip():
        context += '\n\nConteúdo do Confluence sobre o tema deste chamado:\n' + confluence_text.strip()[:1200]
    context = context[:3800]
    regra_reporter_assignee = (
        'Regra: Reporter = quem foi atendido; Assignee = analista que atende. '
        'Ao falar do Assignee, use verbo no ativo: "atendeu", "prestou atendimento", "respondeu". NUNCA "X atendido" (errado) nem "atended" (inglês). '
        'Refira-se ao Reporter como quem recebeu o atendimento.\n\n'
    )
    pt_grammar = (
        'Responda sempre em português do Brasil (pt-BR), com gramática e conjugação verbal corretas, de forma natural e clara. '
        'Concordância, acentuação e pontuação em pt-BR. Assignee = sujeito da ação: use "atendeu", "prestou atendimento"; NUNCA "atendido" para o analista nem "atended" (inglês). '
    )
    if mode == 'melhoria':
        prompt = (
            'Responda em português brasileiro. Use APENAS os dados abaixo (não invente). Analise a base de conhecimento (Jira + Confluence) e só então redija os pontos.\n\n'
            + regra_reporter_assignee +
            'Foque no ATENDIMENTO e nas AÇÕES do Assignee (analista), não no Reporter. Evite linguagem muito técnica; use termos simples: resposta, comunicação, solução, prazo, clareza. '
            + pt_grammar + '\n\n'
            'Chamado com Satisfaction baixo (1-3) e SLA vencido. Redija até 5 PONTOS DE MELHORIA concretos: o que o Assignee poderia ter feito de diferente no atendimento para feedback positivo do Reporter.\n\n'
            'OBRIGATÓRIO: não escreva frase introdutória. Saída APENAS linhas no formato "Melhoria: <frase concreta>." uma por linha. Frases completas, sem cortar no meio. Baseie-se nos comentários e ações do Assignee. Proibido comentário vago ou genérico.\n\n'
            'Base de conhecimento:\n' + context
        )
    elif mode == 'fortes':
        prompt = (
            'Responda em português brasileiro. Use APENAS os dados abaixo (não invente). Analise a base de conhecimento e só então redija os pontos.\n\n'
            + regra_reporter_assignee +
            'Foque no ATENDIMENTO e nas AÇÕES do Assignee (analista), não no Reporter. Evite linguagem muito técnica; use termos simples: resposta, comunicação, solução, prazo, clareza. '
            + pt_grammar + '\n\n'
            'Chamado com Satisfaction=5 e dentro do SLA. Redija até 5 pontos positivos concretos do que o Assignee fez bem no atendimento.\n\n'
            'OBRIGATÓRIO: não escreva frase introdutória. Saída APENAS linhas no formato "Forte: <frase concreta>." uma por linha. Frases completas, sem cortar no meio. Proibido comentário vago ou genérico.\n\n'
            'Base de conhecimento:\n' + context
        )
    else:
        prompt = (
            'Responda em português brasileiro. Use APENAS os dados abaixo (não invente). Analise a base e só então redija as listas.\n\n'
            + regra_reporter_assignee +
            'Foque no ATENDIMENTO e nas AÇÕES do Assignee (analista), não no Reporter. Evite linguagem muito técnica; use termos simples sobre atendimento. '
            + pt_grammar + '\n\n'
            'Saída APENAS linhas no formato "Melhoria: <frase>." ou "Forte: <frase>." até 5 de cada. Frases completas, sem cortar no meio. Não escreva frase introdutória. Cada ponto concreto e específico (proibido vago ou genérico).\n\n'
            'Base de conhecimento:\n' + context
        )

    def parse_content(content):
        if content:
            out = _parse_pontos_ollama_response(content)
            if out.get('melhorias') or out.get('fortes'):
                return out
        return None

    def ensure_pontos_never_empty(out, mode):
        """Retorna o resultado sem preencher com texto genérico (sem fallback)."""
        if not out:
            out = {'melhorias': [], 'fortes': []}
        return out

    # Mesma lógica de get_issue_note_from_ollama: modelos, ordem dos endpoints (generate → chat → v1); timeout maior para pontos (contexto maior)
    default_model = (model or 'llama3.2').strip()
    models_installed = []
    try:
        r = requests.get(f'{base}/api/tags', timeout=5)
        if r.status_code == 200:
            data = r.json()
            for item in (data.get('models') or []):
                name = (item.get('name') or item.get('model') or '').strip()
                if name and name not in models_installed:
                    models_installed.append(name)
    except Exception:
        pass
    fallback_models = [default_model, 'llama3.2', 'llama3.1', 'llama3', 'qwen2.5:0.5b', 'qwen2.5', 'mistral', 'gemma2:2b']
    seen = set()
    models_to_try = []
    for name in models_installed + fallback_models:
        n = (name or '').strip()
        if n and n not in seen:
            seen.add(n)
            models_to_try.append(n)

    max_retries = 4
    req_timeout = 180
    for m in models_to_try:
        if not m:
            continue
        m = m.strip()
        # 1) /api/generate (mesma ordem que notas); options.num_predict limita saída para evitar timeout 500
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(
                    f'{base}/api/generate',
                    json={
                        'model': m,
                        'prompt': prompt,
                        'stream': False,
                        'options': {'temperature': 0, 'num_predict': 1024},
                    },
                    timeout=req_timeout,
                )
                if r.status_code == 404:
                    break
                if r.status_code == 200:
                    data = r.json()
                    content = (data.get('response') or '').strip()
                    result = parse_content(content)
                    if result:
                        return ensure_pontos_never_empty(result, mode)
                if attempt < max_retries:
                    time.sleep(1.5)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt < max_retries:
                    time.sleep(1.5)
                break
            except Exception:
                if attempt < max_retries:
                    time.sleep(1.5)
                break
        # 2) /api/chat; num_predict limita saída para evitar timeout 500
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(
                    f'{base}/api/chat',
                    json={
                        'model': m,
                        'messages': [{'role': 'user', 'content': prompt}],
                        'stream': False,
                        'options': {'temperature': 0, 'num_predict': 1024},
                    },
                    timeout=req_timeout,
                )
                if r.status_code == 404:
                    break
                if r.status_code == 200:
                    data = r.json()
                    msg = data.get('message') or {}
                    content = (msg.get('content') or '').strip()
                    result = parse_content(content)
                    if result:
                        return ensure_pontos_never_empty(result, mode)
                if attempt < max_retries:
                    time.sleep(1.5)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt < max_retries:
                    time.sleep(1.5)
                break
            except Exception:
                if attempt < max_retries:
                    time.sleep(1.5)
                break
        # 3) /v1/chat/completions
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(
                    f'{base}/v1/chat/completions',
                    json={
                        'model': m,
                        'messages': [{'role': 'user', 'content': prompt}],
                        'stream': False,
                        'temperature': 0,
                    },
                    timeout=req_timeout,
                )
                if r.status_code == 404:
                    break
                if r.status_code == 200:
                    data = r.json()
                    if data.get('error'):
                        if attempt < max_retries:
                            time.sleep(1.5)
                        break
                    choices = data.get('choices') or []
                    if choices:
                        content = (choices[0].get('message') or {}).get('content') or ''
                        content = content.strip()
                        result = parse_content(content)
                        if result:
                            return ensure_pontos_never_empty(result, mode)
                if attempt < max_retries:
                    time.sleep(1.5)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt < max_retries:
                    time.sleep(1.5)
                break
            except Exception:
                if attempt < max_retries:
                    time.sleep(1.5)
                break
    return ensure_pontos_never_empty({'melhorias': [], 'fortes': []}, mode)


def stats_pontos_melhoria_fortes(issues, ollama_url, field_ids, limit=12, auth=None, sla_by_key=None):
    """
    Extrai pontos de melhoria e fortes via Ollama.
    Pontos de melhoria: no mínimo 5 tickets (Satisfaction 1-3 e SLA vencido; se faltar, completa com sat 1-3 ou SLA vencido).
    Pontos fortes: no mínimo 5 tickets (Satisfaction=5 e dentro do SLA; se faltar, completa com sat 5 ou dentro do SLA).
    sla_by_key: dados da coluna SLAs do modo lista (obrigatório para seleção correta).
    Retorna: top5Melhoria, top5Fortes, melhoriaByRequestType, pontosPorIssue.
    """
    from collections import defaultdict
    import unicodedata
    import time
    count_melhoria = defaultdict(int)
    count_forte = defaultdict(int)
    melhoria_keys = defaultdict(list)
    forte_keys = defaultdict(list)
    by_rt_melhoria = defaultdict(lambda: defaultdict(int))
    pontos_por_issue = []
    sla_by_key = sla_by_key or {}
    # Pontos de melhoria: no mínimo 5 tickets (mesmo critério de quantidade dos pontos positivos). Preferir Satisfaction 1-3 e SLA vencido; completar até 5.
    melhoria_candidates = []
    seen_melhoria = set()
    for issue in issues:
        key = issue.get('key')
        if not key or key in seen_melhoria:
            continue
        sat = get_satisfaction_numeric(issue, field_ids)
        if sat is None or sat not in (1, 2, 3):
            continue
        if not _issue_sla_breached(key, sla_by_key):
            continue
        melhoria_candidates.append(issue)
        seen_melhoria.add(key)
        if len(melhoria_candidates) >= 5:
            break
    if len(melhoria_candidates) < 5:
        for issue in issues:
            key = issue.get('key')
            if not key or key in seen_melhoria:
                continue
            sat = get_satisfaction_numeric(issue, field_ids)
            if sat is None or sat not in (1, 2, 3):
                continue
            melhoria_candidates.append(issue)
            seen_melhoria.add(key)
            if len(melhoria_candidates) >= 5:
                break
    if len(melhoria_candidates) < 5:
        for issue in issues:
            key = issue.get('key')
            if not key or key in seen_melhoria:
                continue
            if not _issue_sla_breached(key, sla_by_key):
                continue
            melhoria_candidates.append(issue)
            seen_melhoria.add(key)
            if len(melhoria_candidates) >= 5:
                break
    # Pontos fortes: até 5 tickets. Preferir Satisfaction=5 e dentro do SLA; se faltar, completar com Satisfaction=5 ou dentro do SLA (só Jira).
    fortes_candidates = []
    seen_fortes = set()
    for issue in issues:
        key = issue.get('key')
        if not key or key in seen_fortes:
            continue
        sat = get_satisfaction_numeric(issue, field_ids)
        if sat != 5:
            continue
        if not _issue_sla_within(key, sla_by_key):
            continue
        fortes_candidates.append(issue)
        seen_fortes.add(key)
        if len(fortes_candidates) >= 5:
            break
    if len(fortes_candidates) < 5:
        for issue in issues:
            key = issue.get('key')
            if not key or key in seen_fortes:
                continue
            sat = get_satisfaction_numeric(issue, field_ids)
            within = _issue_sla_within(key, sla_by_key)
            if sat == 5 or within:
                fortes_candidates.append(issue)
                seen_fortes.add(key)
                if len(fortes_candidates) >= 5:
                    break
    # Analisar melhoria (boas práticas para melhorar resultados negativos)
    for idx, issue in enumerate(melhoria_candidates):
        if idx > 0:
            time.sleep(0.8)
        key = issue.get('key')
        rt = _get_request_type_from_issue(issue, field_ids)
        comments_text = fetch_issue_comments_text(auth, key, max_comments=20) or '' if auth else ''
        confluence_text = fetch_confluence_for_issue(issue, auth, field_ids) if auth else ''
        out = get_issue_pontos_ollama(issue, ollama_url, field_ids, comments_text=comments_text, auth=auth, sla_by_key=sla_by_key, mode='melhoria', confluence_text=confluence_text)
        summary = ((issue.get('fields') or {}).get('summary') or '').strip() or '(sem título)'
        pontos_por_issue.append({'key': key, 'summary': summary[:200], 'melhorias': list(out.get('melhorias') or []), 'fortes': []})
        for m in (out.get('melhorias') or []):
            if not _is_sensible_ponto(m):
                continue
            m_norm = m.strip().lower()[:150]
            if not m_norm:
                continue
            m_norm = ''.join(c for c in unicodedata.normalize('NFD', m_norm) if unicodedata.category(c) != 'Mn')
            count_melhoria[m_norm] += 1
            melhoria_keys[m_norm].append(key)
            by_rt_melhoria[rt][m_norm] += 1
    # Analisar fortes (o que foi feito de bom nos tickets Satisfaction=5 e dentro do SLA)
    for idx, issue in enumerate(fortes_candidates):
        if idx > 0:
            time.sleep(0.8)
        key = issue.get('key')
        rt = _get_request_type_from_issue(issue, field_ids)
        comments_text = fetch_issue_comments_text(auth, key, max_comments=20) or '' if auth else ''
        confluence_text = fetch_confluence_for_issue(issue, auth, field_ids) if auth else ''
        out = get_issue_pontos_ollama(issue, ollama_url, field_ids, comments_text=comments_text, auth=auth, sla_by_key=sla_by_key, mode='fortes', confluence_text=confluence_text)
        summary = ((issue.get('fields') or {}).get('summary') or '').strip() or '(sem título)'
        pontos_por_issue.append({'key': key, 'summary': summary[:200], 'melhorias': [], 'fortes': list(out.get('fortes') or [])})
        for f in (out.get('fortes') or []):
            if not _is_sensible_ponto(f):
                continue
            f_norm = f.strip().lower()[:150]
            if not f_norm:
                continue
            f_norm = ''.join(c for c in unicodedata.normalize('NFD', f_norm) if unicodedata.category(c) != 'Mn')
            count_forte[f_norm] += 1
            forte_keys[f_norm].append(key)
    top5_melhoria = sorted(count_melhoria.items(), key=lambda x: -x[1])[:5]
    top5_fortes = sorted(count_forte.items(), key=lambda x: -x[1])[:5]
    melhoria_by_rt = {}
    for rt, d in by_rt_melhoria.items():
        melhoria_by_rt[rt] = dict(sorted(d.items(), key=lambda x: -x[1])[:5])
    def unique_order(seq):
        return list(dict.fromkeys(seq))
    return {
        'top5Melhoria': [{'label': lbl, 'count': c, 'keys': unique_order(melhoria_keys[lbl])} for lbl, c in top5_melhoria],
        'top5Fortes': [{'label': lbl, 'count': c, 'keys': unique_order(forte_keys[lbl])} for lbl, c in top5_fortes],
        'melhoriaByRequestType': melhoria_by_rt,
        'pontosPorIssue': pontos_por_issue,
    }


def get_issue_note_from_rovo(issue, rovo_url, rovo_api_key=None, criteria=None):
    """
    Usa o agente Rovo (Atlassian Studio) para dar uma nota de 1 a 5 ao chamado.
    rovo_url: endpoint que recebe POST {key, summary, description, criteria} e retorna {nota, comentario}.
    criteria: critérios de avaliação enviados ao Rovo (usa ROVO_DEFAULT_CRITERIA se não informado).
    Retorna {"nota": int, "comentario": str} ou None se erro.
    """
    if not rovo_url or not rovo_url.strip():
        return None
    fields = issue.get('fields', {})
    payload = {
        'key': issue.get('key', ''),
        'summary': (fields.get('summary') or '').strip() or '(sem título)',
        'description': _description_to_plain_text(fields.get('description'))[:3000],
        'criteria': (criteria or ROVO_DEFAULT_CRITERIA).strip()[:1500],
        'agentName': os.environ.get('ROVO_AGENT_NAME', 'L1/BPO Ticket Helper').strip(),
        'cloudId': os.environ.get('ROVO_CLOUD_ID', 'c43390d3-e5f8-43ca-9eec-c382a5220bd9').strip(),
    }
    headers = {'Content-Type': 'application/json'}
    if rovo_api_key and rovo_api_key.strip():
        headers['Authorization'] = f'Bearer {rovo_api_key.strip()}'
    try:
        r = requests.post(
            rovo_url.strip(),
            json=payload,
            headers=headers,
            timeout=30,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        nota = data.get('nota')
        if nota is not None:
            nota = max(1, min(5, int(nota)))
        return {'nota': nota or 0, 'comentario': (data.get('comentario') or '')[:200]}
    except Exception:
        return None


def get_issue_note_from_agent(issue, api_key):
    """
    Usa um agente (OpenAI) para dar uma nota de 1 a 5 ao chamado.
    Retorna {"nota": int, "comentario": str} ou None se sem API key/erro.
    """
    if not api_key or not api_key.strip():
        return None
    try:
        import openai
    except ImportError:
        return None
    fields = issue.get('fields', {})
    summary = (fields.get('summary') or '').strip() or '(sem título)'
    description = _description_to_plain_text(fields.get('description'))
    text = f'Título: {summary}\nDescrição: {description}'[:3000]
    try:
        client = openai.OpenAI(api_key=api_key.strip())
        r = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[
                {'role': 'system', 'content': (
                    'Você avalia chamados de suporte. Responda APENAS com um JSON válido, sem markdown, no formato: '
                    '{"nota": N, "comentario": "frase curta"} onde N é um número de 1 a 5 (1=baixa urgência/clareza, 5=alta). '
                    'comentario deve ser uma única frase em português explicando a nota.'
                )},
                {'role': 'user', 'content': 'Avalie este chamado e dê uma nota de 1 a 5:\n\n' + text},
            ],
            max_tokens=150,
            temperature=0.3,
        )
        content = (r.choices[0].message.content or '').strip()
        if not content:
            return None
        import json
        # Remover possível markdown
        if content.startswith('```'):
            content = content.split('```')[1]
            if content.startswith('json'):
                content = content[4:]
        data = json.loads(content)
        nota = data.get('nota')
        if nota is not None:
            nota = max(1, min(5, int(nota)))
        return {'nota': nota or 0, 'comentario': (data.get('comentario') or '')[:200]}
    except Exception:
        return None


def get_issue_note_from_vertex(
    issue, project_id, location,
    credentials_path=None,
    client_id=None, client_secret=None, refresh_token=None,
    model=None,
    criteria=None,
    field_ids=None,
    comments_text=None,
):
    """
    Usa Vertex AI (Gemini) para dar uma nota de 1 a 5 ao chamado.
    Autenticação (em ordem de prioridade):
    1) credentials_path: path para o JSON da service account (GOOGLE_APPLICATION_CREDENTIALS).
    2) client_id + client_secret + refresh_token: OAuth (ex.: VERTEX_CLIENT_ID, VERTEX_CLIENT_SECRET, VERTEX_REFRESH_TOKEN no .env).
    3) ADC (gcloud auth application-default login).
    project_id: VERTEX_PROJECT_ID. location: VERTEX_LOCATION (ex.: us-central1).
    model: nome do modelo (ex.: gemini-2.0-flash-exp); usa VERTEX_MODEL ou gemini-2.0-flash se não informado.
    field_ids: opcional; usado para incluir tempo de 1ª resposta e resolução no contexto.
    comments_text: opcional; texto dos comentários do issue (respostas do Assignee) para análise textual.
    Retorna {"nota": int, "comentario": str} ou dict com nota None e comentario com erro.
    """
    def _err(msg):
        return {'nota': None, 'comentario': f'Erro Vertex: {msg}'}
    if not project_id or not str(project_id).strip():
        return _err('VERTEX_PROJECT_ID não configurado')
    project_id = str(project_id).strip()
    location = (location or 'us-central1').strip()
    model = (model or os.environ.get('VERTEX_MODEL', '') or 'gemini-2.0-flash').strip()
    if credentials_path and credentials_path.strip():
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path.strip()
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GOOGLE_CLOUD_LOCATION'] = location
    os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
    try:
        from google import genai
        from google.genai.types import HttpOptions
    except ImportError as e:
        return _err(f'Pacote google-genai não instalado: {e}')
    credentials = None
    if client_id and client_secret and refresh_token:
        try:
            from google.oauth2.credentials import Credentials
            import google.auth.transport.requests
            credentials = Credentials(
                token=None,
                refresh_token=refresh_token.strip(),
                token_uri='https://oauth2.googleapis.com/token',
                client_id=client_id.strip(),
                client_secret=client_secret.strip(),
                scopes=['https://www.googleapis.com/auth/cloud-platform'],
            )
            credentials.refresh(google.auth.transport.requests.Request())
        except Exception:
            credentials = None
    # Contexto: título, descrição, tempo de 1ª resposta, tempo de resolução, comentários (respostas do Assignee)
    context = _issue_context_for_ollama(issue, field_ids)
    if comments_text and comments_text.strip():
        context += '\n\nComentários / respostas do Assignee:\n' + (comments_text.strip()[:4000])
    text = context
    system = (
        'Você avalia chamados de suporte L1. Dê uma nota de 1 a 5 e um comentário curto em português. '
        'Responda APENAS com um JSON válido, sem markdown: {"nota": N, "comentario": "frase curta em português"}. '
        'Critérios de avaliação: (1) Tempo de resposta do Assignee (1ª resposta e resolução); '
        '(2) Análise textual das respostas do Assignee (clareza, objetividade); '
        '(3) Se o problema foi realmente resolvido; (4) Se a comunicação foi clara e ágil. '
        'Escala: 1=ruim (resposta lenta, vaga ou sem resolução), 5=excelente (chamado claro, completo, resposta rápida, problema resolvido, comunicação clara e ágil). '
        'Exemplo de nota 5: chamado claro, completo e com impacto direto no desenvolvimento, justificando a urgência; assignee respondeu rápido e resolveu com clareza.'
    )
    if criteria and str(criteria).strip():
        system += ' Critérios adicionais: ' + str(criteria).strip()[:800]
    prompt = system + '\n\n---\n\nAvalie este chamado e dê uma nota de 1 a 5:\n\n' + text
    import json
    import time
    max_retries = 3
    backoff_seconds = [20, 40, 80]
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            client_kw = dict(vertexai=True, project=project_id, location=location, http_options=HttpOptions(api_version='v1'))
            if credentials is not None:
                client_kw['credentials'] = credentials
            client = genai.Client(**client_kw)
            response = client.models.generate_content(model=model, contents=prompt)
            content = (getattr(response, 'text', None) or '').strip()
            if not content and hasattr(response, 'candidates') and response.candidates:
                parts = getattr(response.candidates[0], 'content', None)
                if hasattr(parts, 'parts') and parts.parts:
                    content = (getattr(parts.parts[0], 'text', None) or '').strip()
            if not content:
                return {'nota': None, 'comentario': 'Erro Vertex: resposta vazia'}
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
            content = content.strip()
            data = json.loads(content)
            nota = data.get('nota')
            if nota is not None:
                nota = max(1, min(5, int(nota)))
            return {'nota': nota or 0, 'comentario': (data.get('comentario') or '')[:200]}
        except json.JSONDecodeError as e:
            return {'nota': None, 'comentario': f'Erro Vertex: resposta não é JSON válido ({e})'}
        except Exception as e:
            last_error = e
            err_msg = str(e).strip()
            is_rate_limit = (
                '429' in err_msg or 'RESOURCE_EXHAUSTED' in err_msg or 'Quota exceeded' in err_msg
                or 'quota' in err_msg.lower()
            )
            if is_rate_limit and attempt < max_retries:
                wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
                time.sleep(wait)
                continue
            return {'nota': None, 'comentario': f'Erro Vertex: {err_msg[:300]}'}
    return {'nota': None, 'comentario': f'Erro Vertex: {str(last_error).strip()[:300]}'}


def search_jql(auth, jql, field_ids, limit=None, columns=None):
    """
    Run JQL search using /rest/api/3/search/jql.
    Returns list of issue dicts.
    columns: opcional, lista de {id, label} do filtro; quando informado, usa esses campos na busca.
    """
    if columns:
        fields_to_fetch = [c['id'] for c in columns if c.get('id')]
        if 'key' not in fields_to_fetch and 'issuekey' not in fields_to_fetch:
            fields_to_fetch.insert(0, 'key')
        # Garantir status (portal), created/resolutiondate para cálculo de Time to resolution
        for fid in ('status', 'summary', 'description', 'created', 'updated', 'resolutiondate', NUBANK_TIME_TO_RESOLUTION_FID, NUBANK_TIME_TO_FIRST_RESPONSE_FID):
            if fid not in fields_to_fetch:
                fields_to_fetch.append(fid)
    else:
        fields_to_fetch = ['key', 'summary', 'description', 'reporter', 'assignee', 'status', 'created', 'updated', 'resolutiondate']
        for name, fid in field_ids.items():
            if fid and fid not in fields_to_fetch:
                fields_to_fetch.append(fid)
        for fid in (NUBANK_TIME_TO_RESOLUTION_FID, NUBANK_TIME_TO_FIRST_RESPONSE_FID):
            if fid not in fields_to_fetch:
                fields_to_fetch.append(fid)

    all_issues = []
    next_page_token = None
    batch_size = min(100, limit) if limit else 100
    max_retries = 5

    while True:
        if limit and len(all_issues) >= limit:
            break
        current_batch = min(batch_size, limit - len(all_issues)) if limit else batch_size

        payload = {
            'jql': jql,
            'maxResults': current_batch,
            'fields': fields_to_fetch,
        }
        if next_page_token:
            payload['nextPageToken'] = next_page_token

        for retry in range(max_retries):
            r = requests.post(
                f'{JIRA_URL}/rest/api/3/search/jql',
                auth=auth,
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                json=payload,
                timeout=60,
            )
            if r.status_code == 429:
                time.sleep(int(r.headers.get('Retry-After', 5)))
                continue
            break
        if r.status_code != 200:
            raise RuntimeError(f'Jira API error: {r.status_code} - {r.text}')

        data = r.json()
        issues = data.get('issues', [])
        all_issues.extend(issues)
        next_page_token = data.get('nextPageToken')
        if not next_page_token or (limit and len(all_issues) >= limit):
            break

    if limit and len(all_issues) > limit:
        all_issues = all_issues[:limit]
    return all_issues


# JQL para tickets reabertos: status que saiu de Done/Closed/Resolved (reabertura). Período = quando a mudança ocorreu (DURING).
REOPENED_JQL_BASE = 'status CHANGED FROM ("Done", "Closed", "Resolved")'


def _reopened_jql_for_period(month, year):
    """Monta JQL de reabertura com o mesmo período (mês/ano) usado no modo lista.
    DURING filtra pela data em que o status foi alterado (reabertura)."""
    import calendar
    try:
        last_day = calendar.monthrange(int(year), int(month))[1]
        first = f'{year}-{month:02d}-01'
        last = f'{year}-{month:02d}-{last_day}'
        return f'{REOPENED_JQL_BASE} DURING ("{first}", "{last}")'
    except (TypeError, ValueError):
        return None


def stats_reopened_for_period(auth, field_ids, month, year, limit=5000):
    """
    Busca tickets reabertos no mesmo período (mês/ano) selecionado no modo lista.
    JQL: status CHANGED FROM ("Done", "Closed", "Resolved") DURING (início, fim do mês).
    Retorna { 'total', 'byPeriod': [ { period, count } ], 'keys': [ ... ] }.
    Em falha (ex.: Jira sem suporte a DURING com CHANGED), retorna total 0 e listas vazias.
    """
    jql = _reopened_jql_for_period(month, year)
    if not jql or not auth:
        return {'total': 0, 'byPeriod': [], 'keys': []}
    try:
        issues = search_jql(auth, jql, field_ids, limit=limit)
    except Exception:
        return {'total': 0, 'byPeriod': [], 'keys': []}
    keys = [i.get('key') for i in issues if i.get('key')]
    period_key = f'{year}-{month:02d}' if month and year else None
    by_period = [{'period': period_key, 'count': len(issues)}] if period_key else []
    return {'total': len(issues), 'byPeriod': by_period, 'keys': keys}


def _format_seconds(seconds):
    """Formata duração em segundos para exibição (ex.: 86400 -> '1d 0h'). Aceita milissegundos se valor muito grande."""
    if seconds is None:
        return ''
    try:
        total = int(float(seconds))
        if total < 0:
            return ''
        # Se valor > ~10 anos em segundos, provavelmente está em milissegundos
        if total > 86400 * 365 * 10:
            total = total // 1000
        days, r = divmod(total, 86400)
        hours, r = divmod(r, 3600)
        mins, secs = divmod(r, 60)
        if days > 0:
            return f'{days}d {hours}h'
        if hours > 0:
            return f'{hours}h {mins}m'
        if mins > 0:
            return f'{mins}m'
        return f'{secs}s'
    except (TypeError, ValueError):
        return ''


def _format_seconds_hhmm(seconds):
    """Formata duração em segundos como HH:MM (ex.: 50880 -> '14:13'). Suporta valores negativos (ex.: -2:18)."""
    if seconds is None:
        return ''
    try:
        total = int(float(seconds))
        neg = total < 0
        if neg:
            total = abs(total)
        # Se valor > ~10 anos em segundos, provavelmente está em milissegundos
        if total > 86400 * 365 * 10:
            total = total // 1000
        hours = total // 3600
        minutes = (total % 3600) // 60
        s = f'{hours}:{minutes:02d}'
        return f'-{s}' if neg else s
    except (TypeError, ValueError):
        return ''


def _parse_duration_string(s):
    """Parse string like '1d 2h 30m', '2h 30m', '5m' or '15m' to seconds. Espaço opcional entre número e letra."""
    if not s or not isinstance(s, str) or not s.strip():
        return None
    import re
    total = 0
    m = re.search(r'(\d+)\s*d', s, re.I)
    if m:
        total += int(m.group(1)) * 86400
    m = re.search(r'(\d+)\s*h', s, re.I)
    if m:
        total += int(m.group(1)) * 3600
    m = re.search(r'(\d+)\s*m', s, re.I)
    if m:
        total += int(m.group(1)) * 60
    m = re.search(r'(\d+)\s*s', s, re.I)
    if m:
        total += int(m.group(1))
    return total if total > 0 else None


def _parse_sla_timestamp_to_seconds(ts):
    """
    Converte o texto de tempo da coluna SLA (mesma fonte do modo lista) para segundos.
    Aceita: '2h 30m', '1d 0h', '14:13', '-2:18', ou string com ' (em andamento)'.
    Usado para preencher duration_seconds quando a API não retorna elapsedTime numérico.
    """
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()
    if ' (em andamento)' in s:
        s = s.replace(' (em andamento)', '').strip()
    if not s:
        return None
    # Formato HH:MM ou -HH:MM (como _format_seconds_hhmm)
    import re
    mm = re.match(r'^(-?)(\d+):(\d{2})$', s.strip())
    if mm:
        sign = -1 if mm.group(1) == '-' else 1
        h, m = int(mm.group(2)), int(mm.group(3))
        return sign * (h * 3600 + m * 60)
    # Formato 1d 2h 30m
    return _parse_duration_string(s)


def _duration_to_seconds(val):
    """Converte valor do campo Time to first response (Jira) para segundos. Retorna None se não for possível."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        s = int(float(val))
        if s > 1e10:
            return s // 1000
        return s
    if isinstance(val, str):
        return _parse_duration_string(val)
    if isinstance(val, dict):
        if val.get('seconds') is not None:
            return int(val['seconds'])
        v = val.get('value')
        if v is not None and isinstance(v, (int, float)):
            s = int(float(v))
            return s // 1000 if s > 1e10 else s
        if v is not None and isinstance(v, str):
            return _parse_duration_string(v)
        for key in ('friendly', 'display'):
            s = val.get(key) or ''
            if isinstance(s, str) and s.strip():
                out = _parse_duration_string(s)
                if out is not None:
                    return out
    return None


def format_field_value(val):
    """Format a raw Jira field value for display (Time to resolution / Time to first response em HH:MM, como no Jira)."""
    if val is None:
        return ''
    if isinstance(val, (int, float)):
        # Duração em segundos -> HH:MM (ex.: 14:13, -2:18)
        return _format_seconds_hhmm(val)
    if isinstance(val, dict):
        # Jira Service Desk "Request Type" (objeto com requestType.name)
        if 'requestType' in val:
            rt = val['requestType']
            if isinstance(rt, dict):
                return rt.get('name') or rt.get('defaultName') or ''
            return format_field_value(rt)
        # Objeto de duração (Jira pode retornar friendly, seconds, value)
        if 'friendly' in val and val.get('friendly'):
            return str(val.get('friendly'))
        if 'seconds' in val:
            return _format_seconds_hhmm(val.get('seconds'))
        if 'value' in val and val.get('value') is not None:
            v = val.get('value')
            if isinstance(v, (int, float)):
                return _format_seconds_hhmm(v)
            return str(v)
        return val.get('displayName') or val.get('name') or val.get('value') or str(val)
    if isinstance(val, (list, tuple)):
        return ', '.join(format_field_value(v) for v in val) if val else ''
    # String (ex.: "2h 30m" já formatado pelo Jira)
    if isinstance(val, str) and val.strip():
        return val.strip()
    return str(val) if val else ''


def _parse_iso_date(s):
    """Parse ISO date string to datetime; return None on failure."""
    if not s:
        return None
    try:
        from datetime import datetime
        import re
        t = s.strip().replace('Z', '+00:00')
        # Jira pode retornar -0300 (sem dois pontos); fromisoformat espera -03:00
        t = re.sub(r'([+-])(\d{2})(\d{2})$', r'\1\2:\3', t)
        return datetime.fromisoformat(t)
    except Exception:
        try:
            # Fallback: só a parte da data
            from datetime import datetime
            return datetime.fromisoformat(str(s)[:19].replace('Z', '+00:00'))
        except Exception:
            return None


def _format_duration(created_iso, resolved_iso):
    """Compute duration between created and resolutiondate; return HH:MM (como no Jira)."""
    created = _parse_iso_date(created_iso)
    resolved = _parse_iso_date(resolved_iso)
    if not created or not resolved:
        return ''
    delta = resolved - created
    total_seconds = int(delta.total_seconds())
    return _format_seconds_hhmm(total_seconds)


def get_field_display_value(issue, field_id, field_ids):
    """Retorna o valor formatado de um campo do issue (para exibição em coluna dinâmica)."""
    if not field_id:
        return ''
    if field_id == 'issuekey' or field_id == 'key':
        return issue.get('key', '')
    fields = issue.get('fields', {})
    val = fields.get(field_id)
    if val is None:
        return ''
    if field_id == 'reporter':
        return val.get('displayName', '') if isinstance(val, dict) else format_field_value(val)
    if field_id == 'assignee':
        return val.get('displayName', 'Unassigned') if isinstance(val, dict) else (format_field_value(val) or 'Unassigned')
    if field_id == 'issuetype':
        return val.get('name', '') if isinstance(val, dict) else format_field_value(val)
    if field_id == 'status':
        return val.get('name', '') if isinstance(val, dict) else format_field_value(val)
    if field_id in ('created', 'updated', 'resolutiondate') and val:
        parsed = _parse_iso_date(val)
        if parsed:
            return parsed.strftime('%Y-%m-%d %H:%M')
        return str(val)[:19] if len(str(val)) >= 19 else str(val)
    if field_id == NUBANK_TIME_TO_RESOLUTION_FID or field_id == field_ids.get('Time to resolution'):
        fmt = format_field_value(val)
        if fmt:
            return fmt
        return _format_duration(fields.get('created'), fields.get('resolutiondate')) or 'Em aberto'
    if field_id == NUBANK_TIME_TO_FIRST_RESPONSE_FID or field_id == field_ids.get('Time to first response'):
        return format_field_value(val) or '—'
    return format_field_value(val)


def get_row_values_for_columns(issue, columns, field_ids):
    """Retorna lista de valores para exibição, na ordem das colunas do filtro."""
    return [get_field_display_value(issue, col['id'], field_ids) for col in columns]


def get_row_values(issue, field_ids):
    """Extract display row: Reporter, Time to resolution, Time to first response, Assignee, Request Type."""
    fields = issue.get('fields', {})
    key = issue.get('key', '')

    reporter = fields.get('reporter') or {}
    reporter_str = reporter.get('displayName', '') if isinstance(reporter, dict) else format_field_value(reporter)

    assignee = fields.get('assignee') or {}
    assignee_str = assignee.get('displayName', 'Unassigned') if isinstance(assignee, dict) else (format_field_value(assignee) or 'Unassigned')

    time_to_res = ''
    fid = field_ids.get('Time to resolution')
    if fid:
        time_to_res = format_field_value(fields.get(fid))
    if not time_to_res:
        time_to_res = format_field_value(fields.get(NUBANK_TIME_TO_RESOLUTION_FID))
    if not time_to_res:
        time_to_res = _format_duration(fields.get('created'), fields.get('resolutiondate'))
    if not time_to_res:
        # Issue em aberto (sem resolutiondate) ou campo não preenchido no Jira
        time_to_res = 'Em aberto' if not fields.get('resolutiondate') else '—'

    time_to_first = ''
    fid = field_ids.get('Time to first response')
    if fid:
        time_to_first = format_field_value(fields.get(fid))
    if not time_to_first:
        time_to_first = format_field_value(fields.get(NUBANK_TIME_TO_FIRST_RESPONSE_FID))
    if not time_to_first:
        time_to_first = '—'  # campo não preenchido no Jira para este issue

    request_type = ''
    fid = field_ids.get('Request Type')
    if fid:
        request_type = format_field_value(fields.get(fid))

    satisfaction = '—'
    fid_sat = field_ids.get('Satisfaction')
    if fid_sat:
        raw = fields.get(fid_sat)
        if raw is not None:
            if isinstance(raw, (int, float)) and 1 <= raw <= 5:
                satisfaction = str(int(raw))
            elif isinstance(raw, dict):
                v = raw.get('rating') or raw.get('value') or raw.get('id') or raw.get('name')
                if v is not None:
                    try:
                        n = int(v) if isinstance(v, (int, float)) else int(str(v).strip())
                        if 1 <= n <= 5:
                            satisfaction = str(n)
                        else:
                            satisfaction = str(n)
                    except (ValueError, TypeError):
                        satisfaction = str(raw.get('displayName') or raw.get('name') or v)
                else:
                    satisfaction = str(raw.get('displayName') or raw.get('name') or raw)
            else:
                try:
                    n = int(raw) if isinstance(raw, (int, float)) else int(str(raw).strip())
                    satisfaction = str(n) if 1 <= n <= 5 else str(raw)
                except (ValueError, TypeError):
                    satisfaction = str(raw)

    return {
        'key': key,
        'Reporter': reporter_str,
        'Time to resolution': time_to_res,
        'Time to first response': time_to_first,
        'Assignee': assignee_str,
        'Request Type': request_type,
        'Satisfaction': satisfaction,
    }


def _issue_sla_breached(issue_key, sla_by_key):
    """True se o chamado tem pelo menos um SLA relevante (TTR/FRT/Time to close) estourado. Usa dados da coluna SLAs."""
    slas = (sla_by_key or {}).get(issue_key) or []
    relevant = [s for s in slas if _sla_name_is_relevant(s.get('name'))]
    if not relevant:
        return False
    return any(not s.get('met', True) for s in relevant)


def _issue_sla_within(issue_key, sla_by_key):
    """True se o chamado tem SLAs relevantes e todos cumpridos. Usa dados da coluna SLAs."""
    slas = (sla_by_key or {}).get(issue_key) or []
    relevant = [s for s in slas if _sla_name_is_relevant(s.get('name'))]
    if not relevant:
        return False
    return all(s.get('met', True) for s in relevant)


def get_satisfaction_numeric(issue, field_ids):
    """Retorna o valor numérico 1-5 do campo Satisfaction do Jira, ou None se ausente/inválido.
    Aceita formato {'rating': 5} ou value/id/name."""
    fid = field_ids.get('Satisfaction')
    if not fid:
        return None
    raw = (issue.get('fields') or {}).get(fid)
    if raw is None:
        return None
    if isinstance(raw, (int, float)) and 1 <= raw <= 5:
        return int(raw)
    if isinstance(raw, dict):
        v = raw.get('rating') or raw.get('value') or raw.get('id') or raw.get('name')
        if v is not None:
            try:
                n = int(v) if isinstance(v, (int, float)) else int(str(v).strip())
                return n if 1 <= n <= 5 else None
            except (ValueError, TypeError):
                pass
    try:
        n = int(raw) if isinstance(raw, (int, float)) else int(str(raw).strip())
        return n if 1 <= n <= 5 else None
    except (ValueError, TypeError):
        return None


def stats_csat(issues, field_ids):
    """
    Calcula CSAT (Customer Satisfaction) apenas a partir da coluna Satisfaction do Jira (1-5 estrelas).
    Retorna { 'average', 'byStar', 'totalWithSatisfaction' }.
    """
    by_star = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    total = 0
    sum_val = 0
    for issue in issues:
        n = get_satisfaction_numeric(issue, field_ids)
        if n is not None:
            by_star[n] = by_star.get(n, 0) + 1
            total += 1
            sum_val += n
    average = round(sum_val / total, 2) if total else None
    return {'average': average, 'byStar': by_star, 'totalWithSatisfaction': total}


def _median(values):
    """Retorna mediana de uma lista numérica; None se vazia."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    if n % 2:
        return round(s[n // 2], 2)
    return round((s[n // 2 - 1] + s[n // 2]) / 2, 2)


def _period_key(dt, by_month=False):
    """Retorna chave de período para agrupamento: 'YYYY-Www' (semana) ou 'YYYY-MM' (mês)."""
    if not dt:
        return None
    if by_month:
        return dt.strftime('%Y-%m')
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def stats_sla_aggregate(sla_by_key):
    """
    Agrega SLAs por issue. sla_by_key: dict issue_key -> list of {'name', 'met'}.
    Retorna: pctWithinSla, totalWithSla, totalBreached, violationCountByName (histograma de violações por nome do SLA).
    """
    total_with_sla = 0
    total_met = 0
    violation_count_by_name = {}
    for key, slas in (sla_by_key or {}).items():
        if not slas:
            continue
        total_with_sla += 1
        all_met = all(s.get('met', True) for s in slas)
        if all_met:
            total_met += 1
        else:
            for s in slas:
                if not s.get('met', True):
                    name = (s.get('name') or '—').strip() or 'SLA'
                    violation_count_by_name[name] = violation_count_by_name.get(name, 0) + 1
    pct = round(100 * total_met / total_with_sla, 1) if total_with_sla else None
    return {
        'pctWithinSla': pct,
        'totalWithSla': total_with_sla,
        'totalMet': total_met,
        'totalBreached': total_with_sla - total_met,
        'violationCountByName': violation_count_by_name,
    }


def _get_issue_resolution_and_frt_seconds(issue, field_ids):
    """Retorna (resolution_seconds, first_response_seconds) para um issue; (None, None) se não houver."""
    fields = issue.get('fields', {})
    created = _parse_iso_date(fields.get('created'))
    resolved = _parse_iso_date(fields.get('resolutiondate'))
    res_sec = None
    if created and resolved:
        res_sec = int((resolved - created).total_seconds())
    fr_sec = None
    for fid in (field_ids.get('Time to first response'), NUBANK_TIME_TO_FIRST_RESPONSE_FID):
        if not fid:
            continue
        val = fields.get(fid)
        fr_sec = _duration_to_seconds(val)
        if fr_sec is not None and fr_sec >= 0:
            break
    return res_sec, fr_sec


def stats_ttr_frt_by_period(issues, field_ids, by_month=False):
    """
    TTR e FRT medianos por semana ou mês (apenas com dados já presentes no issue; sem fallback de comentário).
    Retorna { 'byPeriod': [ { 'period', 'medianTtrHours', 'medianFrtHours', 'count' } ], 'periodList': [...] }.
    """
    by_period = {}
    for issue in issues:
        created = _parse_iso_date((issue.get('fields') or {}).get('created'))
        if not created:
            continue
        pk = _period_key(created, by_month=by_month)
        if not pk:
            continue
        if pk not in by_period:
            by_period[pk] = {'ttr_sec': [], 'frt_sec': [], 'count': 0}
        by_period[pk]['count'] += 1
        res_sec, fr_sec = _get_issue_resolution_and_frt_seconds(issue, field_ids)
        if res_sec is not None:
            by_period[pk]['ttr_sec'].append(res_sec)
        if fr_sec is not None and fr_sec >= 0:
            by_period[pk]['frt_sec'].append(fr_sec)
    period_list = sorted(by_period.keys())
    out = []
    for p in period_list:
        data = by_period[p]
        median_ttr = _median(data['ttr_sec'])
        median_frt = _median(data['frt_sec'])
        out.append({
            'period': p,
            'medianTtrHours': round(median_ttr / 3600, 2) if median_ttr is not None else None,
            'medianFrtHours': round(median_frt / 3600, 2) if median_frt is not None else None,
            'count': data['count'],
        })
    return {'byPeriod': out, 'periodList': period_list}


def stats_nota_temporal(issues, notas, by_month=False):
    """
    Nota final média por período; distribuição 1-5; top analistas por nota.
    notas: dict issue_key -> { 'nota': 1-5, 'comentario': ... }.
    Retorna: byPeriod, distribution, topAnalysts.
    """
    from collections import defaultdict
    by_period = defaultdict(lambda: {'sum': 0, 'count': 0})
    distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    by_analyst = defaultdict(lambda: {'sum': 0, 'count': 0})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        entry = (notas or {}).get(key)
        nota = entry.get('nota') if isinstance(entry, dict) else None
        if nota is None:
            continue
        try:
            n = int(nota)
        except (TypeError, ValueError):
            continue
        if n < 1 or n > 5:
            continue
        created = _parse_iso_date((issue.get('fields') or {}).get('created'))
        if created:
            pk = _period_key(created, by_month=by_month)
            if pk:
                by_period[pk]['sum'] += n
                by_period[pk]['count'] += 1
        distribution[n] = distribution.get(n, 0) + 1
        assignee = (issue.get('fields') or {}).get('assignee') or {}
        name = assignee.get('displayName', '') if isinstance(assignee, dict) else (str(assignee) or 'Unassigned')
        if not name:
            name = 'Unassigned'
        by_analyst[name]['sum'] += n
        by_analyst[name]['count'] += 1
    period_list = sorted(by_period.keys())
    by_period_list = []
    for p in period_list:
        d = by_period[p]
        avg = round(d['sum'] / d['count'], 2) if d['count'] else None
        by_period_list.append({'period': p, 'avgNota': avg, 'count': d['count']})
    analysts_list = []
    for name, d in by_analyst.items():
        if d['count'] > 0:
            analysts_list.append({
                'assignee': name,
                'avgNota': round(d['sum'] / d['count'], 2),
                'count': d['count'],
            })
    analysts_list.sort(key=lambda x: (-x['avgNota'], -x['count']))
    top_analysts = analysts_list[:15]
    bottom_analysts = sorted(analysts_list, key=lambda x: (x['avgNota'], -x['count']))[:10]
    return {
        'byPeriod': by_period_list,
        'distribution': distribution,
        'topAnalysts': top_analysts,
        'bottomAnalysts': bottom_analysts,
    }


def stats_csat_by_period(issues, field_ids, by_month=False):
    """CSAT médio por período (semana ou mês). Retorna { 'byPeriod': [ { period, average, totalWithSatisfaction } ] }."""
    from collections import defaultdict
    by_period = defaultdict(lambda: {'sum': 0, 'count': 0})
    for issue in issues:
        created = _parse_iso_date((issue.get('fields') or {}).get('created'))
        if not created:
            continue
        pk = _period_key(created, by_month=by_month)
        if not pk:
            continue
        n = get_satisfaction_numeric(issue, field_ids)
        if n is not None:
            by_period[pk]['sum'] += n
            by_period[pk]['count'] += 1
    period_list = sorted(by_period.keys())
    return {
        'byPeriod': [
            {'period': p, 'average': round(by_period[p]['sum'] / by_period[p]['count'], 2), 'totalWithSatisfaction': by_period[p]['count']}
            for p in period_list
        ],
    }


def stats_csat_vs_nota(issues, field_ids, notas):
    """Pontos para scatter CSAT x Nota final. Retorna { 'points': [ { csat, nota, key } ] }."""
    points = []
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        csat = get_satisfaction_numeric(issue, field_ids)
        entry = (notas or {}).get(key)
        nota = entry.get('nota') if isinstance(entry, dict) else None
        if nota is not None:
            try:
                nota = int(nota)
            except (TypeError, ValueError):
                nota = None
        if csat is not None and nota is not None and 1 <= nota <= 5:
            points.append({'csat': csat, 'nota': nota, 'key': key})
    return {'points': points}


def stats_csat_by_request_type(issues, field_ids):
    """CSAT por Request Type. Retorna { 'byRequestType': { rt: { average, total, byStar } } }."""
    from collections import defaultdict
    by_rt = defaultdict(lambda: {'sum': 0, 'count': 0, 'byStar': {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}})
    for issue in issues:
        row = get_row_values(issue, field_ids)
        rt = (row.get('Request Type') or '').strip() or '(sem tipo)'
        n = get_satisfaction_numeric(issue, field_ids)
        if n is not None:
            by_rt[rt]['sum'] += n
            by_rt[rt]['count'] += 1
            by_rt[rt]['byStar'][n] = by_rt[rt]['byStar'].get(n, 0) + 1
    out = {}
    for rt, d in by_rt.items():
        if d['count'] > 0:
            out[rt] = {
                'average': round(d['sum'] / d['count'], 2),
                'total': d['count'],
                'byStar': d['byStar'],
            }
    return {'byRequestType': out}


def stats_volume_by_period(issues, by_month=False):
    """Tickets por período (semana ou mês). Retorna { 'byPeriod': [ { period, count } ] }."""
    from collections import defaultdict
    by_period = defaultdict(int)
    for issue in issues:
        created = _parse_iso_date((issue.get('fields') or {}).get('created'))
        if not created:
            continue
        pk = _period_key(created, by_month=by_month)
        if pk:
            by_period[pk] += 1
    period_list = sorted(by_period.keys())
    return {'byPeriod': [{'period': p, 'count': by_period[p]} for p in period_list]}


def stats_volume_by_analyst(issues):
    """Contagem de tickets por analista (assignee). Retorna { 'byAnalyst': [ { assignee, count } ] }."""
    from collections import defaultdict
    by_analyst = defaultdict(int)
    for issue in issues:
        assignee = (issue.get('fields') or {}).get('assignee') or {}
        name = assignee.get('displayName', '') if isinstance(assignee, dict) else (str(assignee) or 'Unassigned')
        if not name:
            name = 'Unassigned'
        by_analyst[name] += 1
    list_analysts = [{'assignee': k, 'count': v} for k, v in by_analyst.items()]
    list_analysts.sort(key=lambda x: -x['count'])
    return {'byAnalyst': list_analysts}


def stats_sla_pct_by_period(issues, sla_by_key, by_month=False):
    """% de tickets dentro do SLA (TTR + FRT) por período. Retorna { 'byPeriod': [ { period, pctWithinSla, total, met } ] }."""
    from collections import defaultdict
    by_period = defaultdict(lambda: {'met': 0, 'total': 0})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        created = _parse_iso_date((issue.get('fields') or {}).get('created'))
        if not created:
            continue
        pk = _period_key(created, by_month=by_month)
        if not pk:
            continue
        slas = (sla_by_key or {}).get(key) or []
        if not slas:
            continue
        by_period[pk]['total'] += 1
        if all(s.get('met', True) for s in slas):
            by_period[pk]['met'] += 1
    period_list = sorted(by_period.keys())
    out = []
    for p in period_list:
        d = by_period[p]
        pct = round(100 * d['met'] / d['total'], 1) if d['total'] else None
        out.append({'period': p, 'pctWithinSla': pct, 'total': d['total'], 'met': d['met']})
    return {'byPeriod': out}


def stats_sla_by_analyst(issues, sla_by_key):
    """SLA por analista: met/total e %. Retorna { 'byAnalyst': [ { assignee, met, total, pct } ] }."""
    from collections import defaultdict
    by_analyst = defaultdict(lambda: {'met': 0, 'total': 0})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        slas = (sla_by_key or {}).get(key) or []
        if not slas:
            continue
        assignee = (issue.get('fields') or {}).get('assignee') or {}
        name = assignee.get('displayName', '') if isinstance(assignee, dict) else (str(assignee) or 'Unassigned')
        if not name:
            name = 'Unassigned'
        by_analyst[name]['total'] += 1
        if all(s.get('met', True) for s in slas):
            by_analyst[name]['met'] += 1
    out = []
    for name, d in by_analyst.items():
        if d['total'] > 0:
            out.append({
                'assignee': name,
                'met': d['met'],
                'total': d['total'],
                'pct': round(100 * d['met'] / d['total'], 1),
            })
    out.sort(key=lambda x: (-x['pct'], -x['total']))
    return {'byAnalyst': out}


def _get_request_type_from_issue(issue, field_ids):
    """Extrai Request Type do issue."""
    row = get_row_values(issue, field_ids)
    return (row.get('Request Type') or '').strip() or '(sem tipo)'


def stats_nota_by_request_type(issues, notas, field_ids):
    """Nota média por Request Type. Retorna { 'byRequestType': { rt: { avgNota, count } } }."""
    from collections import defaultdict
    by_rt = defaultdict(lambda: {'sum': 0, 'count': 0})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        entry = (notas or {}).get(key)
        nota = entry.get('nota') if isinstance(entry, dict) else None
        if nota is None:
            continue
        try:
            n = int(nota)
        except (TypeError, ValueError):
            continue
        if n < 1 or n > 5:
            continue
        rt = _get_request_type_from_issue(issue, field_ids)
        by_rt[rt]['sum'] += n
        by_rt[rt]['count'] += 1
    out = {}
    for rt, d in by_rt.items():
        if d['count'] > 0:
            out[rt] = {'avgNota': round(d['sum'] / d['count'], 2), 'count': d['count']}
    return {'byRequestType': out}


def stats_sla_by_request_type(issues, sla_by_key, field_ids):
    """% dentro do SLA por Request Type. Retorna { 'byRequestType': { rt: { met, total, pct } } }."""
    from collections import defaultdict
    by_rt = defaultdict(lambda: {'met': 0, 'total': 0})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        slas = (sla_by_key or {}).get(key) or []
        if not slas:
            continue
        rt = _get_request_type_from_issue(issue, field_ids)
        by_rt[rt]['total'] += 1
        if all(s.get('met', True) for s in slas):
            by_rt[rt]['met'] += 1
    out = {}
    for rt, d in by_rt.items():
        if d['total'] > 0:
            out[rt] = {'met': d['met'], 'total': d['total'], 'pct': round(100 * d['met'] / d['total'], 1)}
    return {'byRequestType': out}


def stats_ttr_frt_by_request_type_from_sla(issues, sla_by_key, field_ids):
    """
    Tempo médio de resolução e de 1ª resposta (em horas) por Request Type usando APENAS a coluna SLA do modo lista.
    Fonte: sla_by_key (mesma da coluna SLAs). Sem fallback para campos do issue.
    Retorna { 'byRequestType': { rt: { count, avgResolutionHours, avgFirstResponseHours } }, 'requestTypeList': [...] }.
    """
    from collections import defaultdict
    by_rt = defaultdict(lambda: {'count': 0, 'resolution_seconds': [], 'first_response_seconds': []})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        rt = _get_request_type_from_issue(issue, field_ids)
        by_rt[rt]['count'] += 1
        slas = (sla_by_key or {}).get(key) or []
        for s in slas:
            sec = s.get('duration_seconds')
            if sec is None or sec < 0:
                continue
            tipo = s.get('tipo')
            if tipo == 'TTR':
                by_rt[rt]['resolution_seconds'].append(sec)
            elif tipo == 'FRT':
                by_rt[rt]['first_response_seconds'].append(sec)
    request_type_list = sorted(by_rt.keys())
    out = {}
    for rt in request_type_list:
        r = by_rt[rt]
        res_sec = r['resolution_seconds']
        fr_sec = r['first_response_seconds']
        avg_res_h = round(sum(res_sec) / len(res_sec) / 3600, 2) if res_sec else None
        avg_fr_h = round(sum(fr_sec) / len(fr_sec) / 3600, 2) if fr_sec else None
        out[rt] = {
            'count': r['count'],
            'avgResolutionHours': avg_res_h,
            'avgFirstResponseHours': avg_fr_h,
        }
    return {'byRequestType': out, 'requestTypeList': request_type_list}


def stats_critical_pct_by_period(issues, field_ids, by_month=False):
    """% de tickets críticos por período com base na coluna Satisfaction do Jira (1 ou 2 = crítico).
    Retorna { 'byPeriod': [ { period, pctCritical, count, total, keys } ] } (keys = lista de issue_key com Satisfaction 1 ou 2 no período)."""
    from collections import defaultdict
    by_period = defaultdict(lambda: {'critical': 0, 'total': 0, 'keys': []})
    for issue in issues:
        key = issue.get('key')
        if not key:
            continue
        n = get_satisfaction_numeric(issue, field_ids or {})
        if n is None:
            continue
        created = _parse_iso_date((issue.get('fields') or {}).get('created'))
        if not created:
            continue
        pk = _period_key(created, by_month=by_month)
        if not pk:
            continue
        by_period[pk]['total'] += 1
        if n <= 2:
            by_period[pk]['critical'] += 1
            by_period[pk]['keys'].append(key)
    period_list = sorted(by_period.keys())
    out = []
    for p in period_list:
        d = by_period[p]
        pct = round(100 * d['critical'] / d['total'], 1) if d['total'] else None
        out.append({
            'period': p, 'pctCritical': pct, 'count': d['critical'], 'total': d['total'],
            'keys': d.get('keys', []),
        })
    return {'byPeriod': out}


def get_first_comment_created_seconds(auth, issue_key, created_iso):
    """
    Fallback: calcula tempo até primeira resposta como (primeiro comentário - created).
    Retorna segundos ou None se não houver comentário ou em erro.
    """
    if not auth or not issue_key or not created_iso:
        return None
    created_dt = _parse_iso_date(created_iso)
    if not created_dt:
        return None
    try:
        r = requests.get(
            f'{JIRA_URL}/rest/api/3/issue/{issue_key}/comment',
            auth=auth,
            headers={'Accept': 'application/json'},
            params={'maxResults': 50, 'orderBy': 'created'},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        comments = data.get('comments') or data.get('values') or []
        if not comments:
            return None
        first = min(comments, key=lambda c: c.get('created') or '')
        first_created = first.get('created')
        if not first_created:
            return None
        first_dt = _parse_iso_date(first_created)
        if not first_dt:
            return None
        delta = (first_dt - created_dt).total_seconds()
        return int(delta) if delta >= 0 else None
    except Exception:
        return None


def fetch_issue_comments_text(auth, issue_key, max_comments=20):
    """
    Busca comentários do issue no Jira e retorna texto formatado (autor, data, corpo em texto plano).
    Usado para avaliação de notas (respostas do Assignee). Retorna string vazia se erro ou sem comentários.
    """
    if not auth or not issue_key:
        return ''
    try:
        r = requests.get(
            f'{JIRA_URL}/rest/api/3/issue/{issue_key}/comment',
            auth=auth,
            headers={'Accept': 'application/json'},
            params={'maxResults': max_comments, 'orderBy': 'created'},
            timeout=15,
        )
        if r.status_code != 200:
            return ''
        data = r.json()
        comments = data.get('comments') or data.get('values') or []
        if not comments:
            return ''
        lines = []
        for c in comments[:max_comments]:
            author = (c.get('author') or {}).get('displayName') or (c.get('author') or {}).get('name') or '—'
            created = (c.get('created') or '')[:19]
            body = _description_to_plain_text(c.get('body'))
            lines.append(f'[{created}] {author}: {body[:500]}')
        return '\n\n'.join(lines).strip()[:4000]
    except Exception:
        return ''


def stats_by_request_type(issues, field_ids, auth=None):
    """
    Agrega por Request Type: contagem, tempo médio de resolução e de primeira resposta (em horas).
    Usa o campo Time to first response do Jira; quando vazio, usa primeiro comentário (created → 1º comentário) para preencher o gráfico.
    Retorna { 'byRequestType': { rt: { count, avgResolutionHours, avgFirstResponseHours } }, 'requestTypeList': [...] }.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    by_rt = {}
    need_fallback = []
    for issue in issues:
        row = get_row_values(issue, field_ids)
        rt = (row.get('Request Type') or '').strip() or '(sem tipo)'
        if rt not in by_rt:
            by_rt[rt] = {'count': 0, 'resolution_seconds': [], 'first_response_seconds': []}
        by_rt[rt]['count'] += 1
        fields = issue.get('fields', {})
        created = _parse_iso_date(fields.get('created'))
        resolved = _parse_iso_date(fields.get('resolutiondate'))
        if created and resolved:
            by_rt[rt]['resolution_seconds'].append(int((resolved - created).total_seconds()))
        sec = None
        for fid in (field_ids.get('Time to first response'), NUBANK_TIME_TO_FIRST_RESPONSE_FID):
            if not fid:
                continue
            val = fields.get(fid)
            sec = _duration_to_seconds(val)
            if sec is not None and sec >= 0:
                break
        if sec is not None and sec >= 0:
            by_rt[rt]['first_response_seconds'].append(sec)
        elif auth:
            need_fallback.append((issue, rt, fields.get('created'), issue.get('key')))
    max_fallback = 30
    if need_fallback and auth:
        need_fallback = need_fallback[:max_fallback]
        def one(item):
            _issue, _rt, created_iso, key = item
            s = get_first_comment_created_seconds(auth, key, created_iso)
            return _rt, s
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(one, item): item for item in need_fallback}
            for future in as_completed(futures):
                try:
                    rt, sec = future.result()
                    if sec is not None and sec >= 0 and rt in by_rt:
                        by_rt[rt]['first_response_seconds'].append(sec)
                except Exception:
                    pass
    request_type_list = sorted(by_rt.keys())
    out = {}
    for rt in request_type_list:
        r = by_rt[rt]
        res_sec = r['resolution_seconds']
        fr_sec = r['first_response_seconds']
        avg_res_h = round(sum(res_sec) / len(res_sec) / 3600, 2) if res_sec else None
        avg_fr_h = round(sum(fr_sec) / len(fr_sec) / 3600, 2) if fr_sec else None
        out[rt] = {
            'count': r['count'],
            'avgResolutionHours': avg_res_h,
            'avgFirstResponseHours': avg_fr_h,
        }
    return {'byRequestType': out, 'requestTypeList': request_type_list}


def _normalize_for_keyword(s):
    """Remove acentos e deixa minúsculo para matching de palavras-chave."""
    if not s:
        return ''
    s = (s or '').lower().strip()
    import unicodedata
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


# Subcategorias por palavras-chave no ticket (título + descrição). Primeira que bater define a subcategoria.
OTHER_KEYWORD_CATEGORIES = [
    # Impressora somente se tiver imprimir, impressora ou printer
    ('Impressora', ['imprimir', 'impressora', 'printer']),
    ('VPN', ['vpn']),
    ('Escopo', ['escopo', 'escope']),
    ('Wi-Fi', ['wifi', 'wi-fi']),
    ('Shuffle', ['shuffle']),
    ('Office 365', ['office365', 'office 365']),
    ('Okta', ['okta']),
    ('MFA / 2FA', ['mfa', '2fa']),
    ('Senha', ['senha', 'password']),
    ('App', ['app']),
    ('Slack', ['slack']),
    ('Backoffice', ['backoffice', 'back office']),
    ('Staging', ['staging']),
    ('Certificados', ['certificados', 'certificates', 'certificado', 'certificate']),
    ('AWS Groups', ['aws-groups', 'aws groups']),
    ('GitHub', ['github']),
    ('Acesso', ['acesso', 'access']),
    ('Configuração / Setup', ['configuração', 'configuracao', 'setup']),
    ('Conta desativada', ['conta desativada']),
    ('Wia', ['wia']),
    ('Drive', ['drive']),
    ('Bloqueio', ['bloqueio']),
    ('Pombão', ['pombão', 'pombao', 'pombāo']),
    ('Migração', ['migração', 'migracao', 'migraçao', 'migraçāo']),
]

# Label para chamados sem nenhuma palavra-chave encontrada na descrição (não usar fallback "Dúvida / Orientação").
NO_KEYWORD_MATCH_LABEL = 'Sem palavra-chave na descrição'


def _match_other_keyword(text_norm, keywords):
    """Retorna True se alguma keyword aparecer em text_norm (texto já normalizado)."""
    return any(kw in text_norm for kw in keywords)


def _match_other_keyword_second_pass(words_norm, keywords):
    """
    Segundo passo: para cada palavra (ex: 'acessar'), verifica se alguma keyword
    está contida nela ou ela está contida na keyword (ex: 'acesso' em 'acessar').
    Assim pegamos variações como acessar, impressão, instalando.
    """
    for w in words_norm:
        if len(w) < 3:
            continue
        for kw in keywords:
            if len(kw) < 3:
                continue
            if kw in w or w in kw:
                return True
    return False


def stats_other_by_keywords(issues, field_ids):
    """
    Filtra issues com Request Type "Other" (ou "Others") e agrupa por palavras-chave na descrição.
    Análise em dois passos: (1) match exato de termos; (2) para os não classificados, match por
    palavras (substring / variações) para reduzir "Outros (sem palavra-chave)".
    Retorna { 'otherByKeyword': { label: count }, 'otherKeywordList': [labels] }.
    """
    other_issues = []
    for issue in issues:
        row = get_row_values(issue, field_ids)
        rt = (row.get('Request Type') or '').strip()
        if not rt:
            continue
        rt_lower = rt.lower()
        if rt_lower not in ('other', 'others', 'outro', 'outros'):
            continue
        other_issues.append(issue)
    if not other_issues:
        return {'otherByKeyword': {}, 'otherKeywordList': []}
    by_keyword = {}
    for label, keywords in OTHER_KEYWORD_CATEGORIES:
        by_keyword[label] = 0
    by_keyword[NO_KEYWORD_MATCH_LABEL] = 0
    for issue in other_issues:
        _, desc_plain = get_issue_summary_and_description(issue, field_ids)
        text = (desc_plain or '').strip()
        text_norm = _normalize_for_keyword(text)
        assigned = False
        for label, keywords in OTHER_KEYWORD_CATEGORIES:
            if _match_other_keyword(text_norm, [_normalize_for_keyword(kw) for kw in keywords]):
                by_keyword[label] += 1
                assigned = True
                break
        if not assigned:
            words_norm = [w for w in text_norm.replace('.', ' ').replace(',', ' ').replace(';', ' ').split() if len(w) >= 3]
            for label, keywords in OTHER_KEYWORD_CATEGORIES:
                kws_norm = [_normalize_for_keyword(kw) for kw in keywords]
                if _match_other_keyword_second_pass(words_norm, kws_norm):
                    by_keyword[label] += 1
                    assigned = True
                    break
        if not assigned:
            by_keyword[NO_KEYWORD_MATCH_LABEL] += 1
    keyword_list = [label for label, _ in OTHER_KEYWORD_CATEGORIES if by_keyword.get(label, 0) > 0]
    if by_keyword.get(NO_KEYWORD_MATCH_LABEL, 0) > 0:
        keyword_list.append(NO_KEYWORD_MATCH_LABEL)
    return {'otherByKeyword': by_keyword, 'otherKeywordList': keyword_list}


def _classify_issue_by_keywords(issue, field_ids):
    """Classifica um issue em uma subcategoria por palavras-chave na descrição. Sem fallback: sem match retorna NO_KEYWORD_MATCH_LABEL."""
    _, desc_plain = get_issue_summary_and_description(issue, field_ids)
    text = (desc_plain or '').strip()
    text_norm = _normalize_for_keyword(text)
    for label, keywords in OTHER_KEYWORD_CATEGORIES:
        if _match_other_keyword(text_norm, [_normalize_for_keyword(kw) for kw in keywords]):
            return label
    words_norm = [w for w in text_norm.replace('.', ' ').replace(',', ' ').replace(';', ' ').split() if len(w) >= 3]
    for label, keywords in OTHER_KEYWORD_CATEGORIES:
        kws_norm = [_normalize_for_keyword(kw) for kw in keywords]
        if _match_other_keyword_second_pass(words_norm, kws_norm):
            return label
    return NO_KEYWORD_MATCH_LABEL


def stats_keyword_breakdown_by_request_type(issues, field_ids):
    """
    Para cada Request Type, agrupa os chamados por subcategoria (palavras-chave na descrição).
    Sem dados de fallback: não classificados por palavra-chave ficam em NO_KEYWORD_MATCH_LABEL.
    Retorna { 'keywordBreakdownByRequestType': { rt: { byKeyword: {...}, keywordList: [...] } } }.
    """
    by_rt = {}
    for issue in issues:
        row = get_row_values(issue, field_ids)
        rt = (row.get('Request Type') or '').strip() or '(sem tipo)'
        if rt not in by_rt:
            by_rt[rt] = []
        by_rt[rt].append(issue)
    result = {}
    for rt, rt_issues in by_rt.items():
        by_keyword = {}
        keys_by_keyword = {}
        for label, _ in OTHER_KEYWORD_CATEGORIES:
            by_keyword[label] = 0
            keys_by_keyword[label] = []
        by_keyword[NO_KEYWORD_MATCH_LABEL] = 0
        keys_by_keyword[NO_KEYWORD_MATCH_LABEL] = []
        for issue in rt_issues:
            key = issue.get('key') or ''
            label = _classify_issue_by_keywords(issue, field_ids)
            by_keyword[label] = by_keyword.get(label, 0) + 1
            if key:
                keys_by_keyword.setdefault(label, []).append(key)
        keyword_list = [label for label, _ in OTHER_KEYWORD_CATEGORIES if by_keyword.get(label, 0) > 0]
        if by_keyword.get(NO_KEYWORD_MATCH_LABEL, 0) > 0:
            keyword_list.append(NO_KEYWORD_MATCH_LABEL)
        result[rt] = {'byKeyword': by_keyword, 'keywordList': keyword_list, 'keysByKeyword': keys_by_keyword}
    return {'keywordBreakdownByRequestType': result}


def _subcategory_labels():
    """Lista de labels de subcategoria para o Ollama (mesma ordem que OTHER_KEYWORD_CATEGORIES + NO_KEYWORD_MATCH_LABEL)."""
    return [label for label, _ in OTHER_KEYWORD_CATEGORIES] + [NO_KEYWORD_MATCH_LABEL]


def get_issue_subcategory_from_ollama(issue, ollama_url, model=None, field_ids=None):
    """
    Classifica o chamado em uma subcategoria usando Ollama com a descrição do ticket (reflete melhor o caso real).
    Usa apenas título e descrição do ticket. Retorna um dos labels de _subcategory_labels() ou NO_KEYWORD_MATCH_LABEL em falha.
    """
    if not ollama_url or not ollama_url.strip():
        return NO_KEYWORD_MATCH_LABEL
    base = ollama_url.strip().rstrip('/')
    model = (model or 'llama3.2').strip()
    labels = _subcategory_labels()
    summary_plain, desc_plain = get_issue_summary_and_description(issue, field_ids)
    title = (summary_plain or '').strip() or '(sem título)'
    desc = (desc_plain or '').strip() or ''
    context = 'Título: ' + title + '\n\nDescrição do ticket:\n' + desc
    context = context[:4000]
    prompt = (
        'Responda em português do Brasil (pt-BR), com gramática e conjugação corretas. '
        'Classifique o chamado de suporte abaixo em UMA das subcategorias, com base na descrição do ticket. '
        'Responda APENAS com o nome exato da subcategoria, sem explicação nem ponto final.\n\n'
        'Subcategorias: ' + ', '.join(labels) + '\n\n' + context
    )
    def _parse_content(data, use_chat=False):
        if use_chat:
            msg = data.get('message') or {}
            return (msg.get('content') or '').strip()
        return (data.get('response') or '').strip()

    try:
        content = ''
        # Ordem: /api/chat (Ollama recente), depois /api/generate
        for use_chat, url in [(True, f'{base}/api/chat'), (False, f'{base}/api/generate')]:
            try:
                if use_chat:
                    r = requests.post(
                        url,
                        json={
                            'model': model,
                            'messages': [{'role': 'user', 'content': prompt}],
                            'stream': False,
                            'options': {'temperature': 0},
                        },
                        timeout=90,
                    )
                else:
                    r = requests.post(
                        url,
                        json={'model': model, 'prompt': prompt, 'stream': False},
                        timeout=90,
                    )
                if r.status_code == 200:
                    data = r.json()
                    content = _parse_content(data, use_chat)
                    if content:
                        break
                if r.status_code != 404:
                    break
            except Exception:
                break
        if not content:
            return NO_KEYWORD_MATCH_LABEL
        first_line = content.split('\n')[0].strip().rstrip('.;')
        for label in labels:
            if first_line == label or first_line in label or label in first_line:
                return label
        if first_line and len(first_line) > 2:
            for label in labels:
                if first_line.lower() == label.lower():
                    return label
        return NO_KEYWORD_MATCH_LABEL
    except Exception:
        return NO_KEYWORD_MATCH_LABEL


def stats_keyword_breakdown_by_request_type_ollama(issues, field_ids, ollama_url, model=None):
    """
    Mesma estrutura que stats_keyword_breakdown_by_request_type, mas classifica cada chamado com Ollama
    (reflete melhor o caso real que palavras-chave).
    """
    if not ollama_url or not ollama_url.strip():
        return stats_keyword_breakdown_by_request_type(issues, field_ids)
    by_rt = {}
    for issue in issues:
        row = get_row_values(issue, field_ids)
        rt = (row.get('Request Type') or '').strip() or '(sem tipo)'
        if rt not in by_rt:
            by_rt[rt] = []
        by_rt[rt].append(issue)
    result = {}
    labels_all = _subcategory_labels()
    for rt, rt_issues in by_rt.items():
        by_keyword = {label: 0 for label in labels_all}
        keys_by_keyword = {label: [] for label in labels_all}
        for issue in rt_issues:
            key = issue.get('key') or ''
            label = get_issue_subcategory_from_ollama(issue, ollama_url, model, field_ids)
            if label not in by_keyword:
                by_keyword[label] = 0
                keys_by_keyword[label] = []
            by_keyword[label] = by_keyword.get(label, 0) + 1
            if key:
                keys_by_keyword.setdefault(label, []).append(key)
        keyword_list = [label for label in labels_all if by_keyword.get(label, 0) > 0]
        result[rt] = {'byKeyword': by_keyword, 'keywordList': keyword_list, 'keysByKeyword': keys_by_keyword}
    return {'keywordBreakdownByRequestType': result}


def print_table(issues, field_ids, include_key=True):
    """Print a text table to stdout."""
    rows = [get_row_values(iss, field_ids) for iss in issues]
    cols = ['key'] + COLUMNS if include_key else COLUMNS
    widths = {c: max(len(str(c)), max((len(str(r.get(c, ''))) for r in rows), default=0)) for c in cols}
    widths = {c: min(w, 50) for c, w in widths.items()}

    sep = '-' * (sum(widths.values()) + len(cols) - 1)
    header = ' | '.join(str(c)[:widths[c]].ljust(widths[c]) for c in cols)
    print(sep)
    print(header)
    print(sep)
    for r in rows:
        line = ' | '.join(str(r.get(c, ''))[:widths[c]].ljust(widths[c]) for c in cols)
        print(line)
    print(sep)
    print(f'Total: {len(issues)} issues')


def write_html(issues, field_ids, output_path, jql):
    """Write an HTML dashboard file."""
    rows = [get_row_values(iss, field_ids) for iss in issues]
    base_url = JIRA_URL.rstrip('/')
    key_link = lambda k: f'<a href="{base_url}/browse/{k}" target="_blank">{k}</a>'

    html_rows = []
    for r in rows:
        cells = [f'<td>{key_link(r["key"])}</td>']
        for c in COLUMNS:
            cells.append(f'<td>{r.get(c, "")}</td>')
        html_rows.append('<tr>' + ''.join(cells) + '</tr>')

    table_body = '\n'.join(html_rows)
    cols_header = '<th>Key</th>' + ''.join(f'<th>{c}</th>' for c in COLUMNS)

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>L1 Dashboard - Jira</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 20px; background: #f5f5f5; }}
    h1 {{ color: #172b4d; }}
    .jql {{ background: #fff; padding: 10px; border-radius: 6px; font-size: 12px; color: #505f79; margin-bottom: 16px; word-break: break-all; }}
    table {{ border-collapse: collapse; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }}
    th {{ background: #0052cc; color: #fff; padding: 12px 16px; text-align: left; font-weight: 600; }}
    td {{ padding: 10px 16px; border-bottom: 1px solid #dfe1e6; }}
    tr:hover {{ background: #f4f5f7; }}
    a {{ color: #0052cc; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .count {{ margin-top: 12px; color: #505f79; }}
  </style>
</head>
<body>
  <h1>L1 Dashboard</h1>
  <div class="jql"><strong>JQL:</strong> {jql}</div>
  <table>
    <thead><tr>{cols_header}</tr></thead>
    <tbody>
{table_body}
    </tbody>
  </table>
  <p class="count">Total: {len(issues)} issues</p>
</body>
</html>
'''
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'Dashboard saved to {output_path}', file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='L1 Jira dashboard - Reporter, Time to resolution, Time to first response, Assignee, Request Type')
    parser.add_argument('--jql', default=DEFAULT_JQL, help='JQL query (default: L1 filter + order by Time to resolution)')
    parser.add_argument('--limit', type=int, default=None, help='Max issues to fetch')
    parser.add_argument('--html', metavar='FILE', default=None, help='Also write HTML dashboard to FILE')
    parser.add_argument('--no-key', action='store_true', help='Hide Key column in text output')
    parser.add_argument('--list-fields', metavar='SEARCH', nargs='?', const='', default=None,
                        help='List all Jira field names (and IDs). Optional SEARCH filters by name (e.g. "resolution", "Support")')
    args = parser.parse_args()

    try:
        email, api_token = get_jira_credentials()
    except Exception as e:
        print(f'Credentials error: {e}', file=sys.stderr)
        print('Set JIRA_EMAIL and JIRA_API_TOKEN (or use .env).', file=sys.stderr)
        sys.exit(1)
    auth = (email, api_token)

    if args.list_fields is not None:
        search = args.list_fields if args.list_fields != '' else None
        rows = list_all_fields(auth, search=search)
        print(f'Fields from Jira (total: {len(rows)})', file=sys.stderr)
        print('ID\t\t\tName')
        print('-' * 60)
        for fid, fname in rows:
            print(f'{fid}\t{fname}')
        return

    jql = args.jql.strip()
    if jql.upper().startswith('AND '):
        jql = jql[4:].strip()

    print('Resolving custom fields...', file=sys.stderr)
    field_ids = resolve_custom_fields(auth)
    missing = [n for n, fid in field_ids.items() if fid is None]
    if missing:
        print(f'Warning: could not resolve field names (will show empty): {missing}', file=sys.stderr)
        print('Tip: run  python l1_dashboard.py --list-fields  to see exact field names in your Jira.', file=sys.stderr)

    print('Running JQL search...', file=sys.stderr)
    issues = search_jql(auth, jql, field_ids, limit=args.limit)
    print(f'Found {len(issues)} issues.', file=sys.stderr)
    if len(issues) == 0:
        print('Tip: your Jira has only standard fields (no "Support Level - ITOPS"). Try:  --jql "order by created DESC"  or  --jql "order by resolutiondate ASC"  to list issues.', file=sys.stderr)

    print_table(issues, field_ids, include_key=not args.no_key)

    if args.html:
        write_html(issues, field_ids, args.html, jql)


if __name__ == '__main__':
    main()
