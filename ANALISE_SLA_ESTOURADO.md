# Por que os tickets aparecem como “estourados” por analista

Esta análise justifica como o L1 Dashboard determina que um chamado está **estourado** (fora do SLA) e como isso é usado no **feedback mensal por analista**.

---

## 1. Fonte dos dados: API do Jira

- Na **busca**, o app chama a API do Jira Service Management para cada issue retornada:
  - **Endpoint:** `GET /rest/servicedeskapi/request/{issueKey}/sla`
- A resposta traz as métricas de SLA do chamado (nome, ciclos completados, ciclo em andamento, prazos, etc.).
- Não há fallback próprio de “estourado”: o que vale é o retorno da API do Jira. O app apenas **interpreta** esse retorno.

---

## 2. Como o Jira indica “estourado” (breached)

Para **cada** métrica de SLA retornada pela API, o app lê o flag de estouro nesta ordem:

1. **Último ciclo completado**  
   `completedCycles[-1].breached`  
   → Se o último ciclo já encerrado estourou, a métrica é considerada estourada.

2. **Ciclo em andamento** (se ainda não há ciclo completado ou não estourou no completado)  
   `ongoingCycle.breached` ou `ongoingCycle.hasFailed`  
   → Se o ciclo atual está estourado/falhou, a métrica é considerada estourada.

3. **Nível do item**  
   `item.breached` ou `item.hasFailed`  
   → Fallback na própria métrica.

Internamente o app guarda: **`met = not breached`**  
- `met = False` → estourado → na interface aparece o **X vermelho** na coluna SLAs.  
- `met = True` → cumprido → aparece **✓**.

Ou seja: **um chamado aparece como estourado quando o Jira devolve `breached = true` (ou equivalente) para pelo menos uma métrica de SLA considerada relevante.**

---

## 3. Quais SLAs entram na conta de “estourado”

Só contam para “chamado estourado” as métricas **relevantes** (TTR, FRT, Time to close):

- Time to close after resolution  
- Time to first response / First response / Primeira resposta  
- Time to resolution / Time to resolution / Resolução  

Outras métricas (ex.: “Awaiting for approval”, “In Progress - Waiting for support”) **não** entram na lógica de estouro do feedback.

**Regra no código:**  
Um chamado é considerado **estourado** se **pelo menos um** SLA relevante tiver **`met = False`** (ou seja, `breached = true` no Jira).  
Isso é exatamente o mesmo critério usado para mostrar o **X vermelho** na coluna SLAs do modo lista.

---

## 4. Por analista no feedback mensal

- Para cada **analista** (Assignee), o app:
  - Pega todos os chamados desse analista no resultado da busca.
  - Para cada chamado, usa o **mesmo** `sla_by_key` da busca (mesmos dados da coluna SLAs do modo lista).
  - Conta quantos chamados têm pelo menos um SLA relevante estourado (`_issue_sla_breached` = True).

Resultado:

- **“N chamados estourados”** do analista = chamados em que a **API do Jira** indicou estouro em pelo menos uma métrica relevante (TTR, FRT ou Time to close).
- **“Muitos estourados”** = quando esse analista tem **mais** chamados estourados do que cumpridos (`estourados > cumpridos`). Só nesse caso o feedback mensal cita prazos como ponto a melhorar e pode listar as keys.

Não há segunda fonte de SLA: é sempre o mesmo `sla_by_key` da busca, que vem 100% da API do Jira.

---

## 5. Resumo (justificativa)

| Pergunta | Resposta |
|----------|----------|
| De onde vem “estourado”? | Da API do Jira (`/rest/servicedeskapi/request/{key}/sla`). O app não calcula prazos por conta própria. |
| O que é “estourado” por métrica? | `breached = true` (ou `hasFailed`) no ciclo completado ou em andamento. No app: `met = False` → X na lista. |
| Quais SLAs contam? | Apenas TTR, FRT e Time to close after resolution (e variações de nome em EN/PT). |
| Por que o ticket está estourado? | Porque o Jira retornou pelo menos uma métrica relevante com `breached = true`. |
| Por que o analista tem X estourados? | Porque X dos seus chamados, na resposta da API do Jira, têm pelo menos um SLA relevante estourado. |

Assim, **os tickets aparecem como estourados por analista porque a API do Jira indica estouro para pelo menos um SLA relevante (TTR, FRT ou Time to close) nesses chamados**; o dashboard apenas reflete e agrupa essa informação por analista.
