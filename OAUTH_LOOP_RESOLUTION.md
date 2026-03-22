# Resolução: OAuth2 Auto-Login Loop em Liquidation

## Problema Reportado
A função `liquidation.py` estava disparando a janela de autenticação Google (OAuth2) repetidamente durante execução, causando múltiplas tentativas de auto-login.

## Causa Raiz Identificada
1. **Session Validation Cascade**: Durante a extração via `_fetch_page_http()`, cada tentativa de requisição HTTP que recebia 401/retcode inválido disparava `SessionExpiredError`
2. **Auto-Login Handler**: A exceção era capturada por `core/session.py` que automaticamente acionava `refresh_session()` (OAuth2 Google)
3. **Repeated Calls**: Múltiplas páginas/tentativas causavam múltiplas cascatas de auto-login

## Soluções Implementadas

### 1. **Session Validation Before Extraction** (linha ~605 em `run()`)
```python
# Validação única no início, antes de processar chunks
try:
    test_params = {
        "reason_id": LIQUIDATION["reason_id"],
        "pageno": 1,
        "count": 1,
    }
    test_result = session.get(LIQUIDATION["api_url"], params=test_params)
    
    if isinstance(test_result, dict) and test_result.get("retcode") == 0:
        console.print(f"[green]✅ Sessão validada e funcional[/green]")
    else:
        console.print(f"[yellow]⚠️ Sessão retornou retcode inválido...")
except SessionExpiredError:
    console.print(f"[yellow]⚠️ Sessão expirada durante validação[/yellow]")
```

**Benefício**: Testa sessão **uma única vez** no início; se expirar durante validação, o handler já foi disparado e não se repete.

### 2. **Graceful SessionExpiredError Handling em `_fetch_page_http()` (commit fe3170a)**
```python
except SessionExpiredError:
    # Não tenta auto-login aqui; deixa para fallback Selenium
    console.print(f"[dim]  Página {page}: sessão expirada, usando fallback[/dim]")
    return None
```

**Benefício**: Não redireciona cascata para auto-login durante extração; simplesmente retorna `None` para triggage fallback Selenium.

### 3. **HTTP→Selenium Fallback Architecture** (já existente)
```
Para cada página:
  1. Try HTTP direto (rápido: 50-100ms) → sucesso ✅
  2. If HTTP fails → ativa Selenium (mais robusto: 500-2000ms)
  3. Log qual método foi usado
```

**Benefício**: HTTP para performance; Selenium para robustez — sem repetidas tentativas de auto-login.

## Commits Relacionados

| Commit | Descrição |
|--------|-----------|
| `fe3170a` | Improve _fetch_page_http() SessionExpiredError handling |
| `2b8e344` | Add HTTP fallback strategy to Liquidation module |
| `207d7b1` | Update GitHub Actions to support Node.js 24 |
| `25d9b14` | Parallelize Recebimento SOC page fetching |

## Como Testar

### Opção 1: Teste Simples (Recomendado ⭐)
```bash
cd c:\PROJETOS\Auto-EDA-SPX
python test_liquidation_simple.py
```

Escolher opção **1** (HTTP Direto) ou **3** (HTTP→Selenium fallback).

**Esperado**: Nenhuma janela de browser deve aparecer múltiplas vezes.

### Opção 2: Teste Completo com Menu
```bash
cd c:\PROJETOS\Auto-EDA-SPX
python test_liquidation_manual.py
```

Escolher opção **1** ou **2** (HTTP).

### Opção 3: Execução Normal (CI/Scheduler)
```bash
cd c:\PROJETOS\Auto-EDA-SPX
python main.py --liquidation
```

## Validação de Sucesso ✅

- ✅ Nenhuma janela OAuth2 apareça repetidamente
- ✅ HTTP direto funciona sem disparar auto-login
- ✅ Selenium fallback ativa apenas se HTTP falhar
- ✅ Extração completa com dados coletados corretamente
- ✅ Logs mostram qual método (HTTP ou Selenium) foi usado por página

## Configurações Relacionadas

### core/config.py
- `LIQUIDATION["api_url"]`: Endpoint SPX para ER48
- `LIQUIDATION["reason_id"]`: Filtro de razão (ER48 = liquidação resolvida)
- `LIQUIDATION["page_size"]`: Itens por página (padrão: 50)
- `LIQUIDATION["days_ago"]`: Dias históricos a coletar

### core/session.py
- `_session_lock`: `threading.RLock()` para thread-safety
- Auto-refresh dispara apenas em SessionExpiredError - agora prevenido por validação inicial

## Próximos Passos

1. **Executar teste** (`test_liquidation_simple.py`) para confirmar resolução
2. **Monitorar CI** nas próximas execuções programadas (30 min em GitHub Actions)
3. **Considerar**: Cache de sessão entre chunks para reduzir validações

---

**Status**: 🟢 IMPLEMENTADO E COMMITADO  
**Validação Pendente**: Execução de teste pelo usuário
