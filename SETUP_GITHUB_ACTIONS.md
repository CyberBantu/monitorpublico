# Configuração do GitHub Actions - Monitor Público

Este documento explica como configurar os secrets necessários para a pipeline de dados funcionar.

---

## Secrets Necessários

Você precisa configurar **3 secrets** no GitHub:

| Secret | Descrição |
|--------|-----------|
| `GCP_KEYFILE_JSON` | Chave JSON da Service Account do GCP |
| `EMAIL_USERNAME` | Email Gmail para enviar notificações |
| `EMAIL_PASSWORD` | Senha de App do Gmail |

---

## Passo a Passo

### 1. Configurar `GCP_KEYFILE_JSON`

1. Abra o arquivo `monitorpublico-8d9a6eee6448.json` (sua chave da Service Account)
2. Copie **todo o conteúdo** do arquivo JSON
3. No GitHub, vá para:
   - **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
4. Nome: `GCP_KEYFILE_JSON`
5. Valor: Cole o conteúdo do JSON
6. Clique em **Add secret**

---

### 2. Configurar Email para Notificações

Para enviar emails pelo Gmail, você precisa criar uma **Senha de App**:

#### 2.1 Ativar Verificação em 2 Etapas (se ainda não tiver)
1. Vá para: https://myaccount.google.com/security
2. Ative a **Verificação em duas etapas**

#### 2.2 Criar Senha de App
1. Vá para: https://myaccount.google.com/apppasswords
2. Selecione **Outro (nome personalizado)**
3. Digite: `GitHub Actions Monitor Publico`
4. Clique em **Gerar**
5. Copie a senha de 16 caracteres (ex: `xxxx xxxx xxxx xxxx`)

#### 2.3 Adicionar Secrets no GitHub

**EMAIL_USERNAME:**
- Nome: `EMAIL_USERNAME`
- Valor: `christianbasilio97@gmail.com` (ou o email que você quer usar para enviar)

**EMAIL_PASSWORD:**
- Nome: `EMAIL_PASSWORD`
- Valor: A senha de 16 caracteres gerada (sem espaços)

---

## Verificar Configuração

Depois de configurar os 3 secrets, você terá:

```
Repository secrets:
├── GCP_KEYFILE_JSON
├── EMAIL_USERNAME
└── EMAIL_PASSWORD
```

---

## Testar a Pipeline

### Opção 1: Push para main
Faça qualquer commit na branch `main` e a pipeline será executada automaticamente.

### Opção 2: Executar manualmente
1. Vá para **Actions** no GitHub
2. Selecione **Pipeline de Dados - Monitor Público**
3. Clique em **Run workflow**

---

## Fluxo da Pipeline

```
┌─────────────────────────────────────────────────────────┐
│                    TRIGGER                               │
│  Push para main / Schedule (17h UTC) / Manual           │
└─────────────────────┬───────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────────────┐
│              INGESTÃO E TRANSFORMAÇÃO                    │
├─────────────────────────────────────────────────────────┤
│  1. Checkout do código                                   │
│  2. Configurar Python 3.11                               │
│  3. Instalar dependências (requirements.txt)             │
│  4. Autenticar no Google Cloud                           │
│  5. Configurar profiles.yml do dbt                       │
│  6. Executar ingestão (ingest_queimados_despesas.py)     │
│  7. dbt debug (verificar conexão)                        │
│  8. dbt run (executar modelos)                           │
│  9. dbt test (executar testes)                           │
└─────────────────────┬───────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────────────┐
│                  NOTIFICAÇÃO                             │
├─────────────────────────────────────────────────────────┤
│  ✅ Sucesso → Email de confirmação                       │
│  ❌ Falha   → Email de alerta com link para logs        │
└─────────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### Erro de autenticação GCP
- Verifique se o JSON está completo (incluindo `{` e `}`)
- Verifique se não há espaços extras no início/fim

### Erro de envio de email
- Verifique se a Verificação em 2 Etapas está ativa
- Verifique se a Senha de App foi gerada corretamente
- A senha deve ter 16 caracteres sem espaços

### dbt debug falha
- Verifique se a Service Account tem permissão de BigQuery Data Editor
- Verifique se o projeto `monitorpublico` existe

---

## Segurança

⚠️ **IMPORTANTE:**
- Nunca commite arquivos `.json` com chaves privadas
- O arquivo `monitorpublico-*.json` está no `.gitignore`
- Secrets do GitHub são criptografados e seguros
