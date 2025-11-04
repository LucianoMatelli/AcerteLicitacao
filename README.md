# © Acerte Licitações — Uso interno. Não distribuir sem alinhamento prévio.

---

# 📑 Acerte Licitações — Buscador PNCP com Persistência

---

Aplicação **Streamlit** para monitoramento de editais públicos diretamente do **PNCP**, com filtros avançados, cards elegantes, controle de histórico e **persistência de estado via GitHub**.

## 🚀 Objetivo
Centralizar a prospecção de licitações de forma escalável, reduzindo retrabalho e qualificando rapidamente o que “vale analisar” versus “não atende”.

Fluxo:
1. Selecionar municípios-alvo (até 25).
2. Consultar a API oficial do PNCP filtrando status.
3. Visualizar os resultados em **cards** paginados.
4. Marcar o que já foi analisado (**TR Elaborado**) ou descartado (**Não Atende**).
5. Exportar para **XLSX**.

Persistência garantida mesmo após hibernação da aplicação.

---

## 🧠 Funcionalidades

### 🔎 Filtros (Sidebar)
- **Palavra-chave**: aplicada localmente (Título e Objeto) após a coleta.
- **Status** (mapeamento PNCP):
  - “A Receber/Recebendo Proposta” → `recebendo_proposta`
  - “Em Julgamento/Propostas Encerradas” → `em_julgamento`
  - “Encerradas” → `encerrado`
  - “Todos” → vazio (sem filtro)
- **Estado (UF)**: obrigatório para habilitar a seleção de municípios.
- **Municípios (máx. 25)**:
  - Lista por UF (catálogo **IBGE**).
  - Conversão automática para **código PNCP** via `ListaMunicipiosPNCP.csv`.
  - Exibição dos selecionados com botão `✕` para remover.
- **Salvar/Excluir pesquisa salva** + **Lista de pesquisas salvas**.

> UI em azul claro sutil, contraste adequado e botões principais com fundo azul escuro e fonte branca.

### 📄 Cards (em vez de tabela)
Cada edital aparece como card com:
- **Título** + badges manuais:
  - `TR Elaborado` (verde)
  - `Não Atende` (vermelho)
- **Cidade/UF**, **Publicação**, **Fim do envio**, **Objeto**
- **Modalidade / Tipo / Órgão**
- **Número do processo**
- **Abrir edital** (link preferencial):https://pncp.gov.br/app/editais/{cnpj_do_orgao}/{ano}/{numero_sequencial}
- (com fallback automático se necessário)

Estilo: fundo azul muito claro, borda suave, sombra discreta, cantos arredondados.

### ✅ Marcações com memória
- Dois checkboxes por card: **TR Elaborado** e **Não Atende**.
- A marcação **persiste entre sessões**:
- `tr_marks.json` e `na_marks.json` são versionados em `data/` do repositório.
- Identificador único (UID) por edital:
- Preferência: `{cnpj}-{ano}-{numero_sequencial}`
- Fallback: hash determinístico dos dados do card.

### ⬇️ Exportação
- Botão **“Baixar XLSX”** (remove colunas técnicas como `_pub_raw`, `_id`).
- Visual alinhado (fundo azul escuro, texto branco, tamanho reduzido).

### 🗂 Paginação
- Itens por página: **10 / 20 / 50**.
- Navegação **Anterior / Próxima** (topo e rodapé).
- Estado controlado por `st.session_state`.

---

## 🔌 Integração com PNCP
Endpoint consumido por município:
GET https://pncp.gov.br/api/search

?tipos_documento=edital
&ordenacao=-data
&pagina=<n>
&tam_pagina=100
&municipios=<CODIGO_PNCP>
[&status=<status>]
- Itera páginas até esgotar itens.
- Une resultados de todos os municípios selecionados.
- Palavra-chave aplicada **client-side** (não força busca textual remota).
- Ordenação final por **Publicação (desc)**.

---

## 🧩 IBGE → PNCP (conversão de município)
- `IBGE_Municipios.csv`: `UF` + `municipio` (catálogo humano).
- `ListaMunicipiosPNCP.csv`: `Municipio` + `id` (código PNCP).
- A aplicação normaliza strings e cruza ambos.
- Ao “Adicionar município”, resolve o **código PNCP** e salva:
  ```json
  {"codigo_pncp": "3721", "nome": "Itapetininga", "uf": "SP"}

## 🔐 Persistência (GitHub Contents API)

Para não perder estado quando a app hiberna, os dados são salvos no repositório:
- Arquivos em data/:
- saved_searches.json — pesquisas/filtros salvos.
- tr_marks.json — marcações de TR Elaborado.
- na_marks.json — marcações de Não Atende.

## Como funciona:
- Leitura: GET na API do GitHub, decodifica Base64 e carrega JSON.
- Escrita: PUT com commit automático (chore: atualizar ... via app).
- Secrets necessários (st.secrets):
- GITHUB_TOKEN = "ghp_xxxxxxxxxxxxxxxxx"
- GITHUB_REPO = "UsuarioOuOrg/NomeDoRepo"
- GITHUB_BRANCH = "main"
- GITHUB_BASEDIR = "data"

- O token precisa do escopo repo (write). Se main tiver proteção que bloqueia commits diretos, use outro branch em GITHUB_BRANCH.
- Fallback: se o PUT falhar (rate limit/permissão), a app salva localmente e emite warning.

## ▶️ Como usar

- Selecione UF.
- Escolha município e clique “➕ Adicionar município” (até 25).
- Defina Status e Palavra-chave.
- Clique Pesquisar.
- Nos cards, marque TR Elaborado / Não Atende conforme a triagem.
- Salve a pesquisa para reuso.
- Baixe o XLSX se precisar trabalhar offline.

## 🧱 Arquitetura

- app.py — UI (Streamlit), integração PNCP, SessionState, persistência GitHub.
- ListaMunicipiosPNCP.csv — mapeia Municipio → id (código PNCP).
- IBGE_Municipios.csv — catálogo UF + municipio.

## 📞 Suporte

- Ajustes de cores/layout: editar bloco <style> no app.py.
- Trocar branch/pasta de persistência: atualizar st.secrets.
- Atualizar catálogos: subir novos IBGE_Municipios.csv e ListaMunicipiosPNCP.csv.

## © Acerte Licitações — Uso interno. Não distribuir sem alinhamento prévio.
