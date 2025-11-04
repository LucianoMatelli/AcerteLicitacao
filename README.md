# 📑 Acerte Licitações — Buscador PNCP com Persistência

Aplicação Streamlit para monitoramento de editais públicos diretamente do PNCP, com filtros avançados, cards elegantes, controle de histórico e persistência de estado via GitHub.

---

## 🚀 Objetivo

Centralizar a prospecção de oportunidades de licitação de forma escalável e operacionalizável, reduzindo o retrabalho manual do time comercial/jurídico e qualificando rapidamente o que “vale analisar” versus o que é “sem interesse”.

O fluxo é:
1. Selecionar municípios-alvo (até 25 por vez).
2. Consultar a API oficial do PNCP filtrando status.
3. Visualizar os resultados em cards organizados e paginados.
4. Marcar o que já foi analisado ou descartado.
5. Exportar para XLSX.

Tudo isso com persistência de preferências e marcações — mesmo após o app "hibernar".

---

## 🧠 Principais Funcionalidades

### 🔎 Filtros (Sidebar)
- **Palavra-chave**  
  Aplicada localmente (client-side) sobre Título e Objeto após a coleta.
- **Status**  
  Opções:
  - “A Receber/Recebendo Proposta” → `recebendo_proposta`
  - “Em Julgamento/Propostas Encerradas” → `em_julgamento`
  - “Encerradas” → `encerrado`
  - “Todos” → envia vazio (sem filtro de status)
- **Estado (UF)**  
  Obrigatório. A seleção de municípios fica bloqueada até a UF ser informada.
- **Municípios (máx. 25)**  
  - Lista dos municípios daquela UF (catálogo IBGE).  
  - Ao adicionar, a aplicação converte o nome do município em seu **código PNCP interno**, usando `ListaMunicipiosPNCP.csv`.  
  - Os municípios selecionados aparecem abaixo, cada um com botão `✕` para remover.
- **Salvar / Excluir pesquisa salva**  
  - Você pode salvar um "pacote de filtros" com nome amigável (ex: “Interior-SP Saúde”).
  - Também é possível excluir pesquisas salvas.
- **Pesquisas salvas**  
  - É possível carregar rapidamente qualquer conjunto salvo.

> Importante: A barra lateral tem visual próprio (azul claro, bordas sutis), mantém contraste, e os botões principais usam fundo azul escuro e fonte branca.

---

### 📄 Exibição de Resultados
Os resultados NÃO aparecem em tabela crua. Cada edital vem como um **card premium**, com:

- **Título do edital**  
  Inclui selos (badges) de status manual:
  - `TR Elaborado` (verde)
  - `Não Atende` (vermelho)
- **Cidade / UF**
- **Data de Publicação**
- **Fim do envio de proposta**
- **Objeto**
- **Modalidade / Tipo / Órgão**
- **Número do processo**
- **Botão "Abrir edital"**  
  Link direto para o PNCP no formato preferencial:
  `https://pncp.gov.br/app/editais/{cnpj_do_orgao}/{ano}/{numero_sequencial}`  
  com fallback automático se esse padrão não estiver disponível.

Os cards usam:
- fundo azul muito claro
- borda suave
- sombra discreta
- cantos arredondados

Essa estética melhora a leitura e transmite maturidade.

---

### ✅ Marcação de Follow-up
Acima de cada card existem dois checkboxes:

- **TR Elaborado**  
  Internamente significa: já houve tratamento técnico / termo de referência / análise inicial.
- **Não Atende**  
  Internamente significa: oportunidade descartada (escopo fora de interesse / inviável / sem fit comercial).

Esses checkboxes:
- São persistidos com memória.
- Reaparecem marcados quando você pesquisa novamente aquele mesmo edital.

Essa memória é crítica para não perder histórico entre sessões e não repetir trabalho quando o app “acorda”.

---

### 📦 Exportação
No final da página há botão **"Baixar XLSX"**.

- Gera um XLSX pronto para enviar / trabalhar offline.
- Remove colunas técnicas internas (_pub_raw, ids internos etc.).
- O botão tem cor alinhada ao branding (azul escuro com texto branco).

---

## 🗂 Paginação

Os cards são paginados:

- Você escolhe `Itens por página` (10 / 20 / 50).
- Navegação por **Anterior / Próxima** no topo e no rodapé dos cards.
- A numeração de página é mantida em `st.session_state`.

Objetivo: evitar render pesado de 200+ cards e manter UX responsiva.

---

## 🔌 Integração com PNCP

A coleta é feita (para cada município selecionado) consumindo o endpoint público do PNCP:

```text
GET https://pncp.gov.br/api/search
  ?tipos_documento=edital
  &ordenacao=-data
  &pagina=<n>
  &tam_pagina=100
  &municipios=<CODIGO_PNCP>
  [&status=<status>]

