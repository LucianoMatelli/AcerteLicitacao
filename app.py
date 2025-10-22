# app.py
from __future__ import annotations
import io
import time
import unicodedata
from typing import List, Dict, Tuple, Optional

import requests
import pandas as pd
import streamlit as st

# ===================== Config de página =====================
st.set_page_config(
    page_title="📑 Acerte Licitações — O seu Buscador de Editais",
    page_icon="📑",
    layout="wide",
)
st.markdown("### 📑 Acerte Licitações — O seu Buscador de Editais")
st.caption("Busca oficial no PNCP por município (com enriquecimento do Objeto).")

# ===================== Constantes =====================
PNCP_API = "https://pncp.gov.br/api/search"
HDRS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://pncp.gov.br/app/editais",
}
TIMEOUT = 30
PAGE_SIZE = 100        # tamanho aprovado
MAX_PAGES = 6          # segurança

STATUS_OPTIONS = [
    ("recebendo_proposta", "Recebendo proposta"),
    ("divulgado", "Divulgado"),
    ("em_andamento", "Em andamento"),
    ("concluido", "Concluído"),
]

# ===================== Lista aprovada de municípios (nome -> id PNCP) =====================
# (Se quiser ampliar depois, basta adicionar aqui.)
MUNICIPIOS_PADRAO: Dict[str, str] = {
    "Porto Feliz/SP": "3721",
    "Conchas/SP": "3405",
    "Torre de Pedra/SP": "3878",
    "Porangaba/SP": "3720",
    "Guareí/SP": "3477",
    "Quadra/SP": "3735",
    "Angatuba/SP": "3292",
    "Capão Bonito/SP": "3385",
    "Campina do Monte Alegre/SP": "3375",
    "Pilar do Sul/SP": "3694",
    "Sarapuí/SP": "3838",
    # "Alambari/SP": "",  # sem código → omitido
    "Capela do Alto/SP": "3386",
    "Itapetininga/SP": "3523",
    "São Miguel Arcanjo/SP": "3829",
    "Cesário Lange/SP": "3399",
    "Itararé/SP": "3533",
    "Buri/SP": "3359",
}

# ===================== Utils =====================
def _extract_items_total(js: dict) -> Tuple[List[dict], Optional[int]]:
    total = js.get("total") or js.get("count") or (js.get("meta") or {}).get("total")
    for k in ("items", "results", "conteudo", "licitacoes", "data", "documents", "documentos"):
        v = js.get(k)
        if isinstance(v, list):
            return v, total
    return [], total

def _build_editais_link(orgao_cnpj: str, ano: str, numero_sequencial: str) -> str:
    cnpj = "".join(c for c in str(orgao_cnpj or "") if c.isdigit())
    ano = str(ano or "").strip()
    seq = str(numero_sequencial or "").strip()
    if len(cnpj) == 14 and ano and seq:
        return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
    return ""

# ===================== Coleta principal (versão validada) =====================
def fetch_by_municipio_id(muni_id: str, status_ui: str, keyword: str) -> List[dict]:
    """
    Caminho principal validado: /api/search?tipos_documento=edital&municipios=<id>
    """
    out: List[dict] = []
    page = 1
    while page <= MAX_PAGES:
        params = {
            "tipos_documento": "edital",
            "ordenacao": "-data",          # validado
            "pagina": page,
            "tam_pagina": PAGE_SIZE,
            "municipios": muni_id,
            "status": status_ui,
        }
        if keyword:
            params["termo"] = keyword

        r = requests.get(PNCP_API, params=params, headers=HDRS, timeout=TIMEOUT)
        if r.status_code >= 400:
            break
        items, total = _extract_items_total(r.json())
        if not items:
            break
        out.extend(items)
        if total and len(out) >= total:
            break
        page += 1
    return out

def enrich_from_catalog2(orgao_cnpj: str, ano: str, numero_sequencial: str, uf_hint: Optional[str] = None) -> dict:
    """
    Enriquecimento para pegar 'description' (Objeto) e campos extras do catalog2.
    Faz uma busca curta e confere CNPJ/Ano/Seq.
    """
    termo = f"{orgao_cnpj} {ano} {numero_sequencial}"
    params = {
        "index": "catalog2",
        "doc_type": "_doc",
        "document_type": "edital",
        "pagina": 1,
        "tam_pagina": 10,
        "ordenacao": "-data_publicacao_pncp",
        "termo": termo,
    }
    if uf_hint:
        params["uf"] = uf_hint
    try:
        r = requests.get(PNCP_API, params=params, headers=HDRS, timeout=TIMEOUT)
        if r.status_code >= 400:
            return {}
        items, _ = _extract_items_total(r.json())
        if not items:
            return {}
        cnpj_d = "".join(c for c in str(orgao_cnpj or "") if c.isdigit())
        ano_d = str(ano or "").strip()
        seq_d = str(numero_sequencial or "").strip()
        for it in items:
            if (
                "".join(c for c in str(it.get("orgao_cnpj") or "") if c.isdigit()) == cnpj_d
                and str(it.get("ano") or "").strip() == ano_d
                and str(it.get("numero_sequencial") or "").strip() == seq_d
            ):
                return it
        return items[0]
    except Exception:
        return {}

def normalize_items(items: List[dict]) -> pd.DataFrame:
    rows = []
    for it in items:
        orgao_cnpj = it.get("orgao_cnpj") or ""
        ano = it.get("ano") or ""
        seq = it.get("numero_sequencial") or ""
        uf_hint = it.get("uf") or None
        rich = enrich_from_catalog2(orgao_cnpj, ano, seq, uf_hint=uf_hint)

        rows.append({
            "Cidade": it.get("municipio_nome") or rich.get("municipio_nome") or "",
            "UF": it.get("uf") or rich.get("uf") or "",
            "Título": it.get("title") or it.get("titulo") or "",
            "Objeto": rich.get("description") or it.get("description") or "",
            "Link para o edital": _build_editais_link(orgao_cnpj, ano, seq),
            "Tipo": it.get("document_type") or rich.get("document_type") or "edital",
            "Orgão": it.get("orgao_nome") or rich.get("orgao_nome") or "",
            "Unidade": it.get("unidade_nome") or rich.get("unidade_nome") or "",
            "Esfera": it.get("esfera_nome") or rich.get("esfera_nome") or "",
            "Modalidade": it.get("modalidade_licitacao_nome") or rich.get("modalidade_licitacao_nome") or "",
            "Publicação": it.get("data_publicacao_pncp") or rich.get("data_publicacao_pncp") or "",
            "Fim do envio de proposta": it.get("data_fim_vigencia") or rich.get("data_fim_vigencia") or "",
            "Tipo (PNCP)": it.get("tipo_nome") or rich.get("tipo_nome") or "",
        })

    df = pd.DataFrame(rows)
    if not df.empty and "Publicação" in df.columns:
        df["__pub__"] = pd.to_datetime(df["Publicação"], errors="coerce")
        df = df.sort_values("__pub__", ascending=False, na_position="last").drop(columns="__pub__", errors="ignore")
    return df

# ===================== Sidebar (seleção de municípios da lista aprovada) =====================
with st.sidebar:
    st.subheader("Parâmetros")

    st.markdown("**Municípios disponíveis** (lista aprovada):")
    munis_nomes = list(MUNICIPIOS_PADRAO.keys())

    select_all = st.checkbox("Selecionar todos", value=False)
    if select_all:
        selected_munis = st.multiselect(
            "Selecione municípios",
            options=munis_nomes,
            default=munis_nomes,
            placeholder="Digite para filtrar…",
            label_visibility="collapsed",
        )
    else:
        selected_munis = st.multiselect(
            "Selecione municípios",
            options=munis_nomes,
            default=[],
            placeholder="Digite para filtrar…",
            label_visibility="collapsed",
        )

    keyword = st.text_input("Palavra-chave (opcional)", value="")

    status_map = {label: key for key, label in STATUS_OPTIONS}
    status_label = st.selectbox("Status no PNCP", options=[l for _, l in STATUS_OPTIONS], index=0)
    status_ui = status_map[status_label]

    run_btn = st.button("🔎 Executar busca", use_container_width=True, type="primary")

# espaço da TABELA (no topo)
tbl_placeholder = st.empty()
st.divider()

# Barra de progresso (exibe município atual — como a versão aprovada)
progress_bar = st.progress(0, text="Aguardando…")

# ===================== Execução =====================
if run_btn:
    if not selected_munis:
        st.warning("Selecione ao menos um município.", icon="⚠️")
    else:
        all_items: List[dict] = []
        total = len(selected_munis)
        t0 = time.time()

        for i, mun in enumerate(selected_munis, start=1):
            progress_bar.progress(
                int(i / total * 100),
                text=f"Buscando: {mun} ({i}/{total}) — status {status_label.lower()}",
            )

            muni_id = MUNICIPIOS_PADRAO.get(mun, "").strip()
            if not muni_id:
                continue

            items_city = fetch_by_municipio_id(muni_id, status_ui=status_ui, keyword=keyword)
            all_items.extend(items_city)

        progress_bar.progress(100, text=f"Concluído — {total} município(s) em {time.time()-t0:.1f}s")

        df = normalize_items(all_items)
        if df.empty:
            tbl_placeholder.warning(
                "Nenhum resultado retornado. Tente outro status, palavra-chave ou municípios.",
                icon="ℹ️",
            )
        else:
            # links clicáveis na tabela
            df_view = df.copy()
            if "Link para o edital" in df_view.columns:
                df_view["Link para o edital"] = df_view["Link para o edital"].apply(
                    lambda u: f"[abrir edital]({u})" if u else ""
                )
            tbl_placeholder.dataframe(df_view, use_container_width=True)

            # Download apenas XLSX (como aprovado)
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as xw:
                df.to_excel(xw, index=False, sheet_name="PNCP")
            st.download_button(
                "⬇️ Baixar XLSX",
                data=buf.getvalue(),
                file_name="pncp_resultados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
            )
