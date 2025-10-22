# app.py
from __future__ import annotations
import os
import io
import time
import json
import unicodedata
from typing import List, Dict, Tuple, Optional

import requests
import pandas as pd
import streamlit as st

# ==========================
# Config / Constantes
# ==========================
st.set_page_config(
    page_title="📑 Acerte Licitações — O seu Buscador de Editais",
    page_icon="📑",
    layout="wide",
)

PNCP_API = "https://pncp.gov.br/api/search"
HDRS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://pncp.gov.br/app/editais",
}
TIMEOUT = 30
DEFAULT_PAGE_SIZE = 100     # PNCP máximo comum
MAX_PAGES = 6               # limite de segurança por cidade

STATUS_OPTIONS = [
    ("recebendo_proposta", "Recebendo proposta"),
    ("divulgado", "Divulgado"),
    ("em_andamento", "Em andamento"),
    ("concluido", "Concluído"),
]

# ==========================
# Utilitários
# ==========================
def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return " ".join(s.split())

def _safe_get(d: dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d

def _build_editais_link(orgao_cnpj: str, ano: str, numero_sequencial: str) -> str:
    cnpj = "".join([c for c in str(orgao_cnpj or "") if c.isdigit()])
    ano = str(ano or "").strip()
    seq = str(numero_sequencial or "").strip()
    if len(cnpj) == 14 and ano and seq:
        return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
    return ""

def _extract_items_total(js: dict) -> Tuple[List[dict], Optional[int]]:
    """
    O PNCP varia a forma de retorno. Tentamos campos usuais.
    """
    total = _safe_get(js, "total") or _safe_get(js, "count") or _safe_get(js, "meta", "total")
    for k in ("items", "results", "conteudo", "licitacoes", "data", "documents", "documentos"):
        v = js.get(k)
        if isinstance(v, list):
            return v, total
    return [], total

# ==========================
# IBGE — UF e Municípios
# ==========================
@st.cache_data(ttl=86400, show_spinner=False)
def ibge_ufs() -> List[Dict]:
    r = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados?orderBy=nome", timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # devolve [{sigla:"SP", nome:"São Paulo"}, ...]
    return [{"sigla": uf["sigla"], "nome": uf["nome"]} for uf in data]

@st.cache_data(ttl=86400, show_spinner=False)
def ibge_municipios(uf_sigla: str) -> List[str]:
    r = requests.get(
        f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf_sigla}/municipios?orderBy=nome",
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return [m["nome"] for m in data]

# ==========================
# Resolução do municipio_id no PNCP
# ==========================
@st.cache_data(ttl=86400, show_spinner=False)
def resolve_pncp_municipio_id(uf: str, municipio_nome: str) -> Optional[str]:
    """
    Tenta obter o municipio_id mais frequente que corresponde ao nome (normalizado)
    dentro de 5 páginas recentes do índice 'catalog2'.
    """
    uf = (uf or "").strip().upper()
    alvo = _norm(municipio_nome)
    contagem: Dict[str, int] = {}

    for pagina in range(1, 6):
        params = {
            "index": "catalog2",
            "doc_type": "_doc",
            "document_type": "edital",
            "pagina": pagina,
            "tam_pagina": 100,
            "ordenacao": "-data_publicacao_pncp",
            "uf": uf,
            "termo": municipio_nome,
        }
        try:
            r = requests.get(PNCP_API, params=params, headers=HDRS, timeout=TIMEOUT)
            if r.status_code >= 400:
                break
            items, _ = _extract_items_total(r.json())
            if not items:
                break
            for it in items:
                nome = _norm(it.get("municipio_nome") or "")
                mid = str(it.get("municipio_id") or it.get("municipio") or "").strip()
                if not mid:
                    continue
                if nome == alvo:
                    contagem[mid] = contagem.get(mid, 0) + 2
                elif nome.startswith(alvo) or alvo.startswith(nome):
                    contagem[mid] = contagem.get(mid, 0) + 1
        except Exception:
            break

    if not contagem:
        return None
    return max(contagem.items(), key=lambda kv: kv[1])[0]

# ==========================
# Busca principal por município (com fallback)
# ==========================
def fetch_pages_by_municipio_id(
    muni_id: Optional[str],
    uf: str,
    municipio_nome: str,
    keyword: str,
    status_ui: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = MAX_PAGES,
) -> List[dict]:
    """
    Se muni_id existir: usa a rota 'municipios=<id>'.
    Senão: fallback por UF + filtro do nome do município.
    """
    all_items: List[dict] = []

    # Caminho "aprovado": municipios=<id>
    if muni_id:
        page = 1
        while page <= max_pages:
            params = {
                "tipos_documento": "edital",
                "pagina": page,
                "tam_pagina": page_size,
                "ordenacao": "-data",
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
            all_items.extend(items)
            if total and len(all_items) >= total:
                break
            page += 1
        return all_items

    # Fallback: UF + filtro de nome do município (cliente)
    page = 1
    alvo = _norm(municipio_nome)
    while page <= max_pages:
        params = {
            "index": "catalog2",
            "doc_type": "_doc",
            "document_type": "edital",
            "pagina": page,
            "tam_pagina": page_size,
            "ordenacao": "-data_publicacao_pncp",
            "uf": uf,
        }
        if keyword:
            params["termo"] = keyword

        r = requests.get(PNCP_API, params=params, headers=HDRS, timeout=TIMEOUT)
        if r.status_code >= 400:
            break
        items, total = _extract_items_total(r.json())
        if not items:
            break

        for it in items:
            nome = _norm(it.get("municipio_nome") or "")
            if nome == alvo or nome.startswith(alvo) or alvo.startswith(nome):
                all_items.append(it)

        if total and page * page_size >= total:
            break
        page += 1

    return all_items

# ==========================
# Enriquecimento (description etc. pelo catalog2)
# ==========================
def enrich_from_catalog2(orgao_cnpj: str, ano: str, numero_sequencial: str, uf: str) -> dict:
    """
    Busca 1 item no catalog2 que bata com CNPJ/Ano/Seq para pegar description e campos ricos.
    Estratégia: pesquisa por 'termo' combinando cnpj/ano/seq e confere match exato.
    """
    termo = f"{orgao_cnpj} {ano} {numero_sequencial}"
    params = {
        "index": "catalog2",
        "doc_type": "_doc",
        "document_type": "edital",
        "pagina": 1,
        "tam_pagina": 10,
        "ordenacao": "-data_publicacao_pncp",
        "uf": uf,
        "termo": termo,
    }
    try:
        r = requests.get(PNCP_API, params=params, headers=HDRS, timeout=TIMEOUT)
        if r.status_code >= 400:
            return {}
        items, _ = _extract_items_total(r.json())
        if not items:
            return {}
        cnpj_d = "".join([c for c in str(orgao_cnpj or "") if c.isdigit()])
        seq_d = str(numero_sequencial or "").strip()
        ano_d = str(ano or "").strip()

        # escolhe o primeiro que casa cnpj/ano/seq
        for it in items:
            if (
                "".join([c for c in str(it.get("orgao_cnpj") or "") if c.isdigit()]) == cnpj_d
                and str(it.get("ano") or "").strip() == ano_d
                and str(it.get("numero_sequencial") or "").strip() == seq_d
            ):
                return it
        # se não achou match perfeito, devolve o primeiro
        return items[0]
    except Exception:
        return {}

# ==========================
# Normalização final -> DataFrame
# ==========================
def normalize_items(items: List[dict], uf: str) -> pd.DataFrame:
    rows = []
    for it in items:
        title = it.get("title") or it.get("titulo") or ""
        orgao_cnpj = it.get("orgao_cnpj") or ""
        ano = it.get("ano") or ""
        seq = it.get("numero_sequencial") or ""

        # enriquecer (para 'description' e outros)
        rich = enrich_from_catalog2(orgao_cnpj, ano, seq, uf)

        row = {
            "Cidade": it.get("municipio_nome") or rich.get("municipio_nome") or "",
            "UF": it.get("uf") or rich.get("uf") or uf,
            "Título": title,
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
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    # ordenar por Publicação desc se possível
    if not df.empty and "Publicação" in df.columns:
        df["__pub__"] = pd.to_datetime(df["Publicação"], errors="coerce")
        df = df.sort_values("__pub__", ascending=False, na_position="last").drop(columns="__pub__", errors="ignore")
    return df

# ==========================
# UI
# ==========================
st.markdown("### 📑 Acerte Licitações — O seu Buscador de Editais")
st.caption("Busca oficial no PNCP com enriquecimento automático do objeto (description).")

with st.sidebar:
    st.subheader("Parâmetros")
    # UF
    ufs = ibge_ufs()
    uf_labels = [f"{u['nome']} ({u['sigla']})" for u in ufs]
    uf_siglas = [u["sigla"] for u in ufs]
    uf_idx = uf_siglas.index("SP") if "SP" in uf_siglas else 0
    uf_choice = st.selectbox("UF", options=list(range(len(ufs))), index=uf_idx, format_func=lambda i: uf_labels[i])
    uf = ufs[uf_choice]["sigla"]

    # Lista de municípios da UF (com busca e múltipla seleção)
    all_cities = ibge_municipios(uf)
    st.write("Municípios (selecione um ou mais):")
    selected_cities = st.multiselect(
        "Selecione municípios",
        options=all_cities,
        default=[],
        placeholder="Digite para filtrar…",
        label_visibility="collapsed",
    )

    # Palavra-chave
    keyword = st.text_input("Palavra-chave (opcional)", value="")

    # Status
    status_map = {label: key for key, label in STATUS_OPTIONS}
    status_label = st.selectbox("Status no PNCP", options=[l for _, l in STATUS_OPTIONS], index=0)
    status_ui = status_map[status_label]

    run_btn = st.button("🔎 Executar busca", use_container_width=True, type="primary")

# espaço de resultados (tabela em cima, como combinado)
tbl_placeholder = st.empty()
st.divider()

# Feedback de andamento por cidade (barra estilo “aprovado”)
progress_text = st.empty()
progress_bar = st.progress(0, text="Aguardando…")

# Execução
if run_btn:
    if not selected_cities:
        st.warning("Selecione ao menos um município.", icon="⚠️")
    else:
        all_collected: List[dict] = []
        total_cities = len(selected_cities)
        started = time.time()

        for idx, city in enumerate(selected_cities, start=1):
            # Atualiza barra com o nome do município atual (em vez de % puro)
            progress_bar.progress(int(idx / total_cities * 100),
                                  text=f"Buscando: {city} ({idx}/{total_cities}) — status {status_label.lower()}")

            # tenta resolver o ID PNCP do município
            muni_id = resolve_pncp_municipio_id(uf, city)

            # coleta itens (caminho principal por ID, senão fallback por UF+city)
            items_m = fetch_pages_by_municipio_id(
                muni_id=muni_id,
                uf=uf,
                municipio_nome=city,
                keyword=keyword,
                status_ui=status_ui,
                page_size=DEFAULT_PAGE_SIZE,
                max_pages=MAX_PAGES,
            )
            all_collected.extend(items_m)

        # Finaliza barra
        progress_bar.progress(100, text=f"Concluído — {total_cities} município(s) em {time.time()-started:.1f}s")

        # Normaliza e mostra tabela
        df = normalize_items(all_collected, uf=uf)

        if df.empty:
            tbl_placeholder.warning("Nenhum resultado retornado. Tente outro status, palavra-chave ou municípios.", icon="ℹ️")
        else:
            # Corrige links como hyperlink clicável
            if "Link para o edital" in df.columns:
                df_disp = df.copy()
                df_disp["Link para o edital"] = df_disp["Link para o edital"].apply(
                    lambda u: f"[abrir edital]({u})" if u else ""
                )
            else:
                df_disp = df

            tbl_placeholder.dataframe(df_disp, use_container_width=True)

            # Download XLSX (apenas XLSX, sem CSV)
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
