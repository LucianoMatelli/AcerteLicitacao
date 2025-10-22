# app.py
from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

# =========================
# Configuração do Streamlit
# =========================
st.set_page_config(
    page_title="🧭 Acerte Licitações — O seu Buscador de Editais",
    page_icon="🧭",
    layout="wide",
)
st.title("🧭 Acerte Licitações — O seu Buscador de Editais")
st.caption(
    "Busque editais no PNCP por UF e (opcionalmente) por municípios. "
    "Use **Carregar municípios** para listar os disponíveis na UF e selecione os desejados antes de executar a busca."
)

# ================
# Parâmetros gerais
# ================
BASE_API = "https://pncp.gov.br/api/search"
TIMEOUT = 30
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
HDRS = {
    "User-Agent": UA,
    "Referer": "https://pncp.gov.br/app/editais",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

UFS = [
    "", "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

# Limites internos (não exibidos na UI)
DEFAULT_PAGE_SIZE = 100
DISCOVERY_PAGES = 5   # para "Carregar municípios"
MAX_PAGES = 30        # para a busca final

# =======================
# Estado / utilitários
# =======================
if "saved_searches" not in st.session_state:
    st.session_state.saved_searches: Dict[str, Dict] = {}

if "muni_choices" not in st.session_state:
    st.session_state.muni_choices: List[str] = []

if "muni_selected" not in st.session_state:
    st.session_state.muni_selected: List[str] = []

def _parse_dt(val: Optional[str]) -> Optional[pd.Timestamp]:
    if not val:
        return None
    try:
        return pd.to_datetime(val, utc=False, errors="coerce")
    except Exception:
        return None

def _fmt_br(dt: Optional[pd.Timestamp]) -> str:
    if isinstance(dt, pd.Timestamp) and not pd.isna(dt):
        if dt.hour or dt.minute or dt.second:
            return dt.strftime("%d/%m/%Y %H:%M")
        return dt.strftime("%d/%m/%Y")
    return ""

def _extract_items_total(js: dict) -> Tuple[List[dict], int]:
    if not isinstance(js, dict):
        return [], 0

    items = []
    for items_key in ("items", "results", "licitacoes", "documentos", "conteudo", "data"):
        if isinstance(js.get(items_key), list):
            items = js[items_key]
            break
    else:
        data = js.get("data") or {}
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            items = data["items"]

    total = 0
    for total_key in ("total", "total_results", "totalItems", "count"):
        if isinstance(js.get(total_key), int):
            total = js[total_key]
            break
    if not total:
        data = js.get("data") or {}
        if isinstance(data, dict) and isinstance(data.get("total"), int):
            total = data["total"]

    if not total and items:
        total = len(items)
    return items or [], int(total or 0)

def fetch_page(
    uf: Optional[str],
    page: int,
    page_size: int = DEFAULT_PAGE_SIZE,
    keyword: str = "",
    status: str = "recebendo_proposta",
) -> Tuple[List[Dict], int]:
    # 1) API “clássica”
    params1 = {
        "tipos_documento": "edital",
        "pagina": page,
        "tam_pagina": page_size,
        "ordenacao": "-data",
    }
    if uf:
        params1["uf"] = uf
    if status:
        params1["status"] = status
    if keyword:
        params1["termo"] = keyword

    r = requests.get(BASE_API, params=params1, headers=HDRS, timeout=TIMEOUT)
    if r.status_code < 400:
        return _extract_items_total(r.json())

    if r.status_code not in (400, 422):
        r.raise_for_status()

    # 2) Fallback “catalog2”
    params2 = {
        "index": "catalog2",
        "doc_type": "_doc",
        "document_type": "edital",
        "pagina": page,
        "tam_pagina": page_size,
        "ordenacao": "-data_publicacao_pncp",
    }
    if uf:
        params2["uf"] = uf
    if keyword:
        params2["termo"] = keyword
    if status:
        params2["status"] = status

    r2 = requests.get(BASE_API, params=params2, headers=HDRS, timeout=TIMEOUT)
    r2.raise_for_status()
    return _extract_items_total(r2.json())

def collect_results(
    uf: Optional[str],
    keyword: str = "",
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = MAX_PAGES,
    progress_cb=None,
) -> List[Dict]:
    all_items: List[Dict] = []
    page = 1
    total_known = None

    while page <= max_pages:
        items, total = fetch_page(uf=uf, page=page, page_size=page_size, keyword=keyword)
        if total_known is None:
            total_known = total

        if not items:
            break

        all_items.extend(items)
        if progress_cb and total:
            progress_cb(min(len(all_items) / total, 0.99))

        if total and len(all_items) >= total:
            break
        page += 1

    if progress_cb:
        progress_cb(1.0)
    return all_items

def _build_link(item: dict) -> str:
    url = (item.get("item_url") or "").strip()
    if url:
        parts = url.strip("/").split("/")
        if len(parts) >= 4:
            cnpj, ano, seq = parts[-3], parts[-2], parts[-1]
            return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
    cnpj = str(item.get("orgao_cnpj") or "").strip()
    ano = str(item.get("ano") or "").strip()
    seq = str(item.get("numero_sequencial") or "").strip()
    if len(cnpj) == 14 and len(ano) == 4 and seq:
        return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
    return ""

def normalize_item(item: dict) -> dict:
    title = item.get("title") or item.get("titulo") or ""
    description = item.get("description") or item.get("descricao") or ""
    document_type = item.get("document_type") or item.get("tipo_documento") or ""
    orgao_nome = item.get("orgao_nome") or item.get("orgao") or ""
    unidade_nome = item.get("unidade_nome") or item.get("unidade") or ""
    esfera_nome = item.get("esfera_nome") or item.get("esfera") or ""
    modalidade = (
        item.get("modalidade_licitacao_nome")
        or item.get("modalidade")
        or item.get("modalidade_nome")
        or ""
    )
    tipo_nome = item.get("tipo_nome") or item.get("tipo") or ""
    municipio_nome = item.get("municipio_nome") or item.get("municipio") or ""
    uf = item.get("uf") or ""

    dt_pub = _parse_dt(item.get("data_publicacao_pncp") or item.get("dataPublicacao"))
    dt_fim = _parse_dt(item.get("data_fim_vigencia") or item.get("dataFimVigencia"))

    link = _build_link(item)

    return {
        "Cidade": municipio_nome,
        "UF": uf,
        "Título": title,
        "Objeto": description,
        "Link para o edital": link,
        "Tipo": document_type,
        "Orgão": orgao_nome,
        "Unidade": unidade_nome,
        "Esfera": esfera_nome,
        "Modalidade": modalidade,
        "Publicação": _fmt_br(dt_pub),
        "Fim do envio de proposta": _fmt_br(dt_fim),
        "Tipo (nome)": tipo_nome,
        "_raw_item_url": item.get("item_url", ""),
    }

# ==================
# Barra lateral (UI)
# ==================
with st.sidebar:
    st.subheader("Parâmetros da busca")
    uf = st.selectbox("UF", UFS, index=UFS.index("SP"))
    keyword = st.text_input("Palavra-chave (opcional)", value="")

    st.divider()
    st.markdown("#### Municípios")
    if st.button("🔎 Carregar municípios desta UF", use_container_width=True):
        try:
            with st.spinner("Carregando municípios…"):
                # varre poucas páginas para montar lista
                sample_items = collect_results(
                    uf=(uf or None),
                    keyword=keyword.strip(),
                    page_size=DEFAULT_PAGE_SIZE,
                    max_pages=DISCOVERY_PAGES,
                )
                munis = sorted({(it.get("municipio_nome") or "").strip()
                                for it in sample_items if (it.get("municipio_nome") or "").strip()})
                st.session_state.muni_choices = munis
                # por padrão, nenhuma pré-selecionada (usuário escolhe)
                st.session_state.muni_selected = []
            st.success(f"{len(st.session_state.muni_choices)} município(s) carregado(s).")
        except Exception as e:
            st.error(f"Falha ao carregar municípios: {e}")

    muni_selected = st.multiselect(
        "Selecione municípios (opcional)",
        options=st.session_state.muni_choices,
        default=st.session_state.muni_selected,
        placeholder="Digite para buscar…",
    )
    st.session_state.muni_selected = muni_selected

    st.divider()
    st.markdown("#### Pesquisas salvas")
    # salvar atual
    col_a, col_b = st.columns([2,1])
    with col_a:
        save_name = st.text_input("Nome da pesquisa", placeholder="Ex.: SP – saúde")
    with col_b:
        if st.button("💾 Salvar", use_container_width=True, type="primary"):
            if not save_name.strip():
                st.warning("Informe um nome para a pesquisa.")
            else:
                st.session_state.saved_searches[save_name.strip()] = {
                    "uf": uf,
                    "keyword": keyword.strip(),
                    "municipios": st.session_state.muni_selected[:],
                }
                st.success(f"Pesquisa **{save_name.strip()}** salva.")

    # aplicar/excluir
    if st.session_state.saved_searches:
        names = sorted(st.session_state.saved_searches.keys())
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            chosen = st.selectbox("Minhas pesquisas", names)
        with col2:
            if st.button("Aplicar", use_container_width=True):
                cfg = st.session_state.saved_searches[chosen]
                # aplica parâmetros
                uf = cfg.get("uf") or ""
                # atualiza selectbox UF visualmente:
                st.session_state._provided_uf = uf  # só pra referência interna
                st.session_state.muni_selected = cfg.get("municipios", [])
                # para exibir lista na UI, recarrega choices (caso vazio)
                if st.session_state.muni_selected and not st.session_state.muni_choices:
                    st.session_state.muni_choices = sorted(st.session_state.muni_selected)
                st.session_state.keyword_applied = cfg.get("keyword", "")
                st.experimental_rerun()
        with col3:
            if st.button("Excluir", use_container_width=True):
                st.session_state.saved_searches.pop(chosen, None)
                st.experimental_rerun()

        # exportar/importar JSON
        colx, coly = st.columns(2)
        with colx:
            if st.download_button(
                "⬇️ Exportar pesquisas (JSON)",
                data=json.dumps(st.session_state.saved_searches, ensure_ascii=False, indent=2),
                file_name="pesquisas_salvas.json",
                mime="application/json",
                use_container_width=True,
            ):
                pass
        with coly:
            up = st.file_uploader("Importar JSON", type=["json"])
            if up is not None:
                try:
                    data = json.load(up)
                    if isinstance(data, dict):
                        st.session_state.saved_searches.update(data)
                        st.success("Pesquisas importadas.")
                        st.experimental_rerun()
                    else:
                        st.error("Arquivo inválido.")
                except Exception as e:
                    st.error(f"Falha ao importar: {e}")

    st.divider()
    run = st.button("🚀 Executar busca", type="primary", use_container_width=True)

# Sincroniza keyword se aplicada via pesquisa salva
if "keyword_applied" in st.session_state:
    # mostra na UI:
    with st.sidebar:
        st.info(f"Palavra-chave aplicada: **{st.session_state.keyword_applied}**")
    # usa para a busca
    effective_keyword = st.session_state.keyword_applied
    # limpa após uso para não confundir próximos runs
    del st.session_state["keyword_applied"]
else:
    effective_keyword = (keyword or "").strip()

# ==========================
# Resultado principal (tabela)
# ==========================
result_placeholder = st.empty()
download_placeholder = st.empty()
summary_placeholder = st.empty()
details_placeholder = st.container()

if run:
    st.toast("Iniciando busca no PNCP…", icon="🔎")
    progress = st.progress(0.0)
    status = st.status("Consultando API do PNCP…", expanded=False)

    try:
        t0 = time.time()
        items = collect_results(
            uf=(uf or None),
            keyword=effective_keyword,
            page_size=DEFAULT_PAGE_SIZE,
            max_pages=MAX_PAGES,
            progress_cb=progress.progress,
        )
        rows = [normalize_item(it) for it in items]
        df = pd.DataFrame(rows)

        # filtro por municípios, se selecionados
        muni_selected = st.session_state.muni_selected or []
        if muni_selected:
            df = df[df["Cidade"].isin(set(muni_selected))].reset_index(drop=True)

        # colunas finais (exibição)
        display_cols = [
            "Cidade", "UF", "Título", "Objeto", "Link para o edital",
            "Tipo", "Orgão", "Unidade", "Esfera", "Modalidade",
            "Publicação", "Fim do envio de proposta", "Tipo (nome)",
        ]
        for c in display_cols:
            if c not in df.columns:
                df[c] = ""
        df = df[display_cols]

        # Tabela primeiro
        with result_placeholder:
            st.subheader("Resultados")
            st.dataframe(df, use_container_width=True, hide_index=True)

        # Download XLSX
        if not df.empty:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as xw:
                df.to_excel(xw, index=False, sheet_name="PNCP")
            buf.seek(0)
            with download_placeholder:
                st.download_button(
                    "⬇️ Baixar XLSX",
                    data=buf,
                    file_name=f"pncp_resultados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )

        elapsed = time.time() - t0
        with summary_placeholder:
            st.success(f"Busca concluída: {len(df)} registro(s) em {elapsed:.1f}s.", icon="✅")

        # Lista por cidade (abaixo)
        with details_placeholder:
            if not df.empty:
                st.markdown("### Editais por cidade (lista)")
                grp = df.groupby(["Cidade", "UF"], dropna=False)
                for (cidade, ufv), g in grp:
                    st.markdown(f"**{cidade or '-'} / {ufv or '-'}** — {len(g)} edital(is)")
                    for _, row in g.iterrows():
                        titulo = (row.get("Título") or "").strip() or "(sem título)"
                        link = (row.get("Link para o edital") or "").strip()
                        objeto = (row.get("Objeto") or "").strip()
                        if link:
                            st.markdown(f"- [{titulo}]({link})  \n  _{objeto}_")
                        else:
                            st.markdown(f"- {titulo}  \n  _{objeto}_")

        status.update(label="Pronto!", state="complete", expanded=False)
        st.toast("Concluído!", icon="🎉")

    except requests.HTTPError as e:
        status.update(label="Falha na busca. Tente novamente em instantes.", state="error", expanded=True)
        st.error(f"Falha na busca. Tente novamente em instantes.\n\n{e.__class__.__name__}: {e}", icon="🛑")
    except Exception as e:
        status.update(label="Ocorreu um erro inesperado.", state="error", expanded=True)
        st.exception(e)
