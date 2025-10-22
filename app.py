# app.py
from __future__ import annotations
import os
import json
import io
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

# ==========================
# Config
# ==========================
st.set_page_config(
    page_title="🧭 Acerte Licitações — O seu Buscador de Editais",
    page_icon="🧭",
    layout="wide",
)

TITLE = "🧭 Acerte Licitações — O seu Buscador de Editais"

PNCP_API = "https://pncp.gov.br/api/search"
INDEX = "catalog2"           # índice estável que usamos
DOC_TYPE = "_doc"
DEFAULT_STATUS = "recebendo_proposta"  # foco no que está aberto
MAX_PAGES = 12               # limite interno de páginas para evitar consultas muito longas
PAGE_SIZE = 100              # fixo (não expõe no UI)
TIMEOUT = 25

SAVED_FILE = Path("saved_searches.json")

UF_LIST = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG",
    "MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR",
    "RS","SC","SE","SP","TO"
]

# ==========================
# Helpers: Saved Searches
# ==========================
def load_saved() -> Dict[str, Dict]:
    try:
        if SAVED_FILE.exists():
            return json.loads(SAVED_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def save_saved(data: Dict[str, Dict]) -> None:
    try:
        SAVED_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # se não conseguir persistir em disco, ainda guardamos em sessão
        st.session_state["_saved_fallback"] = data

def get_saved_from_state() -> Dict[str, Dict]:
    disk = load_saved()
    mem = st.session_state.get("_saved_fallback", {})
    # prefer disk, merge mem keys
    merged = {**mem, **disk}
    return merged

# ==========================
# PNCP Fetch (API)
# ==========================
def fetch_page(uf: str, page: int, keyword: str = "") -> Tuple[List[dict], int]:
    """
    Consulta 1 página do índice 'catalog2' filtrando por UF e status.
    Retorna (itens, total_aprox)
    """
    params = {
        "index": INDEX,
        "doc_type": DOC_TYPE,
        "pagina": page,
        "tam_pagina": PAGE_SIZE,
        "ordenacao": "-data_publicacao_pncp",
        "uf": uf,
        "status": DEFAULT_STATUS,
        "document_type": "edital",
    }
    if keyword.strip():
        params["q"] = keyword.strip()

    r = requests.get(PNCP_API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    js = r.json()

    # achar lista de itens
    items = []
    for key in ("items","results","conteudo","licitacoes","data","documents","documentos"):
        if isinstance(js, dict) and isinstance(js.get(key), list):
            items = js[key]
            break

    total = int(js.get("total", len(items))) if isinstance(js, dict) else len(items)
    return items, total

def collect_results(uf: str, keyword: str = "") -> pd.DataFrame:
    all_rows: List[dict] = []
    total_hint = None

    for page in range(1, MAX_PAGES + 1):
        items, total = fetch_page(uf=uf, page=page, keyword=keyword)
        total_hint = total
        if not items:
            break

        for it in items:
            # campos conforme pedido
            title = it.get("title") or ""
            description = it.get("description") or ""
            item_url = it.get("item_url") or ""
            document_type = it.get("document_type") or ""
            orgao_nome = it.get("orgao_nome") or ""
            unidade_nome = it.get("unidade_nome") or ""
            esfera_nome = it.get("esfera_nome") or ""
            modalidade_nome = it.get("modalidade_licitacao_nome") or ""
            data_pub = it.get("data_publicacao_pncp") or ""
            data_fim_vig = it.get("data_fim_vigencia") or ""
            tipo_nome = it.get("tipo_nome") or ""
            municipio_nome = it.get("municipio_nome") or ""
            uf_val = it.get("uf") or ""

            cnpj = (it.get("orgao_cnpj") or "").strip()
            ano = (it.get("ano") or "").strip()
            num = (it.get("numero_sequencial") or "").strip()
            link_final = ""
            if len(cnpj) == 14 and len(ano) == 4 and num:
                link_final = f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{num}"
            elif item_url:
                # fallback (corrige /app/compras -> /app/editais)
                link_final = "https://pncp.gov.br" + item_url.replace("/app/compras/", "/app/editais/").replace("/compras/","/app/editais/")

            all_rows.append({
                "Cidade": municipio_nome,
                "UF": uf_val,
                "Título": title,
                "Objeto": description,
                "Link para o edital": link_final,
                "Tipo": document_type,
                "Orgão": orgao_nome,
                "Unidade": unidade_nome,
                "Esfera": esfera_nome,
                "Modalidade": modalidade_nome,
                "Publicação": data_pub,
                "Fim do envio de proposta": data_fim_vig,
                "Tipo (nome)": tipo_nome,
            })

        # feedback de progresso
        done = min(page * PAGE_SIZE, total_hint or page * PAGE_SIZE)
        yield_rows = pd.DataFrame(all_rows)
        yield (page, total_hint or 0, yield_rows)

        if len(items) < PAGE_SIZE:
            break

    # final
    final_df = pd.DataFrame(all_rows)
    yield (None, total_hint or len(final_df), final_df)

# ==========================
# UI
# ==========================
st.markdown(f"## {TITLE}")

with st.sidebar:
    st.markdown("### 🔎 Filtros")
    uf = st.selectbox("UF", UF_LIST, index=UF_LIST.index("SP") if "SP" in UF_LIST else 0)
    keyword = st.text_input("Palavra-chave (opcional)", value="")

    st.markdown("---")
    st.markdown("### 🗂️ Pesquisas salvas")
    saved = get_saved_from_state()
    saved_names = sorted(saved.keys())
    chosen_saved = st.selectbox("Carregar pesquisa", ["(nenhuma)"] + saved_names)

    if chosen_saved != "(nenhuma)":
        preset = saved[chosen_saved]
        # mostra breve resumo
        st.caption(f"**{chosen_saved}** — {len(preset.get('municipios', []))} município(s)")
    else:
        preset = {"municipios": []}

    st.markdown("### 📝 Definir/editar pesquisa")
    new_name = st.text_input("Nome da pesquisa")
    municipios_text = st.text_area(
        "Municípios (um por linha, ex.: 'Porto Feliz', 'Sorocaba')",
        value="\n".join(preset.get("municipios", [])),
        height=160,
        placeholder="Ex.:\nPorto Feliz\nTatuí\nSorocaba",
    )
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        if st.button("💾 Salvar pesquisa"):
            nm = new_name.strip()
            muns = [m.strip() for m in municipios_text.splitlines() if m.strip()]
            if nm:
                saved[nm] = {"municipios": muns}
                save_saved(saved)
                st.success(f"Pesquisa '{nm}' salva ({len(muns)} município(s)). Atualize a caixa de seleção acima para vê-la.")
            else:
                st.warning("Informe um nome para a pesquisa antes de salvar.")
    with col_s2:
        if st.button("🗑️ Excluir pesquisa"):
            nm = new_name.strip()
            if nm and nm in saved:
                del saved[nm]
                save_saved(saved)
                st.success(f"Pesquisa '{nm}' excluída.")
            else:
                st.warning("Informe o nome exato de uma pesquisa existente.")
    with col_s3:
        if st.button("🔁 Carregar no editor"):
            if chosen_saved != "(nenhuma)":
                st.session_state["__prefill_name"] = chosen_saved
                st.session_state["__prefill_muns"] = "\n".join(saved[chosen_saved].get("municipios", []))
            else:
                st.info("Selecione uma pesquisa salva para pré-carregar.")

# prefill se solicitado
if "__prefill_name" in st.session_state:
    st.session_state.pop("__prefill_name")

if "__prefill_muns" in st.session_state:
    st.session_state.pop("__prefill_muns")

st.markdown("---")

run_col1, run_col2 = st.columns([1,3])
with run_col1:
    run = st.button("🚀 Executar busca")

progress = st.empty()
status_text = st.empty()
table_placeholder = st.empty()

# ==========================
# Execução
# ==========================
if run:
    # barra de progresso
    prog = progress.progress(0, text="Iniciando consulta ao PNCP…")
    collected_df = pd.DataFrame()

    page_count_est = MAX_PAGES
    pages_done = 0

    try:
        for page_info in collect_results(uf=uf, keyword=keyword):
            page, total_hint, partial_df = page_info
            collected_df = partial_df.copy()

            if page is None:
                # final
                prog.progress(100, text=f"Concluído. Total estimado: {total_hint} itens.")
                break
            else:
                pages_done += 1
                pct = int(min(100, (pages_done / page_count_est) * 100))
                prog.progress(pct, text=f"Coletando página {page}…")

        # filtro por municípios (cliente) se houver lista
        municipios_filter = [m.strip().lower() for m in municipios_text.splitlines() if m.strip()]
        if municipios_filter:
            mask = collected_df["Cidade"].fillna("").str.lower().isin(municipios_filter)
            collected_df = collected_df[mask].reset_index(drop=True)

        # render tabela
        if collected_df.empty:
            table_placeholder.info("Nenhum resultado para os filtros selecionados.")
        else:
            # ordenar por data de publicação desc (quando possível)
            def to_ts(x):
                try:
                    return pd.to_datetime(x, errors="coerce")
                except Exception:
                    return pd.NaT
            if "Publicação" in collected_df.columns:
                collected_df["_ord"] = collected_df["Publicação"].apply(to_ts)
                collected_df = collected_df.sort_values("_ord", ascending=False, na_position="last").drop(columns=["_ord"]).reset_index(drop=True)

            table_placeholder.dataframe(collected_df, use_container_width=True)

            # botão XLSX (sem CSV, por pedido)
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                collected_df.to_excel(writer, index=False, sheet_name="PNCP")
            st.download_button(
                "⬇️ Baixar XLSX",
                data=buf.getvalue(),
                file_name=f"pncp_{uf}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=False
            )

        status_text.success("Busca finalizada.")

    except Exception as e:
        status_text.error("Falha na busca. Tente novamente em instantes.")
        st.exception(e)
else:
    st.caption("Defina os filtros na barra lateral e clique em **Executar busca**.")
