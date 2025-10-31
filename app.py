# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import io
import re
import json
import time
import unicodedata
import hashlib
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

# ==========================
# Configuração de página
# ==========================
st.set_page_config(
    page_title="Acerte Licitações — Seu Buscador de Editais",
    page_icon="📑",
    layout="wide",
)

# ==========================
# Constantes e caminhos
# ==========================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PNCP_PATHS = [
    os.path.join(DATA_DIR, "ListaMunicipiosPNCP.csv"),
    "ListaMunicipiosPNCP.csv",
]
CSV_IBGE_PATHS = [
    os.path.join(DATA_DIR, "IBGE_Municipios.csv"),
    "IBGE_Municipios.csv",
]
SAVED_SEARCHES_PATH = os.path.join(BASE_DIR, "saved_searches.json")
SAVED_TR_PATH = os.path.join(BASE_DIR, "tr_marks.json")
SAVED_NA_PATH = os.path.join(BASE_DIR, "na_marks.json")

ORIGIN = "https://pncp.gov.br"
BASE_API = ORIGIN + "/api/search"
HEADERS = {
    "User-Agent": "AcerteLicitacoes/1.1 (+streamlit)",
    "Referer": "https://pncp.gov.br/app/editais",
    "Accept-Language": "pt-BR,pt;q=0.9",
}
TAM_PAGINA_FIXO = 100

STATUS_LABELS = [
    "A Receber/Recebendo Proposta",
    "Em Julgamento/Propostas Encerradas",
    "Encerradas",
    "Todos",
]
STATUS_MAP = {
    "A Receber/Recebendo Proposta": "recebendo_proposta",
    "Em Julgamento/Propostas Encerradas": "em_julgamento",
    "Encerradas": "encerrado",
    "Todos": "",
}
UF_PLACEHOLDER = "— Selecione a UF —"

# ==========================
# Utilitários
# ==========================
def _norm(s: str) -> str:
    s = str(s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _items_from_json(js) -> List[Dict]:
    if isinstance(js, dict):
        for k in ["items", "results", "conteudo", "licitacoes", "data", "documents", "documentos", "content", "resultados"]:
            v = js.get(k)
            if isinstance(v, list):
                return v
    if isinstance(js, list):
        return js
    return []

def _fmt_dt_iso_to_br(dt: str) -> str:
    if not dt:
        return ""
    try:
        ts = pd.to_datetime(dt, errors="coerce", utc=False)
        if pd.isna(ts):
            return ""
        return ts.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""

def _full_url(item_url: str) -> str:
    if not item_url:
        return ""
    if isinstance(item_url, str) and item_url.startswith("http"):
        return item_url
    return ORIGIN.rstrip("/") + "/" + str(item_url).lstrip("/")

def _build_pncp_link(item: Dict) -> str:
    cnpj = str(item.get("orgao_cnpj", "") or "").strip()
    ano = str(item.get("ano", "") or "").strip()
    seq = str(item.get("numero_sequencial", "") or "").strip()
    if len(cnpj) == 14 and ano.isdigit() and seq:
        return f"{ORIGIN}/app/editais/{cnpj}/{ano}/{seq}"
    raw = item.get("item_url", "") or item.get("url", "") or ""
    url = _full_url(raw)
    url = url.replace("/app/compras/", "/app/editais/").replace("/compras/", "/app/editais/")
    return url

def _primeiro_valor(*args):
    for a in args:
        if a:
            return a
    return ""

def _uid_from_row(row: Dict) -> str:
    cnpj = str(row.get("_orgao_cnpj") or "").strip()
    ano = str(row.get("_ano") or "").strip()
    seq = str(row.get("_seq") or "").strip()
    if len(cnpj) == 14 and ano.isdigit() and seq:
        return f"{cnpj}-{ano}-{seq}"
    link = str(row.get("Link para o edital") or "")
    m = re.search(r"/app/editais/(\d{14})/(\d{4})/(\w+)", link)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    base = f"{row.get('Título','')}-{row.get('municipio_codigo','')}-{row.get('_pub_raw','')}-{row.get('Orgão','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

# ==========================
# Loaders
# ==========================
@st.cache_data(show_spinner=False)
def load_municipios_pncp() -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    seps = [",", ";", "\t", "|"]
    last_err = None

    def _guess_columns(df: pd.DataFrame):
        cols_norm = {_norm(c): c for c in df.columns}
        col_nome = cols_norm.get("municipio") or cols_norm.get("nome") or ("Municipio" if "Municipio" in df.columns else None)
        col_codigo = cols_norm.get("id") or cols_norm.get("codigo") or ("id" if "id" in df.columns else None)
        col_uf = cols_norm.get("uf") or cols_norm.get("estado") or None
        return col_nome, col_codigo, col_uf

    for path in CSV_PNCP_PATHS:
        if os.path.exists(path):
            for enc in encodings:
                for sep in seps:
                    try:
                        df = pd.read_csv(path, dtype=str, sep=sep, encoding=enc, engine="python", on_bad_lines="skip")
                        if df is None or df.shape[0] == 0 or df.shape[1] == 0:
                            continue
                        col_nome, col_codigo, col_uf = _guess_columns(df)
                        if not col_nome or not col_codigo:
                            raise ValueError("Não foi possível detectar colunas de 'Municipio' (nome) e 'id' (código PNCP).")
                        out = pd.DataFrame({
                            "nome": df[col_nome].astype(str).str.strip(),
                            "codigo_pncp": df[col_codigo].astype(str).str.strip(),
                        })
                        out["uf"] = df[col_uf].astype(str).str.strip() if col_uf else ""
                        out["nome_norm"] = out["nome"].map(_norm)
                        out = out[out["codigo_pncp"] != ""].drop_duplicates(subset=["codigo_pncp"]).reset_index(drop=True)
                        return out
                    except Exception as e:
                        last_err = e
                        continue
    if last_err:
        raise last_err
    raise FileNotFoundError("ListaMunicipiosPNCP.csv não encontrada em ./data ou na raiz do projeto.")

@st.cache_data(show_spinner=False)
def load_ibge_catalog() -> Optional[pd.DataFrame]:
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    seps = [",", ";", "\t", "|"]
    for path in CSV_IBGE_PATHS:
        if os.path.exists(path):
            for enc in encodings:
                for sep in seps:
                    try:
                        df = pd.read_csv(path, dtype=str, sep=sep, encoding=enc, engine="python", on_bad_lines="skip")
                        if df is None or df.shape[0] == 0 or df.shape[1] < 2:
                            continue
                        cols = {c.lower().strip(): c for c in df.columns}
                        col_uf = next((cols[k] for k in cols if k in ["uf", "sigla_uf", "estado"]), None)
                        col_mun = next((cols[k] for k in cols if k in ["municipio", "município", "nome"]), None)
                        if not col_uf or not col_mun:
                            continue
                        out = pd.DataFrame({
                            "uf": df[col_uf].astype(str).str.strip().str.upper(),
                            "municipio": df[col_mun].astype(str).str.strip(),
                        })
                        out["municipio_norm"] = out["municipio"].map(_norm)
                        out = out.drop_duplicates(subset=["uf", "municipio_norm"]).reset_index(drop=True)
                        return out
                    except Exception:
                        continue
    return None

# ==========================
# Coleta PNCP
# ==========================
def consultar_pncp_por_municipio(
    municipio_id: str,
    status_value: str = "recebendo_proposta",
    tam_pagina: int = TAM_PAGINA_FIXO,
    delay_s: float = 0.05,
) -> List[Dict]:
    out: List[Dict] = []
    pagina = 1
    while True:
        params = {
            "tipos_documento": "edital",
            "ordenacao": "-data",
            "pagina": pagina,
            "tam_pagina": tam_pagina,
            "municipios": municipio_id,
        }
        if status_value:
            params["status"] = status_value

        r = requests.get(BASE_API, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        js = r.json()
        itens = _items_from_json(js)

        if not itens:
            break

        out.extend(itens)
        if len(itens) < tam_pagina:
            break

        pagina += 1
        time.sleep(delay_s)
    return out

def montar_registro(item: Dict, municipio_codigo: str) -> Dict:
    pub_raw = item.get("data_publicacao_pncp") or item.get("data") or item.get("dataPublicacao") or ""
    fim_raw = item.get("data_fim_vigencia") or item.get("fimEnvioProposta") or ""

    processo = _primeiro_valor(
        item.get("numeroProcesso"),
        item.get("processo"),
        item.get("numero_processo"),
        item.get("numeroProcessoLicitatorio"),
        item.get("numero_processo_licitatorio"),
        item.get("processoLicitatorio"),
        item.get("numProcesso"),
        item.get("processNumber"),
        item.get("numero_processo_adm"),
        item.get("numeroProcessoAdministrativo"),
    )

    orgao_cnpj = str(item.get("orgao_cnpj") or "").strip()
    ano = str(item.get("ano") or "").strip()
    seq = str(item.get("numero_sequencial") or "").strip()
    raw_id = str(item.get("id") or item.get("uuid") or "")

    return {
        "municipio_codigo": municipio_codigo,
        "Cidade": item.get("municipio_nome", ""),
        "UF": item.get("uf", ""),
        "Título": item.get("title", "") or item.get("titulo", ""),
        "Objeto": item.get("description", "") or item.get("objeto", ""),
        "Link para o edital": _build_pncp_link(item),
        "Modalidade": item.get("modalidade_licitacao_nome", ""),
        "Tipo": item.get("tipo_nome", ""),
        "Tipo (documento)": item.get("document_type", ""),
        "Orgão": item.get("orgao_nome", "") or item.get("orgao", ""),
        "Unidade": item.get("unidade_nome", ""),
        "Esfera": item.get("esfera_nome", ""),
        "Publicação": _fmt_dt_iso_to_br(pub_raw),
        "Fim do envio de proposta": _fmt_dt_iso_to_br(fim_raw),
        "numero_processo": str(processo or "").strip(),
        "_pub_raw": pub_raw,
        "_orgao_cnpj": orgao_cnpj,
        "_ano": ano,
        "_seq": seq,
        "_id": raw_id,
    }

# ==========================
# Persistência e estado
# ==========================
def _load_saved_searches() -> Dict[str, Dict]:
    if os.path.exists(SAVED_SEARCHES_PATH):
        try:
            with open(SAVED_SEARCHES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _persist_saved_searches(d: Dict[str, Dict]):
    try:
        with open(SAVED_SEARCHES_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"Falha ao salvar pesquisas: {e}")

def _load_marks(path: str) -> Dict[str, bool]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {str(k): bool(v) for k, v in data.items()}
        except Exception:
            pass
    return {}

def _persist_marks(path: str, d: Dict[str, bool]):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"Falha ao salvar marcações em {os.path.basename(path)}: {e}")

def _ensure_session_state():
    if "selected_municipios" not in st.session_state:
        st.session_state.selected_municipios = []
    if "saved_searches" not in st.session_state:
        st.session_state.saved_searches = _load_saved_searches()
    if "sidebar_inputs" not in st.session_state:
        st.session_state.sidebar_inputs = {
            "palavra_chave": "",
            "status_label": STATUS_LABELS[0],
            "uf": UF_PLACEHOLDER,
            "save_name": "",
            "selected_saved": None,
        }
    if "uf_prev" not in st.session_state:
        st.session_state.uf_prev = UF_PLACEHOLDER
    if "municipio_nonce" not in st.session_state:
        st.session_state.municipio_nonce = 0
    if "card_page" not in st.session_state:
        st.session_state.card_page = 1
    if "page_size_cards" not in st.session_state:
        st.session_state.page_size_cards = 10
    if "results_df" not in st.session_state:
        st.session_state.results_df = None
    if "results_signature" not in st.session_state:
        st.session_state.results_signature = None
    if "tr_marks" not in st.session_state:
        st.session_state.tr_marks = _load_marks(SAVED_TR_PATH)
    if "na_marks" not in st.session_state:
        st.session_state.na_marks = _load_marks(SAVED_NA_PATH)

# ==========================
# Coleta agregada (CACHE)
# ==========================
@st.cache_data(ttl=900, show_spinner=False)
def coletar_por_assinatura(signature: dict) -> pd.DataFrame:
    registros: List[Dict] = []
    codigos = signature.get("municipios", [])
    status_value = signature.get("status", "")
    for codigo in codigos:
        try:
            itens = consultar_pncp_por_municipio(
                codigo, status_value=status_value, tam_pagina=TAM_PAGINA_FIXO
            )
        except Exception:
            itens = []
        for it in itens:
            registros.append(montar_registro(it, codigo))

    df = pd.DataFrame(registros)
    q = (signature.get("q") or "").strip()
    if q and not df.empty:
        mask = (
            df["Título"].fillna("").str.contains(q, case=False, na=False)
            | df["Objeto"].fillna("").str.contains(q, case=False, na=False)
        )
        df = df[mask].copy()

    try:
        df["_pub_dt"] = pd.to_datetime(df["_pub_raw"], errors="coerce", utc=False)
    except Exception:
        df["_pub_dt"] = pd.NaT
    df.sort_values("_pub_dt", ascending=False, na_position="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# ==========================
# Sidebar
# ==========================
def _sidebar(pncp_df: pd.DataFrame, ibge_df: Optional[pd.DataFrame]):
    st.sidebar.header("🔎 Filtros")

    palavra = st.sidebar.text_input(
        "Palavra-chave (aplicada no título/objeto)",
        value=st.session_state.sidebar_inputs["palavra_chave"],
        key="palavra_chave_input",
    )
    status_label = st.sidebar.radio(
        "Status",
        STATUS_LABELS,
        index=STATUS_LABELS.index(st.session_state.sidebar_inputs["status_label"]) if st.session_state.sidebar_inputs["status_label"] in STATUS_LABELS else 0,
        key="status_radio",
        help="Selecione em qual etapa (status) quer encontrar os Editais no PNCP.",
    )

    if ibge_df is not None:
        ufs = sorted(ibge_df["uf"].dropna().unique().tolist())
    else:
        ufs = sorted([u for u in pncp_df.get("uf", pd.Series([], dtype=str)).dropna().unique().tolist() if u])

    ufs = [UF_PLACEHOLDER] + ufs

    uf = st.sidebar.selectbox(
        "Estado (UF) — Obrigatório",
        ufs,
        index=ufs.index(st.session_state.sidebar_inputs["uf"]) if st.session_state.sidebar_inputs["uf"] in ufs else 0,
        key="uf_select",
    )

    if uf != st.session_state.uf_prev:
        st.session_state.uf_prev = uf
        st.session_state.municipio_nonce += 1

    st.sidebar.markdown("**Municípios (máximo 25)**")
    if uf == UF_PLACEHOLDER:
        st.sidebar.info("Selecione um Estado (UF) para habilitar a seleção de municípios.")
        chosen = None
        add_clicked = False
    else:
        if ibge_df is not None:
            df_show = ibge_df[ibge_df["uf"] == uf].copy()
            df_show["label"] = df_show["municipio"] + " / " + df_show["uf"]
            mun_options = df_show[["municipio", "uf", "label"]].values.tolist()
        else:
            df_temp = pncp_df.copy()
            if "uf" in df_temp.columns:
                df_temp = df_temp[df_temp["uf"].str.upper() == uf.upper()]
            df_temp["label"] = df_temp["nome"] + " / " + df_temp.get("uf", "")
            mun_options = df_temp[["nome", "uf", "label"]].values.tolist()

        labels = ["—"] + [row[2] for row in mun_options]
        chosen = st.sidebar.selectbox(
            "Selecione a(s) cidade(s) e adicione a pesquisa",
            labels,
            index=0,
            key=f"municipio_select_{st.session_state.municipio_nonce}",
        )
        add_clicked = st.sidebar.button(
            "➕ Adicionar município", use_container_width=True, key=f"add_mun_btn_{st.session_state.municipio_nonce}"
        )

    if add_clicked:
        if chosen == "—":
            st.sidebar.warning("Selecione um município antes de adicionar.")
        else:
            sel_row = next((row for row in mun_options if row[2] == chosen), None)
            if sel_row:
                nome_sel, uf_sel, _ = sel_row
                _add_municipio_by_name(nome_sel, uf_sel, pncp_df)

    if st.session_state.selected_municipios:
        st.sidebar.caption("Municípios Selecionados:")
        keep_list = []
        for m in st.session_state.selected_municipios:
            c1, c2 = st.sidebar.columns([0.82, 0.18])
            with c1:
                st.markdown(f"- **{m['nome']}** / {m.get('uf','')} (`{m['codigo_pncp']}`)")
            with c2:
                if st.button("✕", key=f"rm_{m['codigo_pncp']}", help=f"Remover {m['nome']}"):
                    pass
                else:
                    keep_list.append(m)
        if len(keep_list) != len(st.session_state.selected_municipios):
            st.session_state.selected_municipios = keep_list
            st.rerun()

    st.sidebar.subheader("💾 Pesquisa salva")
    save_name = st.sidebar.text_input(
        "Nome da pesquisa", value=st.session_state.sidebar_inputs["save_name"], key="save_name_input"
    )
    col_s1, col_s2 = st.sidebar.columns(2)
    with col_s1:
        salvar = st.button("Salvar", use_container_width=True, key="btn_salvar")
    with col_s2:
        excluir = st.button("Excluir", use_container_width=True, key="btn_excluir")

    if excluir:
        name = save_name.strip()
        if name and name in st.session_state.saved_searches:
            del st.session_state.saved_searches[name]
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' excluída.")
        else:
            st.sidebar.error("Informe o nome exato de uma pesquisa salva para excluir.")

    if salvar:
        name = save_name.strip()
        if not name:
            st.sidebar.error("Informe um nome para salvar.")
        else:
            st.session_state.saved_searches[name] = {
                "palavra_chave": palavra,
                "status_label": status_label,
                "uf": uf,
                "municipios": st.session_state.selected_municipios,
            }
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' salva.")

    st.sidebar.subheader("📚 Pesquisas salvas")
    saved_names = sorted(list(st.session_state.saved_searches.keys()))
    selected_saved = st.sidebar.selectbox("Carregar pesquisa", ["—"] + saved_names, index=0, key="select_saved")
    carregar = st.sidebar.button("Carregar", use_container_width=True, key="btn_carregar")

    if carregar:
        sel = selected_saved
        if sel and sel != "—":
            payload = st.session_state.saved_searches.get(sel, {})
            if payload:
                st.session_state.sidebar_inputs["palavra_chave"] = payload.get("palavra_chave", "")
                st.session_state.sidebar_inputs["status_label"] = (
                    payload.get("status_label", STATUS_LABELS[0])
                    if payload.get("status_label", STATUS_LABELS[0]) in STATUS_LABELS
                    else STATUS_LABELS[0]
                )
                st.session_state.sidebar_inputs["uf"] = payload.get("uf", UF_PLACEHOLDER)
                st.session_state.uf_prev = st.session_state.sidebar_inputs["uf"]
                st.session_state.municipio_nonce += 1
                st.session_state.selected_municipios = payload.get("municipios", [])
                st.session_state.sidebar_inputs["save_name"] = sel
                st.sidebar.success(f"Pesquisa '{sel}' carregada.")
                st.rerun()

    st.session_state.sidebar_inputs["palavra_chave"] = palavra
    st.session_state.sidebar_inputs["status_label"] = status_label
    st.session_state.sidebar_inputs["uf"] = uf
    st.session_state.sidebar_inputs["save_name"] = save_name
    st.session_state.sidebar_inputs["selected_saved"] = selected_saved

    disparar_busca = st.sidebar.button("🔎 Pesquisar", use_container_width=True, type="primary", key="btn_pesquisar")
    if disparar_busca and uf == UF_PLACEHOLDER:
        st.sidebar.error("Selecione uma UF para habilitar a pesquisa.")
        disparar_busca = False

    return disparar_busca

# ==========================
# Helpers de municípios
# ==========================
def _add_municipio_by_name(nome_municipio: str, uf: Optional[str], pncp_df: pd.DataFrame) -> None:
    if not nome_municipio:
        return
    sel = st.session_state.selected_municipios
    if len(sel) >= 25:
        st.warning("Limite de 25 municípios por pesquisa atingido.")
        return
    nome_norm = _norm(nome_municipio)
    candidates = pncp_df.copy()
    if "uf" in candidates.columns and uf and uf != "Todos":
        candidates = candidates[candidates["uf"].str.upper() == str(uf).upper()]
    candidates = candidates[candidates["nome_norm"] == nome_norm]
    if candidates.empty:
        candidates = pncp_df[pncp_df["nome_norm"] == nome_norm]
    if candidates.empty:
        st.error(f"Não localizei o município '{nome_municipio}' na planilha PNCP para resolver o código.")
        return
    row = candidates.iloc[0]
    codigo = row["codigo_pncp"]
    nome = row["nome"]
    uf_val = row.get("uf", uf or "")
    if codigo in [m["codigo_pncp"] for m in sel]:
        return
    sel.append({"codigo_pncp": codigo, "nome": nome, "uf": uf_val})

# ==========================
# Callbacks de paginação
# ==========================
def _cb_prev(total_pages: int):
    st.session_state.card_page = max(1, int(st.session_state.get("card_page", 1)) - 1)

def _cb_next(total_pages: int):
    st.session_state.card_page = min(total_pages, int(st.session_state.get("card_page", 1)) + 1)

def _cb_page_size_change():
    st.session_state.card_page = 1

# ==========================
# UI principal
# ==========================
def main():
    st.title("📑 Acerte Licitações — O seu Buscador de Editais")
    st.caption("Doutor Bro, o seu irmão na licitação: Selecione os municípios e acompanhe seus Editais.")

    # CSS mínimo para manter visual
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] { background: #eef4ff !important; border-right: 1px solid #dfe8ff; min-width: 360px !important; max-width: 360px !important; }
        section[data-testid="stSidebar"] * { color: #112a52 !important; }
        header[data-testid="stHeader"] { background: transparent !important; box-shadow: none !important; height: 3rem; }
        div.block-container { padding-top: 2.1rem; background: #f7faff; padding-bottom: 2rem; }
        .ac-card { background: #f8fbff; border: 1.25px solid #cad9f3; border-radius: 18px; padding: 1.05rem 1.2rem; margin-bottom: 1rem; box-shadow: 0 8px 20px rgba(20, 45, 110, 0.06); }
        .ac-card h3 { margin: 0 0 0.25rem 0; font-size: 1.08rem; color: #0b1b36; }
        .ac-muted { color: #415477; font-size: 0.92rem; }
        .ac-badge { background: #eaf1ff; border: 1px solid #bcd0f7; color: #0b3b8a; padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.82rem; }
        .ac-flag { background: #e3f9e5; border: 1px solid #57b26a; color: #1b6f37; padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.82rem; margin-left: 0.5rem; }
        .ac-flag-na { background: #fde8e7; border: 1px solid #dc5a5a; color: #9b1c1c; padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.82rem; margin-left: 0.5rem; }
        .ac-link { text-decoration:none; padding:0.46rem 0.85rem; border-radius:10px; border:1px solid #96b3e9; color:#0b3b8a; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    _ensure_session_state()

    try:
        pncp_df = load_municipios_pncp()
    except Exception as e:
        st.error(f"Erro ao carregar 'ListaMunicipiosPNCP.csv': {e}")
        st.stop()
    ibge_df = load_ibge_catalog()

    disparar_busca = _sidebar(pncp_df, ibge_df)

    status_value = STATUS_MAP.get(st.session_state.sidebar_inputs["status_label"], "")
    palavra_chave = (st.session_state.sidebar_inputs["palavra_chave"] or "").strip()
    signature = {
        "municipios": [m["codigo_pncp"] for m in st.session_state.selected_municipios],
        "status": status_value,
        "q": palavra_chave.lower(),
    }

    if disparar_busca:
        if not signature["municipios"]:
            st.warning("Selecione pelo menos um município para pesquisar.")
            st.stop()
        with st.spinner("Coletando dados no PNCP..."):
            df = coletar_por_assinatura(signature)
        st.session_state.results_df = df.to_dict("records")
        st.session_state.results_signature = signature
        st.session_state.card_page = 1
    else:
        if st.session_state.results_df is None:
            st.info("Configure os filtros e clique em **Pesquisar**.")
            st.stop()
        df = pd.DataFrame(st.session_state.results_df)
        if st.session_state.results_signature and signature != st.session_state.results_signature:
            st.warning("Filtros alterados após a última coleta. Clique em **Pesquisar** para atualizar.")

    st.subheader(f"Resultados ({len(df)})")
    if df.empty:
        st.info("Nenhum resultado encontrado.")
        return

    if "_pub_dt" not in df.columns:
        try:
            df["_pub_dt"] = pd.to_datetime(df["_pub_raw"], errors="coerce", utc=False)
        except Exception:
            df["_pub_dt"] = pd.NaT
        df.sort_values("_pub_dt", ascending=False, na_position="last", inplace=True)
        df.reset_index(drop=True, inplace=True)

    # ===== Paginação
    _ = st.selectbox(
        "Itens por página",
        [10, 20, 50],
        index=[10, 20, 50].index(st.session_state.get("page_size_cards", 10)) if st.session_state.get("page_size_cards", 10) in [10, 20, 50] else 0,
        key="page_size_cards",
        on_change=_cb_page_size_change,
    )
    page_size_cards = int(st.session_state.get("page_size_cards", 10))

    total_items = len(df)
    total_pages = max(1, (total_items + page_size_cards - 1) // page_size_cards)

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_a:
        st.button("◀ Anterior", key="prev_top", disabled=(st.session_state.get("card_page", 1) <= 1),
                  on_click=_cb_prev, kwargs={"total_pages": total_pages})
    with col_c:
        st.button("Próxima ▶", key="next_top", disabled=(st.session_state.get("card_page", 1) >= total_pages),
                  on_click=_cb_next, kwargs={"total_pages": total_pages})
    with col_b:
        st.markdown(f"**Página {st.session_state.get('card_page',1)} de {total_pages}**")

    start = (st.session_state.get("card_page", 1) - 1) * page_size_cards
    end = start + page_size_cards
    page_df = df.iloc[start:end].copy()

    # ====== CARDS ======
    for _, row in page_df.iterrows():
        uid = _uid_from_row(row)
        tr_flag = bool(st.session_state.tr_marks.get(uid, False))
        na_flag = bool(st.session_state.na_marks.get(uid, False))

        col_spacer, col_cb_tr, col_cb_na = st.columns([6, 1.3, 1.3])
        with col_cb_tr:
            new_tr = st.checkbox("TR Elaborado", value=tr_flag, key=f"tr_{uid}")
        with col_cb_na:
            new_na = st.checkbox("Não Atende", value=na_flag, key=f"na_{uid}")

        updated = False
        if new_tr != tr_flag:
            st.session_state.tr_marks[uid] = bool(new_tr)
            _persist_marks(SAVED_TR_PATH, st.session_state.tr_marks)
            updated = True
        if new_na != na_flag:
            st.session_state.na_marks[uid] = bool(new_na)
            _persist_marks(SAVED_NA_PATH, st.session_state.na_marks)
            updated = True
        if updated:
            st.rerun()

        link = row.get('Link para o edital','')
        titulo = row.get('Título') or '(Sem título)'
        cidade = row.get('Cidade','')
        uf = row.get('UF','')
        pub = row.get('Publicação','')
        fim = row.get('Fim do envio de proposta','')
        objeto = row.get('Objeto','')
        modalidade = row.get('Modalidade','')
        tipo = row.get('Tipo','')
        orgao = row.get('Orgão','')
        proc = (row.get('numero_processo') or '').strip()

        tr_badge = '<span class="ac-flag">TR Elaborado</span>' if new_tr else ''
        na_badge = '<span class="ac-flag-na">Não Atende</span>' if new_na else ''
        processo_html = f'<div class="ac-muted">Processo: {proc}</div>' if proc else '<div></div>'

        html = f"""
        <div class="ac-card">
            <h3>{titulo} {tr_badge} {na_badge}</h3>
            <div class="ac-muted">
                <span class="ac-badge">{cidade} / {uf}</span>
                &nbsp;•&nbsp;
                <strong>Publicação:</strong> {pub}
                &nbsp;|&nbsp;
                <strong>Fim do envio:</strong> {fim}
            </div>
            <div style="margin-top:0.55rem;"><strong>Objeto:</strong> {objeto}</div>
            <div style="display:flex; gap:1rem; margin-top:0.55rem; flex-wrap:wrap;">
                <div><strong>Modalidade:</strong> {modalidade}</div>
                <div><strong>Tipo:</strong> {tipo}</div>
                <div><strong>Órgão:</strong> {orgao}</div>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; margin-top:0.7rem;">
                {processo_html}
                {f'<a class="ac-link" href="{link}" target="_blank">Abrir edital</a>' if isinstance(link, str) and link else ''}
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)

    # Paginação (rodapé)
    col_a2, col_b2, col_c2 = st.columns([1, 2, 1])
    with col_a2:
        st.button("◀ Anterior", key="prev_bottom", disabled=(st.session_state.get("card_page", 1) <= 1),
                  on_click=_cb_prev, kwargs={"total_pages": total_pages})
    with col_c2:
        st.button("Próxima ▶", key="next_bottom", disabled=(st.session_state.get("card_page", 1) >= total_pages),
                  on_click=_cb_next, kwargs={"total_pages": total_pages})
    with col_b2:
        st.markdown(f"**Página {st.session_state.get('card_page',1)} de {total_pages}**")

    st.divider()

    drop_cols = [c for c in ["_pub_raw", "_fim_raw", "_pub_dt", "_orgao_cnpj", "_ano", "_seq", "_id"] if c in df.columns]
    export_df = df.drop(columns=[c for c in drop_cols if c in df.columns]).copy()
    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as wr:
        export_df.to_excel(wr, index=False, sheet_name="PNCP")
    xlsx_bytes = xlsx_buf.getvalue()

    st.markdown("### ⬇️ Baixar planilha")
    st.download_button(
        "Baixar XLSX",
        data=xlsx_bytes,
        file_name=f"pncp_resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary",
    )

if __name__ == "__main__":
    main()
