
import os
import json
import io
import re
import unicodedata
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import requests
import streamlit as st


# ==============================
# Configuração geral do app
# ==============================
st.set_page_config(
    page_title="📑 Acerte Licitações — O seu Buscador de Editais",
    page_icon="📑",
    layout="wide"
)

# Caminhos e constantes
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATHS = [
    os.path.join(DATA_DIR, "ListaMunicipiosPNCP.csv"),
    "ListaMunicipiosPNCP.csv",
]
IBGE_CSV_PATHS = [
    os.path.join(DATA_DIR, "IBGE_Municipios.csv"),
    "IBGE_Municipios.csv",
]
SAVED_SEARCHES_PATH = os.path.join(BASE_DIR, "saved_searches.json")

# ==============================
# PNCP: endpoint e headers (baseline funcional)
# ==============================
ORIGIN = "https://pncp.gov.br"
BASE_API = ORIGIN + "/api/search"
HEADERS = {"User-Agent": "AcerteLicitacoes/1.0 (+streamlit)"}

TAM_PAGINA_FIXO = 100

# UI -> grupos de status
STATUS_LABELS = [

    "A Receber/Recebendo Proposta",
    "Em Julgamento/Propostas Encerradas",
    "Encerradas",
    "Todos",
]

# Mapeamento flexível apenas para exibir; /api/search geralmente ignora/tem outra semântica
STATUS_MAP = {
    "A Receber/Recebendo Proposta": "recebendo_proposta",
    "Em Julgamento/Propostas Encerradas": "em_julgamento",
    "Encerradas": "encerrado",
    "Todos": "",
}
# ==============================
# Utilidades
# ==============================

def _items_from_json(js) -> list[dict]:
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

def _build_pncp_link(item: dict) -> str:
    \"\"\"
    Preferência: https://pncp.gov.br/app/editais/{orgao_cnpj}/{ano}/{numero_sequencial}
    Fallback: converte qualquer '/compras/' para '/app/editais/'.
    \"\"\"
    cnpj = str(item.get("orgao_cnpj", "") or "").strip()
    ano = str(item.get("ano", "") or "").strip()
    seq = str(item.get("numero_sequencial", "") or "").strip()
    if len(cnpj) == 14 and ano.isdigit() and seq:
        return f\"{ORIGIN}/app/editais/{cnpj}/{ano}/{seq}\"

    raw = item.get("item_url", "") or item.get("url", "") or ""
    url = _full_url(raw)
    url = url.replace("/app/compras/", "/app/editais/").replace("/compras/", "/app/editais/")
    return url

def _norm(s: str) -> str:
    s = str(s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _guess_columns(df: pd.DataFrame):
    if df is None or df.shape[1] == 0:
        return None, None, None

    norm_map = {_norm(c): c for c in df.columns}
    nome_keys = ["nome", "municipio", "municipio_", "municipio__"]
    codigo_keys = ["codigo_pncp", "codigo", "id", "pncp", "codigo_pncp_", "codigo_municipio"]
    uf_keys = ["uf", "estado", "sigla_uf", "uf_sigla"]

    col_nome = next((norm_map[k] for k in nome_keys if k in norm_map), None)
    col_codigo = next((norm_map[k] for k in codigo_keys if k in norm_map), None)
    col_uf = next((norm_map[k] for k in uf_keys if k in norm_map), None)
    return col_nome, col_codigo, col_uf


@st.cache_data(show_spinner=False)
def load_municipios_pncp() -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    seps = [",", ";", "\t", "|"]
    last_err = None

    for path in CSV_PATHS:
        if os.path.exists(path):
            for enc in encodings:
                for sep in seps:
                    try:
                        df = pd.read_csv(path, dtype=str, sep=sep, encoding=enc, engine="python", on_bad_lines="skip")
                        if df is None or df.shape[1] == 0 or df.shape[0] == 0:
                            continue

                        col_nome, col_codigo, col_uf = _guess_columns(df)
                        if not col_nome and "Municipio" in df.columns:
                            col_nome = "Municipio"
                        if not col_codigo and "id" in df.columns:
                            col_codigo = "id"

                        if not col_nome or not col_codigo:
                            try:
                                c1, c2 = df.columns[:2]
                                col_nome = col_nome or c1
                                col_codigo = col_codigo or c2
                            except Exception:
                                pass

                        if not col_nome or not col_codigo:
                            raise ValueError("Não foi possível detectar colunas de 'nome' e/ou 'código PNCP' no CSV.")

                        out = pd.DataFrame({
                            "nome": df[col_nome].astype(str).str.strip(),
                            "codigo_pncp": df[col_codigo].astype(str).str.strip()
                        })
                        if col_uf and col_uf in df.columns:
                            out["uf"] = df[col_uf].astype(str).str.strip()
                        else:
                            out["uf"] = ""

                        out["nome_norm"] = out["nome"].map(_norm)
                        out = out[out["codigo_pncp"] != ""]
                        out = out.drop_duplicates(subset=["codigo_pncp"]).reset_index(drop=True)
                        return out
                    except Exception as e:
                        last_err = e
                        continue

    if last_err:
        raise last_err
    raise FileNotFoundError("ListaMunicipiosPNCP.csv não encontrada. Coloque o arquivo em ./data ou na raiz do projeto.")


@st.cache_data(show_spinner=False)
def load_ibge_catalog() -> Optional[pd.DataFrame]:
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    seps = [",", ";", "\t", "|"]
    for path in IBGE_CSV_PATHS:
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
                        out = out.drop_duplicates(subset=["uf","municipio_norm"]).reset_index(drop=True)
                        return out
                    except Exception:
                        continue
    return None


# ==============================
# Consulta legado /api/search
# ==============================
def _compose_param_sets(query: str, codigo_municipio_pncp: str, page: int):
    # Baseline funcional: apenas parâmetros mínimos exigidos pelo /api/search
    return [{
        "term": (query or ""),
        "pagina": page,
        "tamanhoPagina": TAM_PAGINA_FIXO,
        "municipioId": codigo_municipio_pncp,
    }]

def _fetch_search_page(query: str, status_label: str, codigo_municipio_pncp: str, page: int) -> Dict:
    params = _compose_param_sets(query, codigo_municipio_pncp, page)[0]
    resp = requests.get(PNCP_SEARCH_URL, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"{resp.status_code} {resp.reason} @ {PNCP_SEARCH_URL} {params}")
    data = resp.json()
    # Aceita dict com chaves conhecidas ou lista
    if isinstance(data, dict):
        if any(k in data for k in ["content", "items", "resultados", "results"]):
            return {"ok": True, "params": params, "data": data}
    elif isinstance(data, list):
        return {"ok": True, "params": params, "data": data}
    # Payload inesperado
    return {"ok": True, "params": params, "data": data}
def _build_pncp_link(item: Dict) -> str:
    for k in ["url", "link", "href"]:
        if k in item and isinstance(item[k], str) and item[k].startswith("http"):
            return item[k]
    for k in ["id", "identificador", "processoId", "numeroProcesso"]:
        if k in item and str(item[k]).strip():
            return f"https://pncp.gov.br/app/editais/{item[k]}"
    return ""


def consultar_pncp_por_municipio(
    municipio_id: str,
    status_value: str = "recebendo_proposta",
    tam_pagina: int = TAM_PAGINA_FIXO,
    delay_s: float = 0.05,
) -> list[dict]:
    out: list[dict] = []
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

def _collect_results(query: str, status_label: str, codigos_municipio: list[str]) -> pd.DataFrame:
