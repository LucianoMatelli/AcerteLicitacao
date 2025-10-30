from __future__ import annotations

import os
import io
import re
import json
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

# ==========================
# Configuração de página
# ==========================
st.set_page_config(
    page_title="📑 Acerte Licitações — O seu Buscador de Editais",
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

ORIGIN = "https://pncp.gov.br"
BASE_API = ORIGIN + "/api/search"
HEADERS = {
    "User-Agent": "AcerteLicitacoes/1.1 (+streamlit)",
    "Referer": "https://pncp.gov.br/app/editais",
    "Accept-Language": "pt-BR,pt;q=0.9",
}
TAM_PAGINA_FIXO = 100  # parâmetro fixo de coleta
ENRICH_DELAY_S = 0.12  # pequena folga entre chamadas de enriquecimento

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
    """
    Preferência: https://pncp.gov.br/app/editais/{orgao_cnpj}/{ano}/{numero_sequencial}
    Fallback: converte quaisquer caminhos de '/compras/' para '/app/editais/'.
    """
    cnpj = str(item.get("orgao_cnpj", "") or "").strip()
    ano = str(item.get("ano", "") or "").strip()
    seq = str(item.get("numero_sequencial", "") or "").strip()
    if len(cnpj) == 14 and ano.isdigit() and seq:
        return f"{ORIGIN}/app/editais/{cnpj}/{ano}/{seq}"
    raw = item.get("item_url", "") or item.get("url", "") or ""
    url = _full_url(raw)
    url = url.replace("/app/compras/", "/app/editais/").replace("/compras/", "/app/editais/")
    return url

def _parse_valor_number(x) -> Optional[float]:
    """Aceita str '1.234,56' ou float/int; retorna float em R$."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

# ==========================
# Loaders
# ==========================
@st.cache_data(show_spinner=False)
def load_municipios_pncp() -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    seps = [",", ";", "	", "|"]
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
    seps = [",", ";", "	", "|"]
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
# Coleta PNCP (baseline funcional)
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

def _primeiro_valor(*args):
    for a in args:
        if a:
            return a
    return ""

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
        "_fim_raw": fim_raw,
        "_valor_estimado_search": item.get("valor_estimado_total") or item.get("valorTotalEstimado") or item.get("valorEstimado") or item.get("valor") or None,
        "_orgao_cnpj": item.get("orgao_cnpj") or item.get("orgaoCnpj") or "",
        "_ano": item.get("ano") or "",
        "_seq": item.get("numero_sequencial") or item.get("numeroSequencial") or "",
        "_id": item.get("id") or item.get("documentId") or item.get("documento_id") or "",
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

def _ensure_session_state():
    if "selected_municipios" not in st.session_state:
        st.session_state.selected_municipios = []  # list[{codigo_pncp,nome,uf}]
    if "saved_searches" not in st.session_state:
        st.session_state.saved_searches = _load_saved_searches()
    if "sidebar_inputs" not in st.session_state:
        st.session_state.sidebar_inputs = {
            "palavra_chave": "",
            "status_label": STATUS_LABELS[0],  # default
            "uf": "Todos",
            "save_name": "",
            "selected_saved": None,
            "enriquecer_valor": False,
        }
    if "card_page" not in st.session_state:
        st.session_state.card_page = 1
    if "page_size_cards" not in st.session_state:
        st.session_state.page_size_cards = 10
    if "results_df" not in st.session_state:
        st.session_state.results_df = None  # list[dict]
    if "results_signature" not in st.session_state:
        st.session_state.results_signature = None

def _build_signature(palavra_chave: str, status_value: str, enriquecer: bool) -> Dict:
    return {
        "municipios": [m["codigo_pncp"] for m in st.session_state.selected_municipios],
        "status": status_value or "",
        "q": (palavra_chave or "").strip().lower(),
        "enriquecer": bool(enriquecer),
    }

# ==========================
# Cache server-side por assinatura (coleta + enriquecimento opcional)
# ==========================
@st.cache_data(ttl=900, show_spinner=False)
def coletar_por_assinatura(signature: dict) -> pd.DataFrame:
    registros: List[Dict] = []
    codigos = signature.get("municipios", [])
    status_value = signature.get("status", "")
    for codigo in codigos:
        try:
            itens = consultar_pncp_por_municipio(codigo, status_value=status_value, tam_pagina=TAM_PAGINA_FIXO)
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

    if signature.get("enriquecer") and not df.empty:
        df["Valor estimado (R$)"] = None
        for idx, row in df.iterrows():
            v = _parse_valor_number(row.get("_valor_estimado_search"))
            if v is None:
                detail_val = _buscar_valor_estimado_por_det(row)
                v = detail_val
            if v is not None:
                df.at[idx, "Valor estimado (R$)"] = v
    return df

def _possiveis_endpoints_detalhe(row: pd.Series) -> List[Tuple[str, dict]]:
    cnpj = str(row.get("_orgao_cnpj") or "").strip()
    ano = str(row.get("_ano") or "").strip()
    seq = str(row.get("_seq") or "").strip()
    doc_id = str(row.get("_id") or "").strip()

    endpoints: List[Tuple[str, dict]] = []
    if cnpj and ano and seq:
        endpoints += [
            (f"{ORIGIN}/api/editais/{cnpj}/{ano}/{seq}", {}),
            (f"{ORIGIN}/api/licitacoes/{cnpj}/{ano}/{seq}", {}),
            (f"{ORIGIN}/api/documentos/edital/{cnpj}/{ano}/{seq}", {}),
        ]
    if doc_id:
        endpoints += [
            (f"{ORIGIN}/api/documentos/{doc_id}", {}),
            (f"{ORIGIN}/api/documentos/detalhe/{doc_id}", {}),
        ]
    link = row.get("Link para o edital") or ""
    if isinstance(link, str) and "/app/editais/" in link:
        endpoints.append((link + ("&" if "?" in link else "?") + "json=true", {}))
    return endpoints

def _from_detail_json_pegar_valor(js: dict) -> Optional[float]:
    if not isinstance(js, dict):
        return None
    keys_diretas = [
        "valor_estimado_total", "valorTotalEstimado", "valorEstimado",
        "valor_global_estimado", "valorGlobalEstimado", "valor_licitacao",
        "valorTotal", "valor",
    ]
    for k in keys_diretas:
        if k in js:
            v = _parse_valor_number(js.get(k))
            if v is not None:
                return v
    blocos = [
        js.get("edital") or js.get("licitacao") or js.get("documento") or {},
        js.get("dados") or {},
        js.get("metadados") or {},
    ]
    for bloco in blocos:
        if isinstance(bloco, dict):
            for k in keys_diretas:
                if k in bloco:
                    v = _parse_valor_number(bloco.get(k))
                    if v is not None:
                        return v

    total = 0.0
    achou_algo = False
    for arr_key in ["lotes", "itens", "itensLicitacao", "itens_licitacao"]:
        arr = js.get(arr_key)
        if isinstance(arr, list) and arr:
            for it in arr:
                if not isinstance(it, dict):
                    continue
                cand = (
                    it.get("valor_total") or it.get("valorTotal")
                    or it.get("valor_estimado") or it.get("valorEstimado")
                    or it.get("valor")
                )
                v = _parse_valor_number(cand)
                if v is None:
                    vu = _parse_valor_number(it.get("valor_unitario") or it.get("valorUnitario"))
                    qt = _parse_valor_number(it.get("quantidade") or it.get("qtd"))
                    if vu is not None and qt is not None:
                        v = vu * qt
                if v is not None:
                    total += v
                    achou_algo = True
    if achou_algo:
        return total
    return None

def _buscar_valor_estimado_por_det(row: pd.Series) -> Optional[float]:
    endpoints = _possiveis_endpoints_detalhe(row)
    for url, params in endpoints:
        try:
            r = requests.get(url, params=params or {}, headers=HEADERS, timeout=20)
            ct = r.headers.get("Content-Type", "")
            if "json" not in ct.lower():
                time.sleep(ENRICH_DELAY_S)
                continue
            js = r.json()
            v = _from_detail_json_pegar_valor(js)
            if v is not None:
                return v
        except Exception:
            pass
        time.sleep(ENRICH_DELAY_S)
    return None

# ==========================
# Sidebar (restaurada ao layout da versão anexa) — tudo em um único form
# ==========================
def _sidebar_form(pncp_df: pd.DataFrame, ibge_df: Optional[pd.DataFrame]):
    st.sidebar.header("🔎 Filtros")

    # Preparar listas de UF
    if ibge_df is not None:
        ufs = sorted(ibge_df["uf"].dropna().unique().tolist())
    else:
        ufs = sorted([u for u in pncp_df.get("uf", pd.Series([], dtype=str)).dropna().unique().tolist() if u])
    ufs = (["Todos"] + ufs) if ufs else ["Todos"]
    if st.session_state.sidebar_inputs["uf"] not in ufs:
        st.session_state.sidebar_inputs["uf"] = "Todos"

    with st.sidebar.form(key="filtros_form"):
        palavra = st.text_input(
            "Palavra-chave (aplicada no título/objeto após coleta)",
            value=st.session_state.sidebar_inputs["palavra_chave"]
        )

        status_label = st.radio(
            "Status",
            STATUS_LABELS,
            index=STATUS_LABELS.index(st.session_state.sidebar_inputs["status_label"]) if st.session_state.sidebar_inputs["status_label"] in STATUS_LABELS else 0,
            help="Agrupamentos mapeados para valores aceitos pela API do PNCP."
        )

        uf = st.selectbox("Estado (UF)", ufs, index=ufs.index(st.session_state.sidebar_inputs["uf"]))

        # Municípios (IBGE-like → PNCP), com "➕ Adicionar município" DENTRO do form
        st.markdown("**Municípios (máx. 25)**")
        if ibge_df is not None:
            df_show = ibge_df if uf == "Todos" else ibge_df[ibge_df["uf"] == uf]
            df_show = df_show.copy()
            df_show["label"] = df_show["municipio"] + " / " + df_show["uf"]
            mun_options = df_show[["municipio", "uf", "label"]].values.tolist()
        else:
            df_temp = pncp_df.copy()
            if uf != "Todos" and "uf" in df_temp.columns:
                df_temp = df_temp[df_temp["uf"].str.upper() == uf.upper()]
            df_temp["uf"] = df_temp.get("uf", "").astype(str).replace({"nan": ""})
            df_temp["label"] = df_temp["nome"] + " / " + df_temp["uf"]
            mun_options = df_temp[["nome", "uf", "label"]].values.tolist()

        labels = ["—"] + [row[2] for row in mun_options]
        chosen = st.selectbox("Adicionar município (IBGE)", labels, index=0)
        if chosen != "—":
            sel_row = next((row for row in mun_options if row[2] == chosen), None)
            if sel_row and st.form_submit_button("➕ Adicionar município"):
                nome_sel, uf_sel, _ = sel_row
                _add_municipio_by_name(nome_sel, uf_sel, pncp_df)
                st.session_state.sidebar_inputs["uf"] = uf  # manter UF selecionada
                st.session_state.sidebar_inputs["palavra_chave"] = palavra
                st.session_state.sidebar_inputs["status_label"] = status_label
                st.rerun()

        # Lista dos selecionados (só exibição)
        if st.session_state.selected_municipios:
            st.caption("Selecionados:")
            for m in st.session_state.selected_municipios:
                st.write(f"- {m['nome']} / {m.get('uf','')} ({m['codigo_pncp']})")

        # Salvar/Excluir pesquisa salva
        st.subheader("💾 Salvar/Excluir pesquisa salva")
        save_name = st.text_input("Nome da pesquisa", value=st.session_state.sidebar_inputs["save_name"])
        salvar = st.form_submit_button("Salvar")
        excluir = st.form_submit_button("Excluir")

        # Lista de pesquisas salvas
        st.subheader("📚 Pesquisas salvas")
        saved_names = sorted(list(st.session_state.saved_searches.keys()))
        selected_saved = st.selectbox("Carregar pesquisa", ["—"] + saved_names, index=0)
        carregar = st.form_submit_button("Carregar")

        # Enriquecimento (mantido, mas discreto, ainda dentro do form)
        enriquecer_valor = st.checkbox(
            "Incluir Valor Estimado (pode ser mais lento)",
            value=st.session_state.sidebar_inputs.get("enriquecer_valor", False)
        )

        # Botão principal — submit do form
        disparar_busca = st.form_submit_button("🔍 Pesquisar")

    # AÇÕES fora do form (igual versão anexa: apenas salvar/excluir/carregar persistência)
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
                "enriquecer_valor": bool(enriquecer_valor),
            }
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' salva.")

    if carregar:
        sel = selected_saved
        if sel and sel != "—":
            payload = st.session_state.saved_searches.get(sel, {})
            if payload:
                st.session_state.sidebar_inputs["palavra_chave"] = payload.get("palavra_chave", "")
                st.session_state.sidebar_inputs["status_label"] = payload.get("status_label", STATUS_LABELS[0]) if payload.get("status_label", STATUS_LABELS[0]) in STATUS_LABELS else STATUS_LABELS[0]
                st.session_state.sidebar_inputs["uf"] = payload.get("uf", "Todos")
                st.session_state.selected_municipios = payload.get("municipios", [])
                st.session_state.sidebar_inputs["save_name"] = sel
                st.session_state.sidebar_inputs["enriquecer_valor"] = payload.get("enriquecer_valor", False)
                st.sidebar.success(f"Pesquisa '{sel}' carregada.")
                st.rerun()

    # Atualiza inputs persistidos (sem disparar coleta ainda)
    st.session_state.sidebar_inputs["palavra_chave"] = palavra
    st.session_state.sidebar_inputs["status_label"] = status_label
    st.session_state.sidebar_inputs["uf"] = uf
    st.session_state.sidebar_inputs["save_name"] = save_name
    st.session_state.sidebar_inputs["selected_saved"] = selected_saved
    st.session_state.sidebar_inputs["enriquecer_valor"] = bool(enriquecer_valor)

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

def _remove_municipio(codigo: str):
    st.session_state.selected_municipios = [m for m in st.session_state.selected_municipios if m["codigo_pncp"] != codigo]

# ==========================
# UI principal
# ==========================
def main():
    st.title("📑 Acerte Licitações — O seu Buscador de Editais")
    st.caption("Fluxo funcional: /api/search (PNCP) + seleção IBGE→PNCP. Máx. 25 municípios.")

    # CSS (restaurado do anexo, com cards sutis azuis)
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] {
      background: #eef4ff !important;
      border-right: 1px solid #dfe8ff;
    }
    section[data-testid="stSidebar"] * { color: #0f2240 !important; }
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] textarea,
    section[data-testid="stSidebar"] select {
      background: #ffffff !important;
      color: #0f2240 !important;
      border: 1px solid #b7c6e6 !important;
      box-shadow: none !important;
    }
    header[data-testid="stHeader"] { background: transparent !important; box-shadow: none !important; height: 3rem; }
    div.block-container { padding-top: 2.25rem; background: #f9fbff; padding-bottom: 2rem; }
    .ac-card {
      background: #f7fbff;
      border: 1.5px solid #cfdcf6;
      border-radius: 18px;
      padding: 1rem 1.2rem;
      margin-bottom: 0.9rem;
      box-shadow: 0 1px 5px rgba(16, 38, 95, 0.05);
    }
    .ac-card h3 { margin-top: 0; margin-bottom: 0.25rem; font-size: 1.06rem; color: #0b1b36; }
    .ac-muted { color: #44516a; font-size: 0.92rem; }
    .ac-card a { color: #0b3b8a; border-color: #96b3e9 !important; }
    </style>
    """, unsafe_allow_html=True)

    _ensure_session_state()

    # Carregar bases
    try:
        pncp_df = load_municipios_pncp()
    except Exception as e:
        st.error(f"Erro ao carregar 'ListaMunicipiosPNCP.csv': {e}")
        st.stop()
    ibge_df = load_ibge_catalog()

    # Sidebar em formulário (restaurada)
    disparar_busca = _sidebar_form(pncp_df, ibge_df)

    # Monta assinatura e decide origem dos dados (cache vs memória)
    status_value = STATUS_MAP.get(st.session_state.sidebar_inputs["status_label"], "")
    palavra_chave = (st.session_state.sidebar_inputs["palavra_chave"] or "").strip()
    enriquecer = bool(st.session_state.sidebar_inputs.get("enriquecer_valor", False))
    signature = {
        "municipios": [m["codigo_pncp"] for m in st.session_state.selected_municipios],
        "status": status_value,
        "q": palavra_chave.lower(),
        "enriquecer": enriquecer,
    }

    if disparar_busca:
        if not signature["municipios"]:
            st.warning("Selecione pelo menos um município para pesquisar.")
            st.stop()
        with st.spinner("Coletando dados no PNCP..."):
            df = coletar_por_assinatura(signature)
        st.session_state.results_df = df.to_dict("records")
        st.session_state.results_signature = signature
        st.session_state.card_page = 1  # reinicia paginação a cada nova coleta
    else:
        if st.session_state.results_df is None:
            st.info("Configure os filtros e clique em **Pesquisar**.")
            st.stop()
        df = pd.DataFrame(st.session_state.results_df)

        if st.session_state.results_signature and signature != st.session_state.results_signature:
            st.warning("Filtros alterados após a última coleta. Clique em **Pesquisar** para atualizar os resultados.")

    # ===== Renderização =====
    st.subheader(f"Resultados ({len(df)})")
    if df.empty:
        st.info("Nenhum resultado encontrado com os critérios atuais.")
        return

    if "_pub_dt" not in df.columns:
        try:
            df["_pub_dt"] = pd.to_datetime(df["_pub_raw"], errors="coerce", utc=False)
        except Exception:
            df["_pub_dt"] = pd.NaT
        df.sort_values("_pub_dt", ascending=False, na_position="last", inplace=True)
        df.reset_index(drop=True, inplace=True)

    page_size_cards = st.selectbox(
        "Itens por página",
        [10, 20, 50],
        index=[10, 20, 50].index(st.session_state.get("page_size_cards", 10)) if st.session_state.get("page_size_cards", 10) in [10, 20, 50] else 0,
        key="page_size_cards",
    )
    total_items = len(df)
    total_pages = max(1, (total_items + page_size_cards - 1) // page_size_cards)

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_a:
        prev_clicked = st.button("◀ Anterior", key="prev_top", disabled=(st.session_state.get("card_page", 1) <= 1))
    with col_c:
        next_clicked = st.button("Próxima ▶", key="next_top", disabled=(st.session_state.get("card_page", 1) >= total_pages))

    if "card_page" not in st.session_state:
        st.session_state.card_page = 1
    if prev_clicked:
        st.session_state.card_page = max(1, st.session_state.card_page - 1)
    if next_clicked:
        st.session_state.card_page = min(total_pages, st.session_state.card_page + 1)
    if st.session_state.card_page > total_pages:
        st.session_state.card_page = total_pages

    start = (st.session_state.card_page - 1) * page_size_cards
    end = start + page_size_cards
    with col_b:
        st.markdown(f"**Página {st.session_state.card_page} de {total_pages}**")

    page_df = df.iloc[start:end].copy()

    # ====== CARDS ======
    for _, row in page_df.iterrows():
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
        valor_est = row.get('Valor estimado (R$)')

        valor_html = ""
        if valor_est is not None and valor_est != "":
            try:
                valor_html = f"<div><strong>Valor estimado:</strong> R$ {float(valor_est):,.2f}</div>".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception:
                valor_html = f"<div><strong>Valor estimado:</strong> {valor_est}</div>"

        processo_html = f'<div class="ac-muted">Processo: {proc}</div>' if proc else '<div></div>'

        html = f"""
        <div class="ac-card">
            <h3>{titulo}</h3>
            <div class="ac-muted">
                <strong>Cidade/UF:</strong> {cidade} / {uf} &nbsp;|&nbsp;
                <strong>Publicação:</strong> {pub} &nbsp;|&nbsp;
                <strong>Fim do envio:</strong> {fim}
            </div>
            <div style="margin-top:0.5rem;"><strong>Objeto:</strong> {objeto}</div>
            <div style="display:flex; gap:1rem; margin-top:0.5rem; flex-wrap:wrap;">
                <div><strong>Modalidade:</strong> {modalidade}</div>
                <div><strong>Tipo:</strong> {tipo}</div>
                <div><strong>Órgão:</strong> {orgao}</div>
                {valor_html}
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; margin-top:0.6rem;">
                {processo_html}
                {f'<a href="{link}" target="_blank" style="text-decoration:none; padding:0.45rem 0.8rem; border-radius:10px; border:1px solid #96b3e9;">Abrir edital</a>' if isinstance(link, str) and link else ''}
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)

    # Paginação (rodapé)
    col_a2, col_b2, col_c2 = st.columns([1, 2, 1])
    with col_a2:
        prev_clicked2 = st.button("◀ Anterior", key="prev_bottom", disabled=(st.session_state.card_page <= 1))
    with col_c2:
        next_clicked2 = st.button("Próxima ▶", key="next_bottom", disabled=(st.session_state.card_page >= total_pages))
    if prev_clicked2:
        st.session_state.card_page = max(1, st.session_state.card_page - 1)
    if next_clicked2:
        st.session_state.card_page = min(total_pages, st.session_state.card_page + 1)
    if st.session_state.card_page > total_pages:
        st.session_state.card_page = total_pages
    with col_b2:
        st.markdown(f"**Página {st.session_state.card_page} de {total_pages}**")

    st.divider()

    # ===== Exportação XLSX (sem colunas técnicas) =====
    drop_cols = [c for c in ["_pub_raw", "_fim_raw", "_pub_dt", "_valor_estimado_search", "_orgao_cnpj", "_ano", "_seq", "_id"] if c in df.columns]
    export_df = df.drop(columns=drop_cols).copy()
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
