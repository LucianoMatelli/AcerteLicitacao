
from __future__ import annotations

import os
import io
import re
import json
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

# ==========================
# Configuração básica
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
    "User-Agent": "AcerteLicitacoes/1.0 (+streamlit)",
    "Referer": "https://pncp.gov.br/app/editais",
    "Accept-Language": "pt-BR,pt;q=0.9",
}
TAM_PAGINA_FIXO = 100  # valor fixo para a API

STATUS_LABELS = [
    "A Receber/Recebendo Proposta",
    "Em Julgamento/Propostas Encerradas",
    "Encerradas",
    "Todos",
]
# Mapeamento UI → valor PNCP
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
    Fallback: converte qualquer '/compras/' para '/app/editais/'.
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
# Coleta via API (baseline funcional)
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
        "numero_processo": item.get("numeroProcesso") or item.get("processo") or "",
        # colunas auxiliares (não vão para o XLSX)
        "_pub_raw": pub_raw,
        "_fim_raw": fim_raw,
    }

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
        st.session_state.selected_municipios = []  # list[{"codigo_pncp","nome","uf"}]
    if "saved_searches" not in st.session_state:
        st.session_state.saved_searches = _load_saved_searches()
    if "sidebar_inputs" not in st.session_state:
        st.session_state.sidebar_inputs = {
            "palavra_chave": "",
            "status_label": STATUS_LABELS[0],  # default solicitado
            "uf": "Todos",
            "save_name": "",
            "selected_saved": None,
        }
    if "card_page" not in st.session_state:
        st.session_state.card_page = 1
    if "page_size_cards" not in st.session_state:
        st.session_state.page_size_cards = 10

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

def _sidebar(pncp_df: pd.DataFrame, ibge_df: Optional[pd.DataFrame]):
    st.sidebar.header("🔎 Filtros")

    # 1) Palavra-chave (filtro client-side)
    st.session_state.sidebar_inputs["palavra_chave"] = st.sidebar.text_input(
        "Palavra-chave (aplicada no título/objeto após coleta)",
        value=st.session_state.sidebar_inputs["palavra_chave"]
    )

    # 2) Status (radio) default “A Receber/Recebendo Proposta”
    st.session_state.sidebar_inputs["status_label"] = st.sidebar.radio(
        "Status",
        STATUS_LABELS,
        index=STATUS_LABELS.index(st.session_state.sidebar_inputs["status_label"]) if st.session_state.sidebar_inputs["status_label"] in STATUS_LABELS else 0,
        help="Agrupamentos de exibição; mapeados para valores aceitos pela API do PNCP."
    )

    # 3) Estado (UF)
    if ibge_df is not None:
        ufs = sorted(ibge_df["uf"].dropna().unique().tolist())
    else:
        ufs = sorted([u for u in pncp_df.get("uf", pd.Series([], dtype=str)).dropna().unique().tolist() if u])
    ufs = (["Todos"] + ufs) if ufs else ["Todos"]
    if st.session_state.sidebar_inputs["uf"] not in ufs:
        st.session_state.sidebar_inputs["uf"] = "Todos"
    st.session_state.sidebar_inputs["uf"] = st.sidebar.selectbox("Estado (UF)", ufs, index=ufs.index(st.session_state.sidebar_inputs["uf"]))

    # 4) Municípios (IBGE-like → PNCP)
    st.sidebar.markdown("**Municípios (máx. 25)**")
    if ibge_df is not None:
        df_show = ibge_df if st.session_state.sidebar_inputs["uf"] == "Todos" else ibge_df[ibge_df["uf"] == st.session_state.sidebar_inputs["uf"]]
        df_show = df_show.copy()
        df_show["label"] = df_show["municipio"] + " / " + df_show["uf"]
        mun_options = df_show[["municipio", "uf", "label"]].values.tolist()
    else:
        df_temp = pncp_df.copy()
        if st.session_state.sidebar_inputs["uf"] != "Todos" and "uf" in df_temp.columns:
            df_temp = df_temp[df_temp["uf"].str.upper() == st.session_state.sidebar_inputs["uf"].upper()]
        df_temp["uf"] = df_temp.get("uf", "").astype(str).replace({"nan": ""})
        df_temp["label"] = df_temp["nome"] + " / " + df_temp["uf"]
        mun_options = df_temp[["nome", "uf", "label"]].values.tolist()

    labels = ["—"] + [row[2] for row in mun_options]
    chosen = st.sidebar.selectbox("Adicionar município (IBGE)", labels, index=0)
    if chosen != "—":
        sel_row = next((row for row in mun_options if row[2] == chosen), None)
        if sel_row:
            nome_sel, uf_sel, _ = sel_row
            if st.sidebar.button("➕ Adicionar", use_container_width=True):
                _add_municipio_by_name(nome_sel, uf_sel, pncp_df)
                st.rerun()

    if st.session_state.selected_municipios:
        st.sidebar.caption("Selecionados:")
        for m in st.session_state.selected_municipios:
            cols = st.sidebar.columns([0.8, 0.2])
            uf_tag = f" / {m.get('uf','')}" if m.get('uf') else ""
            cols[0].write(f"- {m['nome']}{uf_tag} ({m['codigo_pncp']})")
            if cols[1].button("✖", key=f"rm_{m['codigo_pncp']}"):
                _remove_municipio(m["codigo_pncp"])
                st.rerun()

    # 5) Salvar/Excluir pesquisa salva
    st.sidebar.subheader("💾 Salvar/Excluir pesquisa salva")
    st.session_state.sidebar_inputs["save_name"] = st.sidebar.text_input("Nome da pesquisa", value=st.session_state.sidebar_inputs["save_name"])
    btn_cols = st.sidebar.columns(2)
    if btn_cols[0].button("Salvar", use_container_width=True):
        name = st.session_state.sidebar_inputs["save_name"].strip()
        if not name:
            st.sidebar.error("Informe um nome para salvar.")
        else:
            st.session_state.saved_searches[name] = {
                "palavra_chave": st.session_state.sidebar_inputs["palavra_chave"],
                "status_label": st.session_state.sidebar_inputs["status_label"],
                "uf": st.session_state.sidebar_inputs["uf"],
                "municipios": st.session_state.selected_municipios,
            }
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' salva.")

    if btn_cols[1].button("Excluir", use_container_width=True):
        name = st.session_state.sidebar_inputs["save_name"].strip()
        if name and name in st.session_state.saved_searches:
            del st.session_state.saved_searches[name]
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' excluída.")
        else:
            st.sidebar.error("Informe o nome exato de uma pesquisa salva para excluir.")

    # 6) Lista de pesquisas salvas
    st.sidebar.subheader("📚 Pesquisas salvas")
    saved_names = sorted(list(st.session_state.saved_searches.keys()))
    if saved_names:
        st.session_state.sidebar_inputs["selected_saved"] = st.sidebar.selectbox("Carregar pesquisa", ["—"] + saved_names, index=0)
        if st.sidebar.button("Carregar", use_container_width=True):
            sel = st.session_state.sidebar_inputs["selected_saved"]
            if sel and sel != "—":
                payload = st.session_state.saved_searches.get(sel, {})
                if payload:
                    st.session_state.sidebar_inputs["palavra_chave"] = payload.get("palavra_chave", "")
                    st.session_state.sidebar_inputs["status_label"] = payload.get("status_label", STATUS_LABELS[0]) if payload.get("status_label", STATUS_LABELS[0]) in STATUS_LABELS else STATUS_LABELS[0]
                    st.session_state.sidebar_inputs["uf"] = payload.get("uf", "Todos")
                    st.session_state.selected_municipios = payload.get("municipios", [])
                    st.session_state.sidebar_inputs["save_name"] = sel
                    st.sidebar.success(f"Pesquisa '{sel}' carregada.")
                    st.rerun()
    else:
        st.sidebar.caption("Nenhuma pesquisa salva até o momento.")

    # Botão principal
    pesquisar = st.sidebar.button("🔍 Pesquisar", use_container_width=True)
    return pesquisar

# ==========================
# UI principal
# ==========================
def main():
    st.title("📑 Acerte Licitações — O seu Buscador de Editais")
    st.caption("Fluxo funcional: /api/search (PNCP) + seleção IBGE→PNCP. Máx. 25 municípios.")

    # CSS sutil: sidebar clara; evitar sobreposição do header; cards em azul suave
    st.markdown("""
    <style>
    /* Subtle sidebar theme */
    section[data-testid="stSidebar"] {
      background: #eef4ff !important;
      border-right: 1px solid #dfe8ff;
    }
    section[data-testid="stSidebar"] * { color: #0f2240 !important; }
    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] textarea {
      background: #ffffff !important;
      color: #0f2240 !important;
      border: 1px solid #b7c6e6 !important;
      box-shadow: none !important;
    }
    section[data-testid="stSidebar"] input::placeholder,
    section[data-testid="stSidebar"] textarea::placeholder {
      color: #5a6a8a !important; opacity: .9;
    }

    /* Main header & container spacing: avoid overlap */
    header[data-testid="stHeader"] { background: transparent !important; box-shadow: none !important; height: 3rem; }
    div.block-container { padding-top: 2.25rem; background: #f9fbff; padding-bottom: 2rem; }

    /* Cards */
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

    # Estado
    _ensure_session_state()

    # Carregar bases
    try:
        pncp_df = load_municipios_pncp()
    except Exception as e:
        st.error(f"Erro ao carregar 'ListaMunicipiosPNCP.csv': {e}")
        st.stop()

    ibge_df = load_ibge_catalog()

    # Sidebar
    disparar_busca = _sidebar(pncp_df, ibge_df)

    # Execução
    if disparar_busca:
        if not st.session_state.selected_municipios:
            st.warning("Selecione pelo menos um município para pesquisar.")
            st.stop()

        status_value = STATUS_MAP.get(st.session_state.sidebar_inputs["status_label"], "")
        palavra_chave = (st.session_state.sidebar_inputs["palavra_chave"] or "").strip()

        registros: List[Dict] = []
        progress = st.progress(0.0, text="Iniciando varredura nos municípios selecionados...")
        total = len(st.session_state.selected_municipios)

        for i, m in enumerate(st.session_state.selected_municipios, start=1):
            progress.progress(i / total, text=f"Consultando {m['nome']} ({i}/{total})")
            try:
                itens = consultar_pncp_por_municipio(m["codigo_pncp"], status_value=status_value, tam_pagina=TAM_PAGINA_FIXO)
            except Exception as e:
                st.warning(f"Falha ao consultar {m['nome']}: {e}")
                continue
            for it in itens:
                registros.append(montar_registro(it, m["codigo_pncp"]))

        progress.empty()
        df = pd.DataFrame(registros)

        # Filtro por palavra-chave (client-side no título/objeto)
        if palavra_chave and not df.empty:
            mask = (
                df["Título"].fillna("").str.contains(palavra_chave, case=False, na=False)
                | df["Objeto"].fillna("").str.contains(palavra_chave, case=False, na=False)
            )
            df = df[mask].copy()

        st.subheader(f"Resultados ({len(df)})")
        if df.empty:
            st.info("Nenhum resultado encontrado com os critérios atuais.")
        else:
            # Ordenação correta por data de publicação (desc)
            try:
                df["_pub_dt"] = pd.to_datetime(df["_pub_raw"], errors="coerce", utc=False)
            except Exception:
                df["_pub_dt"] = pd.NaT
            df = df.sort_values("_pub_dt", ascending=False, na_position="last").reset_index(drop=True)

            # Tamanho da página controlado pelo widget (NÃO escrever no session_state com a mesma key!)
            page_size_cards = st.selectbox(
                "Itens por página",
                [10, 20, 50],
                index=[10, 20, 50].index(st.session_state.get("page_size_cards", 10)) if st.session_state.get("page_size_cards", 10) in [10, 20, 50] else 0,
                key="page_size_cards",
            )

            total_items = len(df)
            total_pages = max(1, (total_items + page_size_cards - 1) // page_size_cards)

            # Controles de paginação (topo)
            col_a, col_b, col_c = st.columns([1, 2, 1])
            with col_a:
                prev_clicked = st.button("◀ Anterior", key="prev_top", disabled=(st.session_state.get("card_page", 1) <= 1))
            with col_c:
                next_clicked = st.button("Próxima ▶", key="next_top", disabled=(st.session_state.get("card_page", 1) >= total_pages))

            # Atualiza página e faz clamp
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
                proc = row.get('numero_processo','')

                html = f'''
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
                    </div>
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-top:0.6rem;">
                        <div class="ac-muted">Processo: {proc}</div>
                        {f'<a href="{link}" target="_blank" style="text-decoration:none; padding:0.45rem 0.8rem; border-radius:10px; border:1px solid #96b3e9;">Abrir edital</a>' if isinstance(link, str) and link else ''}
                    </div>
                </div>
                '''
                st.markdown(html, unsafe_allow_html=True)

            # Controles de paginação (rodapé)
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

            # Download XLSX (sem colunas auxiliares)
            export_df = df.drop(columns=[c for c in ["_pub_raw", "_fim_raw", "_pub_dt"] if c in df.columns]).copy()
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
