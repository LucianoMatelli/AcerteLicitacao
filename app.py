
import os
import json
import io
import time
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
    "ListaMunicipiosPNCP.csv",  # fallback no diretório raiz
]
IBGE_CSV_PATHS = [
    os.path.join(DATA_DIR, "IBGE_Municipios.csv"),
    "IBGE_Municipios.csv",
]
SAVED_SEARCHES_PATH = os.path.join(BASE_DIR, "saved_searches.json")

# ==============================
# PNCP: endpoints e parâmetros
# ==============================
# Cadeia de endpoints candidatos (tentará em ordem até obter 200 OK)
PNCP_ENDPOINTS = [
    "https://www.pncp.gov.br/api/consulta/v1/licitacoes",
    "https://pncp.gov.br/api/consulta/v1/licitacoes",
    "https://www.pncp.gov.br/api/search",
    "https://pncp.gov.br/api/search",
]

TAM_PAGINA_FIXO = 100  # baseline aprovado

# UI -> Backend: mapeamento de status
# O filtro da UI tem 4 opções. Cada uma mapeia para 1..n valores plausíveis para a API.
STATUS_LABELS = [
    "A Receber/Recebendo Proposta",
    "Em Julgamento/Propostas Encerradas",
    "Encerradas",
    "Todos",
]

STATUS_MAP = {
    "A Receber/Recebendo Proposta": [
        # valores usuais/observados
        "ABERTO", "RECEBENDO_PROPOSTA", "RECEBENDO", "A_RECEBER", "PUBLICADO"
    ],
    "Em Julgamento/Propostas Encerradas": [
        "EM_JULGAMENTO", "PROPOSTAS_ENCERRADAS", "EM_ANALISE"
    ],
    "Encerradas": [
        "ENCERRADO", "HOMOLOGADO", "CONCLUIDO", "CANCELADO", "ANULADO", "REVOGADO"
    ],
    "Todos": []
}


# ==============================
# Utilidades
# ==============================
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


def _build_pncp_link(item: Dict) -> str:
    for k in ["url", "link", "href"]:
        if k in item and isinstance(item[k], str) and item[k].startswith("http"):
            return item[k]
    for k in ["id", "identificador", "processoId", "numeroProcesso"]:
        if k in item and str(item[k]).strip():
            return f"https://pncp.gov.br/app/editais/{item[k]}"
    return ""


def _pncp_params(query: str, status_label: str, codigo_municipio: str, page: int) -> Dict:
    """
    Monta parâmetros com mapeamento de status e chaves alternativas para municipio.
    """
    status_values = STATUS_MAP.get(status_label, [])
    params = {
        "q": query or "",
        "page": page,
        "size": TAM_PAGINA_FIXO,
        # Alguns endpoints usam 'status', outros 'situacao'; enviamos ambos.
        "status": ",".join(status_values) if status_values else None,
        "situacao": ",".join(status_values) if status_values else None,
        # chaves alternativas para município
        "codigoMunicipio": codigo_municipio,
        "codigo_municipio": codigo_municipio,
        "municipioCodigo": codigo_municipio,
        "codigoIbge": codigo_municipio,
    }
    return {k: v for k, v in params.items() if v not in [None, ""]}


def _try_fetch_with_fallback(params: Dict) -> Dict:
    """
    Tenta múltiplos endpoints até obter 200 OK.
    Preserva a semântica: GET com querystring.
    """
    last_exc = None
    for url in PNCP_ENDPOINTS:
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return {"ok": True, "url": url, "json": resp.json()}
            else:
                last_exc = Exception(f"{resp.status_code} {resp.reason} @ {url}")
        except Exception as e:
            last_exc = e
            continue
    raise RuntimeError(f"Falha nas tentativas de consulta PNCP. Último erro: {last_exc}")


def _collect_results(query: str, status_label: str, codigos_municipio: List[str]) -> pd.DataFrame:
    registros = []
    progress = st.progress(0.0, text="Iniciando varredura nos municípios selecionados...")
    total = len(codigos_municipio)

    for idx, cod in enumerate(codigos_municipio, start=1):
        progress.progress(idx / total, text=f"Consultando município código {cod} ({idx}/{total})")
        page = 1
        while True:
            params = _pncp_params(query, status_label, cod, page=page)
            try:
                res = _try_fetch_with_fallback(params)
                data = res.get("json", {})
            except Exception as e:
                st.warning(f"Falha ao consultar município {cod} na página {page}: {e}")
                break

            items = []
            if isinstance(data, dict):
                if "content" in data and isinstance(data["content"], list):
                    items = data["content"]
                elif "items" in data and isinstance(data["items"], list):
                    items = data["items"]
                elif "resultados" in data and isinstance(data["resultados"], list):
                    items = data["resultados"]
                elif "results" in data and isinstance(data["results"], list):
                    items = data["results"]
            elif isinstance(data, list):
                items = data

            if not items:
                break

            for it in items:
                registros.append({
                    "municipio_codigo": cod,
                    "titulo": it.get("titulo") or it.get("title") or it.get("objeto") or "",
                    "orgao": it.get("orgao") or it.get("unidadeGestora") or it.get("entidade") or "",
                    "status": it.get("status") or it.get("situacao") or "",
                    "data_publicacao": it.get("dataPublicacao") or it.get("data") or it.get("createdAt") or "",
                    "numero_processo": it.get("numeroProcesso") or it.get("processo") or "",
                    "link": _build_pncp_link(it),
                    "raw": json.dumps(it, ensure_ascii=False)
                })

            if len(items) < TAM_PAGINA_FIXO:
                break
            page += 1

    progress.empty()
    if not registros:
        return pd.DataFrame(columns=["municipio_codigo", "titulo", "orgao", "status",
                                     "data_publicacao", "numero_processo", "link"])
    df = pd.DataFrame(registros)
    if "data_publicacao" in df.columns and df["data_publicacao"].notna().any():
        try:
            df["_dt"] = pd.to_datetime(df["data_publicacao"], errors="coerce")
            df = df.sort_values(by=["_dt"], ascending=False).drop(columns=["_dt"])
        except Exception:
            pass
    return df


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
        st.session_state.selected_municipios = []
    if "saved_searches" not in st.session_state:
        st.session_state.saved_searches = _load_saved_searches()
    if "sidebar_inputs" not in st.session_state:
        st.session_state.sidebar_inputs = {
            "palavra_chave": "",
            "status": STATUS_LABELS[-1],  # "Todos"
            "uf": "Todos",
            "save_name": "",
            "selected_saved": None,
        }


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
    if len(candidates) > 1:
        st.info(f"Foram encontradas {len(candidates)} entradas para '{nome_municipio}'. Usarei a primeira ocorrência.")
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

    # 1) Palavra-chave
    st.session_state.sidebar_inputs["palavra_chave"] = st.sidebar.text_input("Palavra-chave", value=st.session_state.sidebar_inputs["palavra_chave"])

    # 2) Status (NOVO layout/labels conforme solicitação)
    st.session_state.sidebar_inputs["status"] = st.sidebar.radio(
        "Status",
        STATUS_LABELS,
        index=STATUS_LABELS.index(st.session_state.sidebar_inputs["status"]) if st.session_state.sidebar_inputs["status"] in STATUS_LABELS else STATUS_LABELS.index("Todos"),
        help="Ajuste de exibição: quatro grupos funcionais. Mapeamento interno para valores da API."
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

    # 4) Municípios (IBGE-like → PNCP code)
    st.sidebar.markdown("**Municípios (máx. 25)**")
    if ibge_df is not None:
        df_show = ibge_df if st.session_state.sidebar_inputs["uf"] == "Todos" else ibge_df[ibge_df["uf"] == st.session_state.sidebar_inputs["uf"]]
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
                "status": st.session_state.sidebar_inputs["status"],
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
                    st.session_state.sidebar_inputs["status"] = payload.get("status", "Todos") if payload.get("status", "Todos") in STATUS_LABELS else "Todos"
                    st.session_state.sidebar_inputs["uf"] = payload.get("uf", "Todos")
                    st.session_state.selected_municipios = payload.get("municipios", [])
                    st.session_state.sidebar_inputs["save_name"] = sel
                    st.sidebar.success(f"Pesquisa '{sel}' carregada.")
                    st.rerun()
    else:
        st.sidebar.caption("Nenhuma pesquisa salva até o momento.")

    pesquisar = st.sidebar.button("🔍 Pesquisar", use_container_width=True)
    return pesquisar


# ==============================
# UI principal
# ==============================
def main():
    st.title("📑 Acerte Licitações — O seu Buscador de Editais")
    st.caption("Sidebar com Status (4 grupos), UF→Município (IBGE-like) e mapeamento para código PNCP. Máx. 25 municípios.")

    _ensure_session_state()

    try:
        pncp_df = load_municipios_pncp()
    except Exception as e:
        st.error(f"Erro ao carregar 'ListaMunicipiosPNCP.csv': {e}")
        st.stop()

    ibge_df = load_ibge_catalog()

    disparar_busca = _sidebar(pncp_df, ibge_df)

    with st.expander("Configuração atual", expanded=False):
        st.write({
            "palavra_chave": st.session_state.sidebar_inputs["palavra_chave"],
            "status": st.session_state.sidebar_inputs["status"],
            "uf": st.session_state.sidebar_inputs["uf"],
            "municipios_selecionados": st.session_state.selected_municipios,
        })

    if disparar_busca:
        if not st.session_state.selected_municipios:
            st.warning("Selecione pelo menos um município para pesquisar.")
            st.stop()

        codigos = [m["codigo_pncp"] for m in st.session_state.selected_municipios]
        query = st.session_state.sidebar_inputs["palavra_chave"]
        status_label = st.session_state.sidebar_inputs["status"]

        with st.spinner("Consultando PNCP..."):
            df = _collect_results(query=query, status_label=status_label, codigos_municipio=codigos)

        st.subheader("Resultados")
        if df.empty:
            st.info("Nenhum resultado encontrado para os critérios informados.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Resultados")
            buffer.seek(0)
            st.download_button(
                label="⬇️ Baixar XLSX",
                data=buffer,
                file_name=f"pncp_resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    st.markdown("---")
    st.caption("Compat: tenta múltiplos endpoints PNCP. Se você confirmar o endpoint exato e os nomes de parâmetros, eu ajusto o mapeamento de forma determinística.")


if __name__ == "__main__":
    main()
