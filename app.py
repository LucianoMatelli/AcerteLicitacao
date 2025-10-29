
import os
import json
import io
import time
from datetime import datetime
from typing import List, Dict

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
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CSV_PATHS = [
    os.path.join(DATA_DIR, "ListaMunicipiosPNCP.csv"),
    "ListaMunicipiosPNCP.csv",  # fallback no diretório raiz
]
SAVED_SEARCHES_PATH = os.path.join(os.path.dirname(__file__), "saved_searches.json")

PNCP_API_URL = "https://pncp.gov.br/api/search"
TAM_PAGINA_FIXO = 100  # conforme baseline aprovado

STATUS_OPCOES = [
    "Qualquer",
    "PUBLICADO",
    "EM_ANDAMENTO",
    "SUSPENSO",
    "REVOGADO",
    "ANULADO",
    "HOMOLOGADO",
    "ENCERRADO",
]


# ==============================
# Utilidades
# ==============================
@st.cache_data(show_spinner=False)
def load_municipios() -> pd.DataFrame:
    """
    Carrega a lista completa de municípios com seus códigos PNCP.
    Estrutura esperada de colunas (tenta detectar automaticamente):
    - código PNCP do município (ex.: 'codigo_pncp' ou 'codigo' ou 'id' etc.)
    - nome do município (ex.: 'nome' ou 'municipio')
    - UF (ex.: 'uf' ou 'estado')
    """
    last_err = None
    for path in CSV_PATHS:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, dtype=str, sep=",")
                # Normaliza colunas
                cols = {c.lower().strip(): c for c in df.columns}
                # Nome
                col_nome = next((cols[k] for k in cols if k in ["nome", "municipio", "município"]), None)
                # Código PNCP
                col_codigo = next((cols[k] for k in cols if k in ["codigo_pncp", "codigo", "código", "id", "pncp", "codigopncp"]), None)
                # UF
                col_uf = next((cols[k] for k in cols if k in ["uf", "estado", "sigla_uf"]), None)

                # Valida detecção mínima
                if not col_nome or not col_codigo:
                    raise ValueError("Não foi possível detectar colunas de 'nome' e/ou 'código PNCP' no CSV.")

                # Cria dataframe padronizado
                out = pd.DataFrame({
                    "nome": df[col_nome].astype(str).str.strip(),
                    "codigo_pncp": df[col_codigo].astype(str).str.strip()
                })
                if col_uf and col_uf in df.columns:
                    out["uf"] = df[col_uf].astype(str).str.strip()
                else:
                    # Se não houver UF no CSV, tenta inferir do nome (não ideal, mas evita quebra)
                    out["uf"] = ""

                # Remove linhas vazias de código
                out = out[out["codigo_pncp"] != ""]
                # Dedup
                out = out.drop_duplicates(subset=["codigo_pncp"]).reset_index(drop=True)
                return out
            except Exception as e:
                last_err = e
                continue
    if last_err:
        raise last_err
    raise FileNotFoundError("ListaMunicipiosPNCP.csv não encontrada. Coloque o arquivo em ./data ou na raiz do projeto.")


def _build_pncp_link(item: Dict) -> str:
    """
    Normaliza link do resultado do PNCP quando houver identificadores úteis na resposta.
    Como o schema pode variar, fazemos o melhor esforço.
    """
    # Tenta campos comuns
    for k in ["url", "link", "href"]:
        if k in item and isinstance(item[k], str) and item[k].startswith("http"):
            return item[k]

    # Fallback best-effort: se houver um identificador, monta link de detalhe
    # (URL meramente ilustrativa — ajuste se necessário conforme sua resposta real da API)
    for k in ["id", "identificador", "processoId", "numeroProcesso"]:
        if k in item and str(item[k]).strip():
            return f"https://pncp.gov.br/app/editais/{item[k]}"
    return ""


def _pncp_params_baseline(query: str, status: str, codigo_municipio: str, page: int = 1) -> Dict:
    """
    Monta os parâmetros de consulta conforme baseline anterior, com TAM_PAGINA_FIXO=100.
    Observação: o PNCP pode usar chaves específicas como 'codigoMunicipio' ou similares.
    Mantemos 'codigo_municipio' e aliases para robustez.
    """
    params = {
        "q": query or "",
        "status": "" if status == "Qualquer" else status,
        "page": page,
        "size": TAM_PAGINA_FIXO,
        # chaves alternativas comuns — a API deve ignorar desconhecidas
        "codigoMunicipio": codigo_municipio,
        "codigo_municipio": codigo_municipio,
        "municipioCodigo": codigo_municipio,
    }
    # Remove entradas vazias
    return {k: v for k, v in params.items() if v not in [None, ""]}


def _fetch_pncp_page(params: Dict) -> Dict:
    """Executa uma página da busca no endpoint do PNCP."""
    resp = requests.get(PNCP_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _collect_results(query: str, status: str, codigos_municipio: List[str]) -> pd.DataFrame:
    """
    Itera municípios selecionados e agrega resultados em um único DataFrame.
    Pressupõe que a resposta traga lista em campo 'content' ou 'items' (ajuste se necessário).
    """
    registros = []
    progress = st.progress(0.0, text="Iniciando varredura nos municípios selecionados...")
    total = len(codigos_municipio)

    for idx, cod in enumerate(codigos_municipio, start=1):
        progress.progress(idx / total, text=f"Consultando município código {cod} ({idx}/{total})")
        page = 1
        while True:
            params = _pncp_params_baseline(query, status, cod, page=page)
            try:
                data = _fetch_pncp_page(params)
            except Exception as e:
                st.warning(f"Falha ao consultar município {cod} na página {page}: {e}")
                break

            # Detecta onde estão os itens
            items = []
            if isinstance(data, dict):
                if "content" in data and isinstance(data["content"], list):
                    items = data["content"]
                elif "items" in data and isinstance(data["items"], list):
                    items = data["items"]
                elif "resultados" in data and isinstance(data["resultados"], list):
                    items = data["resultados"]
                # Se vier direto como lista (?)
                elif "results" in data and isinstance(data["results"], list):
                    items = data["results"]

            if not items:
                break

            # Normaliza cada item para colunas usuais
            for it in items:
                registros.append({
                    "municipio_codigo": cod,
                    "titulo": it.get("titulo") or it.get("title") or it.get("objeto") or "",
                    "orgao": it.get("orgao") or it.get("unidadeGestora") or it.get("entidade") or "",
                    "status": it.get("status") or it.get("situacao") or "",
                    "data_publicacao": it.get("dataPublicacao") or it.get("data") or it.get("createdAt") or "",
                    "numero_processo": it.get("numeroProcesso") or it.get("processo") or "",
                    "link": _build_pncp_link(it),
                    "raw": json.dumps(it, ensure_ascii=False)  # opcional para debug
                })

            # Heurística de paginação: se vier menos que o LIMITE, encerra
            if len(items) < TAM_PAGINA_FIXO:
                break
            page += 1

    progress.empty()
    if not registros:
        return pd.DataFrame(columns=["municipio_codigo", "titulo", "orgao", "status",
                                     "data_publicacao", "numero_processo", "link"])
    df = pd.DataFrame(registros)
    # Ordena por data se possível
    if "data_publicacao" in df.columns and df["data_publicacao"].notna().any():
        try:
            df["_dt"] = pd.to_datetime(df["data_publicacao"], errors="coerce")
            df = df.sort_values(by=["_dt"], ascending=False).drop(columns=["_dt"])
        except Exception:
            pass
    return df


def _download_xlsx_button(df: pd.DataFrame, filename_prefix: str = "pncp_resultados"):
    """Disponibiliza o XLSX para download."""
    if df.empty:
        return
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Resultados")
    buffer.seek(0)
    st.download_button(
        label="⬇️ Baixar XLSX",
        data=buffer,
        file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )


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
            "status": "Qualquer",
            "uf": "Todos",
            "add_mun": None,
            "save_name": "",
            "selected_saved": None,
        }


def _add_municipio(codigo: str, nome: str):
    if not codigo:
        return
    sel = st.session_state.selected_municipios
    if codigo in [m["codigo_pncp"] for m in sel]:
        return  # já existe
    if len(sel) >= 25:
        st.warning("Limite de 25 municípios por pesquisa atingido.")
        return
    sel.append({"codigo_pncp": codigo, "nome": nome})


def _remove_municipio(codigo: str):
    st.session_state.selected_municipios = [m for m in st.session_state.selected_municipios if m["codigo_pncp"] != codigo]


def _sidebar(municipios_df: pd.DataFrame):
    st.sidebar.header("🔎 Filtros")

    # 1) Palavra-chave
    st.session_state.sidebar_inputs["palavra_chave"] = st.sidebar.text_input("Palavra-chave", value=st.session_state.sidebar_inputs["palavra_chave"])

    # 2) Status
    st.session_state.sidebar_inputs["status"] = st.sidebar.selectbox("Status", STATUS_OPCOES, index=STATUS_OPCOES.index(st.session_state.sidebar_inputs["status"]))

    # 3) Estado (UF)
    ufs = sorted([u for u in municipios_df["uf"].dropna().unique().tolist() if u])
    ufs = ["Todos"] + ufs
    if st.session_state.sidebar_inputs["uf"] not in ufs:
        st.session_state.sidebar_inputs["uf"] = "Todos"
    st.session_state.sidebar_inputs["uf"] = st.sidebar.selectbox("Estado (UF)", ufs, index=ufs.index(st.session_state.sidebar_inputs["uf"]))

    # Filtra municípios por UF, se aplicável
    if st.session_state.sidebar_inputs["uf"] == "Todos":
        df_sel = municipios_df.copy()
    else:
        df_sel = municipios_df[municipios_df["uf"] == st.session_state.sidebar_inputs["uf"]].copy()

    # 4) Municípios — seleção incremental com limite 25 e remoção
    st.sidebar.markdown("**Municípios (máx. 25)**")
    # Selectbox para adicionar
    options = (
        df_sel
        .assign(label=lambda d: d["nome"] + " / " + d["uf"].fillna(""))
        [["codigo_pncp", "label", "nome"]]
        .values
        .tolist()
    )
    # Mapeia código -> (label, nome)
    opt_map = {row[0]: (row[1], row[2]) for row in options}

    # Lista de códigos já selecionados para ocultar no dropdown
    selected_codes = {m["codigo_pncp"] for m in st.session_state.selected_municipios}
    available_codes = [c for c in opt_map.keys() if c not in selected_codes]

    if available_codes:
        display_labels = [opt_map[c][0] for c in available_codes]
        chosen = st.sidebar.selectbox("Adicionar município", ["—"] + display_labels, index=0)
        if chosen != "—":
            # descobre código selecionado pelo label
            code = next((c for c, (lbl, _) in opt_map.items() if lbl == chosen), None)
            if st.sidebar.button("➕ Adicionar", use_container_width=True):
                _add_municipio(code, opt_map[code][1])
                st.rerun()
    else:
        st.sidebar.info("Todos os municípios desta UF já foram adicionados.")

    # Lista dos selecionados com botões de exclusão
    if st.session_state.selected_municipios:
        st.sidebar.caption("Selecionados:")
        for m in st.session_state.selected_municipios:
            cols = st.sidebar.columns([0.8, 0.2])
            cols[0].write(f"- {m['nome']} ({m['codigo_pncp']})")
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
                    st.session_state.sidebar_inputs["status"] = payload.get("status", "Qualquer") if payload.get("status", "Qualquer") in STATUS_OPCOES else "Qualquer"
                    st.session_state.sidebar_inputs["uf"] = payload.get("uf", "Todos")
                    st.session_state.selected_municipios = payload.get("municipios", [])
                    st.session_state.sidebar_inputs["save_name"] = sel
                    st.sidebar.success(f"Pesquisa '{sel}' carregada.")
                    st.rerun()
    else:
        st.sidebar.caption("Nenhuma pesquisa salva até o momento.")

    # Botão final: Pesquisar
    pesquisar = st.sidebar.button("🔍 Pesquisar", use_container_width=True)
    return pesquisar


# ==============================
# UI principal
# ==============================
def main():
    st.title("📑 Acerte Licitações — O seu Buscador de Editais")
    st.caption("Versão com **sidebar de filtros** e seleção de municípios (máx. 25 por pesquisa).")

    _ensure_session_state()

    # Carrega municípios
    try:
        municipios_df = load_municipios()
    except Exception as e:
        st.error(f"Erro ao carregar 'ListaMunicipiosPNCP.csv': {e}")
        st.stop()

    # Sidebar
    disparar_busca = _sidebar(municipios_df)

    # Header de contexto/estado atual
    with st.expander("Configuração atual", expanded=False):
        st.write({
            "palavra_chave": st.session_state.sidebar_inputs["palavra_chave"],
            "status": st.session_state.sidebar_inputs["status"],
            "uf": st.session_state.sidebar_inputs["uf"],
            "municipios_selecionados": st.session_state.selected_municipios,
        })

    # Execução da busca quando o usuário clicar
    if disparar_busca:
        if not st.session_state.selected_municipios:
            st.warning("Selecione pelo menos um município para pesquisar.")
            st.stop()

        codigos = [m["codigo_pncp"] for m in st.session_state.selected_municipios]
        query = st.session_state.sidebar_inputs["palavra_chave"]
        status = st.session_state.sidebar_inputs["status"]

        with st.spinner("Consultando PNCP..."):
            df = _collect_results(query=query, status=status, codigos_municipio=codigos)

        st.subheader("Resultados")
        if df.empty:
            st.info("Nenhum resultado encontrado para os critérios informados.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            _download_xlsx_button(df)

    st.markdown("---")
    st.caption("Baseline histórico preservado: paginação fixa TAM_PAGINA_FIXO=100 e normalização de links via `_build_pncp_link`.")

if __name__ == "__main__":
    main()
