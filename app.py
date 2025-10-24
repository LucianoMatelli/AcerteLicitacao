# app.py — 📑 PNCP: consulta por MUNICÍPIO (pelo NOME) diretamente na API
# Execução: streamlit run app.py
# Requisitos: streamlit, requests, pandas, xlsxwriter (ou openpyxl)

from __future__ import annotations
import io
import time
import json
from datetime import date, timedelta
from typing import Dict, List, Optional, Iterable, Tuple

import pandas as pd
import requests
import streamlit as st

# =========================
# Config & Constantes
# =========================
st.set_page_config(page_title="📑 PNCP por Município (nome)", page_icon="📑", layout="wide")

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"       # Recebendo Proposta (dataFinal=yyyyMMdd)
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # Publicações (codigoModalidadeContratacao + datas=yyyyMMdd)

PAGE_SIZE = 50
ALT_PAGE_SIZE = 20
MAX_BLANK_PAGES = 2
RETRY_PER_PAGE = 3
SLEEP_BETWEEN_PAGES = 0.03

STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]
PUBLICACAO_JANELA_DIAS = 60

# Variações de nome de parâmetro para município por NOME
MUNICIPIO_PARAM_VARIANTS = ["municipio", "municipioNome", "nomeMunicipio"]

# Amostragem opcional (caso precise aumentar abrangência temporal do /proposta)
PROPOSTA_DIAS_RETROATIVOS_DEFAULT = 0   # 0 = só hoje
PROPOSTA_PASSO_DIAS_DEFAULT = 1         # irrelevante se dias_retro=0


# =========================
# Helpers
# =========================
def _normalize_text(s: Optional[str]) -> str:
    return (s or "").strip()

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def _as_str(x) -> str:
    return "" if x is None else str(x)

def _classificar_status(nome: Optional[str]) -> str:
    s = _normalize_text(nome).lower()
    if "receb" in s:                          # a receber / recebendo propostas
        return "Recebendo Proposta"
    if "julg" in s or "propostas encerradas" in s:
        return "Propostas Encerradas"
    if "encerrad" in s:
        return "Encerradas"
    return "Todos"

def _xlsx_bytes(df: pd.DataFrame, sheet_name: str = "resultados") -> bytes:
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        return buffer.getvalue()


# =========================
# HTTP session + parser seguro
# =========================
SESSION: Optional[requests.Session] = None

def _get_session() -> requests.Session:
    global SESSION
    if SESSION is None:
        s = requests.Session()
        s.headers.update({
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "PNCP-Municipio-Nome/1.0 (Streamlit)",
            "Connection": "keep-alive",
        })
        SESSION = s
    return SESSION

def _safe_json(r: requests.Response) -> dict:
    """
    JSON tolerante a respostas vazias do PNCP.
    - 204 No Content                      -> {'data': [], 'totalPaginas': 1, 'numeroPagina': 1}
    - 200 com corpo vazio/whitespace      -> {'data': [], 'totalPaginas': 1, 'numeroPagina': 1}
    - Content-Type ausente mas corpo JSON -> tenta json()
    - Content-Type não-JSON e corpo não-vazio -> erro descritivo
    """
    if r.status_code == 204:
        return {"data": [], "totalPaginas": 1, "numeroPagina": 1}
    text = r.text or ""
    if text.strip() == "":
        return {"data": [], "totalPaginas": 1, "numeroPagina": 1}
    ctype = (r.headers.get("Content-Type") or "").lower()
    looks_json = ("json" in ctype) or text.strip().startswith(("{", "["))
    if not looks_json:
        snippet = text[:800].strip()
        raise ValueError(
            f"Resposta não-JSON do servidor PNCP (Content-Type='{ctype}'). "
            f"Trecho: {snippet or '<vazio>'}"
        )
    try:
        return r.json()
    except json.JSONDecodeError as e:
        snippet = text[:800].strip()
        raise ValueError(f"Falha ao decodificar JSON. Trecho recebido: {snippet}") from e


# =========================
# Paginação com fallback e scroll contínuo
# =========================
def _paginacao(endpoint: str, params_base: Dict[str, str], page_size: int = PAGE_SIZE) -> Iterable[list]:
    """
    Itera páginas até ocorrerem MAX_BLANK_PAGES consecutivas vazias.
    Ignora 'totalPaginas' do payload (pode ser truncado no backend).
    RETRY_PER_PAGE tentativas por página, com backoff incremental.
    """
    sess = _get_session()
    pagina = 1
    blank_streak = 0
    while True:
        params = {k: _as_str(v) for k, v in params_base.items()}
        params.update({"pagina": _as_str(pagina), "tamanhoPagina": _as_str(page_size)})

        last_err = None
        for attempt in range(1, RETRY_PER_PAGE + 1):
            try:
                r = sess.get(endpoint, params=params, timeout=60)
                r.raise_for_status()
                payload = _safe_json(r)
                dados = payload.get("data") or []
                if not dados:
                    blank_streak += 1
                    if blank_streak >= MAX_BLANK_PAGES:
                        return
                    else:
                        break
                else:
                    blank_streak = 0
                    yield dados
                    break
            except Exception as e:
                last_err = e
                time.sleep(0.3 * attempt)
        if last_err is not None:
            # fallback page_size 50 -> 20
            if page_size != ALT_PAGE_SIZE:
                for lote in _paginacao(endpoint, params_base, page_size=ALT_PAGE_SIZE):
                    yield lote
                return
            else:
                raise last_err

        pagina += 1
        time.sleep(SLEEP_BETWEEN_PAGES)


# =========================
# Descoberta automática do nome do parâmetro de município
# =========================
def _descobrir_parametro_municipio(endpoint: str, base_params: Dict[str, str], municipio_nome: str) -> Optional[str]:
    """
    Testa as variações conhecidas de nome de parâmetro para município por NOME.
    Retorna a primeira chave funcional (200 com JSON, ainda que vazio).
    """
    sess = _get_session()
    for chave in MUNICIPIO_PARAM_VARIANTS:
        params = dict(base_params)
        params[chave] = municipio_nome
        try:
            r = sess.get(endpoint, params=params, timeout=30)
            if r.status_code in (200, 204):
                # Considera válida se não retornou 4xx/5xx
                _ = _safe_json(r)  # valida JSON ou vazio tolerado
                return chave
        except Exception:
            pass
    return None


# =========================
# Consultas — por município (NOME)
# =========================
def consultar_proposta_por_municipio_nome(municipio_nome: str, dias_retro: int = 0, passo_dias: int = 1) -> List[dict]:
    """
    /proposta com município pelo NOME: tenta automaticamente o nome de parâmetro correto.
    Pode fazer amostragem temporal se desejar (dias_retro>0).
    """
    hoje = date.today()
    cortes = [hoje - timedelta(days=delta) for delta in range(0, max(1, dias_retro)+1, max(1, passo_dias))]

    # Descobre a chave de parâmetro com um "ping" usando o primeiro corte
    base_ping = {"dataFinal": _yyyymmdd(cortes[0])}
    chave = _descobrir_parametro_municipio(ENDP_PROPOSTA, base_ping, municipio_nome)
    if not chave:
        st.warning(f"Não foi possível identificar o parâmetro de município por nome para '{municipio_nome}'.")
        return []

    registros = []
    barra = st.progress(0.0)
    for idx, corte in enumerate(cortes, start=1):
        params = {"dataFinal": _yyyymmdd(corte), chave: municipio_nome}
        for dados in _paginacao(ENDP_PROPOSTA, params, page_size=PAGE_SIZE):
            registros.extend(dados)
        barra.progress(min(1.0, idx / float(len(cortes))))
        time.sleep(0.02)
    return registros


def consultar_publicacao_por_municipio_nome(municipio_nome: str, codigos_modalidade: List[str],
                                            dias_janela: int = PUBLICACAO_JANELA_DIAS) -> List[dict]:
    """
    /publicacao com município pelo NOME (exige códigos de modalidade).
    Descobre automaticamente a chave de parâmetro que funciona.
    """
    if not codigos_modalidade:
        return []
    hoje = date.today()
    base_ping = {
        "dataInicial": _yyyymmdd(hoje - timedelta(days=dias_janela)),
        "dataFinal": _yyyymmdd(hoje),
        # sem modalidade no ping para testar a chave; alguns ambientes exigem,
        # então se falhar no ping, vamos tentar com a 1ª modalidade
    }
    chave = _descobrir_parametro_municipio(ENDP_PUBLICACAO, base_ping, municipio_nome)
    if not chave:
        # tenta ping com modalidade (há ambientes que 422 sem a modalidade)
        base_ping2 = dict(base_ping)
        base_ping2["codigoModalidadeContratacao"] = _as_str(codigos_modalidade[0])
        chave = _descobrir_parametro_municipio(ENDP_PUBLICACAO, base_ping2, municipio_nome)
        if not chave:
            st.warning(f"Não foi possível identificar o parâmetro de município por nome para '{municipio_nome}' em /publicacao.")
            return []

    acumulado = []
    barra = st.progress(0.0)
    total_passos = len(codigos_modalidade)
    for idx, cod in enumerate(codigos_modalidade, start=1):
        params = {
            "dataInicial": _yyyymmdd(hoje - timedelta(days=dias_janela)),
            "dataFinal": _yyyymmdd(hoje),
            "codigoModalidadeContratacao": _as_str(cod),
            chave: municipio_nome,
        }
        for dados in _paginacao(ENDP_PUBLICACAO, params, page_size=PAGE_SIZE):
            acumulado.extend(dados)
        barra.progress(min(1.0, idx / float(total_passos)))
        time.sleep(0.02)
    return acumulado


# =========================
# Filtros locais (palavra-chave e status)
# =========================
def filtrar_por_status_palavra(dados: list, palavra_chave: str, status_label: str) -> list:
    out = []
    for d in dados:
        if status_label != "Todos" and _classificar_status(d.get("situacaoCompraNome")) != status_label:
            continue
        if palavra_chave:
            p = palavra_chave.strip().lower()
            uo = d.get("unidadeOrgao") or {}
            texto = " ".join([
                _normalize_text(d.get("objetoCompra")),
                _normalize_text(d.get("informacaoComplementar")),
                _normalize_text(uo.get("nomeUnidade")),
            ]).lower()
            if p not in texto:
                continue
        out.append(d)
    return out


def normalizar_df(regs: List[dict]) -> pd.DataFrame:
    linhas = []
    for d in regs:
        uo = d.get("unidadeOrgao") or {}
        linhas.append({
            "Status (bucket)": _classificar_status(d.get("situacaoCompraNome")),
            "Situação (PNCP)": d.get("situacaoCompraNome"),
            "UF": uo.get("ufSigla"),
            "Município": uo.get("municipioNome"),
            "Órgão/Unidade": uo.get("nomeUnidade"),
            "Modalidade": d.get("modalidadeNome"),
            "Modo de Disputa": d.get("modoDisputaNome"),
            "Nº Compra": d.get("numeroCompra"),
            "Objeto": d.get("objetoCompra"),
            "Informação Complementar": d.get("informacaoComplementar"),
            "Publicação PNCP": d.get("dataPublicacaoPncp"),
            "Abertura Proposta": d.get("dataAberturaProposta"),
            "Encerramento Proposta": d.get("dataEncerramentoProposta"),
            "Link Origem": d.get("linkSistemaOrigem"),
            "Controle PNCP": d.get("numeroControlePNCP"),
        })
    df = pd.DataFrame(linhas)
    if not df.empty and "Publicação PNCP" in df.columns:
        df = df.sort_values(by=["Publicação PNCP", "Município"], ascending=[False, True])
    return df


# =========================
# Sidebar — Filtros (sem UF e sem códigos)
# =========================
st.sidebar.header("Filtros")

palavra_chave = st.sidebar.text_input("Palavra chave", value="")

# MUNICÍPIOS POR NOME — entrada livre (um ou vários)
mun_input = st.sidebar.text_area(
    "Municípios (por nome, separados por vírgula ou quebra de linha)",
    value="",
    height=90,
    placeholder="Ex.: Porto Feliz\nItapetininga\nSorocaba"
)
municipios_selecionados = [m.strip() for chunk in mun_input.split("\n") for m in chunk.split(",")]
municipios_selecionados = [m for m in municipios_selecionados if m]

status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

# /publicacao necessita modalidades
modalidades_str = ""
if status_label != "Recebendo Proposta":
    modalidades_str = st.sidebar.text_input(
        "Modalidades (códigos PNCP, separados por vírgula)",
        value="",
        placeholder="Ex.: 5, 6, 23 (Pregão, Concorrência etc.)"
    )

with st.sidebar.expander("Amostragem por datas em /proposta (opcional)", expanded=False):
    dias_retro = st.number_input("Dias retroativos (proposta)", min_value=0, max_value=120,
                                 value=PROPOSTA_DIAS_RETROATIVOS_DEFAULT, step=7)
    passo_dias = st.number_input("Passo (dias) entre coletas", min_value=1, max_value=30,
                                 value=PROPOSTA_PASSO_DIAS_DEFAULT, step=1)
    st.caption("Ex.: 0 = apenas hoje. 28/7 = hoje, -7, -14, -21, -28.")

st.sidebar.markdown("---")

if "pesquisas_salvas" not in st.session_state:
    st.session_state["pesquisas_salvas"] = {}
nome_pesquisa = st.sidebar.text_input("Salvar pesquisa", value="", placeholder="Ex.: Recebendo — Porto Feliz")
if st.sidebar.button("Salvar pesquisa"):
    st.session_state["pesquisas_salvas"][nome_pesquisa.strip() or f"Pesquisa {len(st.session_state['pesquisas_salvas'])+1}"] = {
        "palavra_chave": palavra_chave,
        "municipios": municipios_selecionados,
        "status": status_label,
        "modalidades": modalidades_str,
        "dias_retro": int(dias_retro),
        "passo_dias": int(passo_dias),
    }
    st.sidebar.success("Pesquisa salva.")

salvos = st.session_state.get("pesquisas_salvas", {})
escolha_salva = st.sidebar.selectbox("Pesquisas salvas", options=["—"] + list(salvos.keys()), index=0)
if escolha_salva != "—":
    p = salvos[escolha_salva]
    st.sidebar.info(
        f"**{escolha_salva}**\n\n"
        f"- Palavra chave: `{p.get('palavra_chave','')}`\n"
        f"- Municípios: `{', '.join(p.get('municipios', [])) or '—'}`\n"
        f"- Status: `{p.get('status')}`\n"
        f"- Modalidades: `{p.get('modalidades') or '—'}`\n"
        f"- Dias retro: `{p.get('dias_retro', 0)}` • Passo: `{p.get('passo_dias', 1)}`"
    )

st.sidebar.markdown("---")
executar = st.sidebar.button("Executar Pesquisa")


# =========================
# Corpo — Resultados
# =========================
st.title("📑 PNCP — Consulta por NOME do Município (sem UF e sem códigos)")

if executar:
    if not municipios_selecionados:
        st.error("Informe ao menos **um município** pelo nome.")
        st.stop()

    try:
        regs_bruto = []
        total_por_municipio = {}

        # Coleta por município (nome) diretamente na API
        barra_mun = st.progress(0.0)
        for i, mun in enumerate(municipios_selecionados, start=1):
            if status_label == "Recebendo Proposta":
                lote = consultar_proposta_por_municipio_nome(
                    municipio_nome=mun,
                    dias_retro=int(dias_retro),
                    passo_dias=int(passo_dias),
                )
            else:
                cod_modalidades = [x.strip() for x in modalidades_str.split(",") if x.strip()]
                if not cod_modalidades:
                    st.warning(
                        f"Para **{status_label}** no município '{mun}', informe **códigos de modalidade** "
                        "(campo na sidebar). O endpoint /publicacao exige esse parâmetro."
                    )
                    lote = []
                else:
                    lote = consultar_publicacao_por_municipio_nome(
                        municipio_nome=mun,
                        codigos_modalidade=cod_modalidades,
                        dias_janela=PUBLICACAO_JANELA_DIAS,
                    )
            regs_bruto.extend(lote)
            total_por_municipio[mun] = len(lote)
            barra_mun.progress(min(1.0, i / float(len(municipios_selecionados))))
            time.sleep(0.02)
        barra_mun.progress(1.0)

        # Filtro local: palavra-chave + status (status já tende a vir correto; mantemos por segurança)
        regs_filtrados = filtrar_por_status_palavra(regs_bruto, palavra_chave, status_label)

        df = normalizar_df(regs_filtrados)

        # Auditoria por município
        sem_resultado = [m for m in municipios_selecionados if total_por_municipio.get(m, 0) == 0]

        st.subheader("Resultados")
        hoje_txt = _yyyymmdd(date.today())
        st.caption(
            f"Municípios buscados **{len(municipios_selecionados)}** • "
            f"Status **{status_label}** • Palavra-chave **{palavra_chave or '—'}** • Execução **{hoje_txt}**"
        )

        detalhes = "; ".join([f"{m}: {q}" for m, q in total_por_municipio.items()])
        st.info(f"Itens recebidos por município (bruto): {detalhes or '—'} • Após filtros: **{len(df)}**.")

        if sem_resultado:
            st.warning(f"Sem resultados para: {', '.join(sem_resultado)}")

        if df.empty:
            st.warning("Nenhum resultado para os filtros aplicados.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            xlsx = _xlsx_bytes(df, sheet_name="editais")
            st.download_button(
                label="⬇️ Baixar XLSX",
                data=xlsx,
                file_name=f"editais_{status_label}_{hoje_txt}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except requests.HTTPError as e:
        st.error(f"Erro na API PNCP: {e}")
    except ValueError as e:
        st.error(f"Erro de parsing: {e}")
    except Exception as e:
        st.error(f"Falha inesperada: {e}")

else:
    st.info(
        "Fluxo: buscar **direto por NOME do município** na API do PNCP (sem UF e sem códigos), "
        "tentando automaticamente a variação de parâmetro que o backend aceitar. "
        "Opcionalmente, use \"Dias retroativos\" para aumentar a abrangência temporal do /proposta."
    )
