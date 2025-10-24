# app.py — 📑 Acerte Licitações — Consulta por UF + filtro client-side por municípios (lista IBGE)
# Execução: streamlit run app.py
# Requisitos: streamlit, requests, pandas, xlsxwriter (ou openpyxl)

from __future__ import annotations
import io
import time
import json
from datetime import date, timedelta
from typing import Dict, List, Optional, Iterable, Set

import pandas as pd
import requests
import streamlit as st

# =========================
# Config & Constantes
# =========================
st.set_page_config(page_title="📑 Acerte Licitações", page_icon="📑", layout="wide")

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"       # Recebendo Proposta (dataFinal=yyyyMMdd; CONSULTA POR UF)
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # Publicações (codigoModalidadeContratacao + datas=yyyyMMdd; CONSULTA POR UF)
PAGE_SIZE = 50

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]
PUBLICACAO_JANELA_DIAS = 60

# IBGE — apenas para montar a lista de municípios na sidebar
IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"


# =========================
# Helpers
# =========================
def _normalize_text(s: Optional[str]) -> str:
    return (s or "").strip()

def _xlsx_bytes(df: pd.DataFrame, sheet_name: str = "resultados") -> bytes:
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        return buffer.getvalue()

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def _classificar_status(nome: Optional[str]) -> str:
    s = _normalize_text(nome).lower()
    if "receb" in s:                          # a receber / recebendo propostas
        return "Recebendo Proposta"
    if "julg" in s or "propostas encerradas" in s:
        return "Propostas Encerradas"
    if "encerrad" in s:
        return "Encerradas"
    return "Todos"

def _as_str(x) -> str:
    return "" if x is None else str(x)

def _mun_key(s: Optional[str]) -> str:
    """Normaliza nome de município para comparação (casefold + strip)."""
    return _normalize_text(s).casefold()


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
            "User-Agent": "Acerte-Licitacoes/1.0 (+streamlit; PNCP client)",
            "Connection": "keep-alive",
        })
        SESSION = s
    return SESSION

def _safe_json(r: requests.Response) -> dict:
    """
    Decodifica JSON com tolerância a respostas vazias do PNCP.
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
# IBGE — municípios por UF (apenas para UI)
# =========================
@st.cache_data(show_spinner=False, ttl=60*60*24)
def carregar_ibge_df() -> pd.DataFrame:
    try:
        s = _get_session()
        r = s.get(IBGE_URL, timeout=60)
        r.raise_for_status()
        data = r.json()
        rows = []
        if isinstance(data, list):
            for m in data:
                try:
                    nome = m.get("nome")
                    mic = m.get("microrregiao") or {}
                    meso = mic.get("mesorregiao") or {}
                    ufobj = meso.get("UF") or {}
                    uf = ufobj.get("sigla")
                    codigo = m.get("id")
                    if nome and uf and codigo:
                        rows.append({"municipio": str(nome), "uf": str(uf), "codigo_ibge": str(codigo)})
                except Exception:
                    continue
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["municipio", "uf", "codigo_ibge"])
        return df.sort_values(["uf", "municipio"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["municipio", "uf", "codigo_ibge"])

@st.cache_data(show_spinner=False, ttl=60*60*24)
def ibge_por_uf(uf: str) -> pd.DataFrame:
    df = carregar_ibge_df()
    if df.empty:
        return df
    return df[df["uf"] == uf].copy()


# =========================
# PNCP — paginação (retry + parser seguro)
# =========================
def _iterar_paginas(endpoint: str, params_base: Dict[str, str], sleep_s: float = 0.03) -> Iterable[tuple[int, Optional[int], list]]:
    """
    Paginação com até 3 tentativas por página.
    Trata resposta vazia como "sem dados".
    """
    sess = _get_session()
    pagina = 1
    while True:
        params = {k: _as_str(v) for k, v in params_base.items()}
        params.update({"pagina": _as_str(pagina), "tamanhoPagina": _as_str(PAGE_SIZE)})

        last_err = None
        for attempt in range(1, 4):  # 3 tentativas
            try:
                r = sess.get(endpoint, params=params, timeout=60)
                r.raise_for_status()
                payload = _safe_json(r)
                dados = payload.get("data") or []
                if not dados:
                    return
                yield pagina, payload.get("totalPaginas"), dados

                numero = payload.get("numeroPagina") or pagina
                total_pag = payload.get("totalPaginas")
                if total_pag and numero >= total_pag:
                    return
                pagina += 1
                time.sleep(sleep_s)
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(0.3 * attempt)  # backoff progressivo
        if last_err is not None:
            raise last_err


# =========================
# Consultas — SEM usar codigoMunicipioIbge na API (apenas UF)
# =========================
def _filtrar_client_side_por_municipios(dados: list, nomes_municipios: Set[str]) -> list:
    """Filtra pela lista selecionada de nomes de municípios (case-insensitive)."""
    if not nomes_municipios:
        return dados
    keys = set(_mun_key(n) for n in nomes_municipios)
    out = []
    for d in dados:
        uo = d.get("unidadeOrgao") or {}
        if _mun_key(uo.get("municipioNome")) in keys:
            out.append(d)
    return out

def _filtrar_client_side_status_e_palavra(dados: list, palavra_chave: str, status_label: str) -> list:
    out = []
    for d in dados:
        # status bucket
        if status_label != "Todos" and _classificar_status(d.get("situacaoCompraNome")) != status_label:
            continue
        # palavra-chave
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


def consultar_proposta_por_uf(uf: str) -> list:
    """
    /proposta: consulta EXCLUSIVAMENTE por UF (dataFinal=hoje yyyyMMdd).
    Não usa codigoMunicipioIbge. Retorna lista bruta para posterior filtragem client-side.
    """
    params_base = {"uf": uf, "dataFinal": _yyyymmdd(date.today())}
    acumulado = []
    total_baixado = 0
    barra = st.progress(0.0)
    pag_atual, total_pag = 0, None

    for pag, total_pag, dados in _iterar_paginas(ENDP_PROPOSTA, params_base):
        pag_atual = pag
        total_baixado += len(dados)
        acumulado.extend(dados)
        barra.progress(min(1.0, pag_atual / float(total_pag or max(1, pag_atual*10))))

    barra.progress(1.0)
    st.session_state["_telemetria_proposta_total_uf"] = total_baixado
    return acumulado


def consultar_publicacao_por_uf_modalidades(uf: str, codigos_modalidade: List[str],
                                            dias_janela: int = PUBLICACAO_JANELA_DIAS) -> list:
    """
    /publicacao: consulta EXCLUSIVAMENTE por UF (e por modalidade), sem codigoMunicipioIbge.
    """
    if not codigos_modalidade:
        return []
    hoje = date.today()
    params_comuns = {
        "uf": uf,
        "dataInicial": _yyyymmdd(hoje - timedelta(days=dias_janela)),
        "dataFinal": _yyyymmdd(hoje),
    }
    acumulado = []
    barra = st.progress(0.0)
    total_passos = len(codigos_modalidade)
    total_baixado = 0
    for idx, cod in enumerate(codigos_modalidade, start=1):
        params = dict(params_comuns)
        params["codigoModalidadeContratacao"] = _as_str(cod)
        for _, _, dados in _iterar_paginas(ENDP_PUBLICACAO, params):
            total_baixado += len(dados)
            acumulado.extend(dados)
        barra.progress(min(1.0, idx / float(total_passos)))
        time.sleep(0.03)
    barra.progress(1.0)
    st.session_state["_telemetria_publicacao_total_uf"] = total_baixado
    return acumulado


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
# Sidebar — Filtros
# =========================
st.sidebar.header("Filtros")

# Palavra chave
palavra_chave = st.sidebar.text_input("Palavra chave", value="")

# Estado (obrigatório)
uf_escolhida = st.sidebar.selectbox("Estado", options=UFS, index=UFS.index("SP"))
if not uf_escolhida:
    st.sidebar.error("Selecione um Estado (UF).")

# Municípios (lista IBGE por UF) — APENAS PARA SELEÇÃO; NÃO vai para a API
df_ibge_uf = ibge_por_uf(uf_escolhida)
if df_ibge_uf.empty:
    st.sidebar.warning("Falha ao carregar municípios do IBGE. Você pode digitar manualmente os nomes (separados por vírgula).")
    mun_manual = st.sidebar.text_input("Municípios (manual)", value="")
    municipios_selecionados = [m.strip() for m in mun_manual.split(",") if m.strip()]
else:
    # Opção exibida como "Município / UF" para clareza; usaremos APENAS o nome na filtragem local
    opcoes_municipios = [f"{row.municipio} / {row.uf}" for _, row in df_ibge_uf.iterrows()]
    labels_escolhidos = st.sidebar.multiselect("Municipios", options=opcoes_municipios)
    municipios_selecionados = [lab.split(" / ")[0] for lab in labels_escolhidos]

# Status
status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

# Modalidades (obrigatórias apenas quando usar /publicacao)
modalidades_str = ""
if status_label != "Recebendo Proposta":
    modalidades_str = st.sidebar.text_input(
        "Modalidades (códigos PNCP, separados por vírgula)",
        value="",
        placeholder="Ex.: 5, 6, 23 (Pregão, Concorrência etc.)"
    )

st.sidebar.markdown("---")

# Salvar pesquisa
if "pesquisas_salvas" not in st.session_state:
    st.session_state["pesquisas_salvas"] = {}
nome_pesquisa = st.sidebar.text_input("Salvar pesquisa", value="", placeholder="Ex.: SP — Encerradas — Saúde")

if st.sidebar.button("Salvar pesquisa"):
    st.session_state["pesquisas_salvas"][nome_pesquisa.strip() or f"Pesquisa {len(st.session_state['pesquisas_salvas'])+1}"] = {
        "palavra_chave": palavra_chave,
        "uf": uf_escolhida,
        "municipios": municipios_selecionados,
        "status": status_label,
        "modalidades": modalidades_str,
    }
    st.sidebar.success("Pesquisa salva.")

# Pesquisas salvas
salvos = st.session_state.get("pesquisas_salvas", {})
escolha_salva = st.sidebar.selectbox("Pesquisas salvas", options=["—"] + list(salvos.keys()), index=0)
if escolha_salva != "—":
    p = salvos[escolha_salva]
    st.sidebar.info(
        f"**{escolha_salva}**\n\n"
        f"- Palavra chave: `{p.get('palavra_chave','')}`\n"
        f"- UF: `{p.get('uf')}`\n"
        f"- Municípios: `{', '.join(p.get('municipios', [])) or '—'}`\n"
        f"- Status: `{p.get('status')}`\n"
        f"- Modalidades: `{p.get('modalidades') or '—'}`"
    )

st.sidebar.markdown("---")
executar = st.sidebar.button("Executar Pesquisa")


# =========================
# Corpo — Resultados
# =========================
st.title("📑 Acerte Licitações — O seu Buscador de Editais")

if executar:
    if not uf_escolhida:
        st.error("Operação cancelada: o campo **Estado** é obrigatório.")
        st.stop()

    try:
        regs_bruto = []
        total_baixado_uf = 0

        if status_label == "Recebendo Proposta":
            # 1) Coleta por UF (sem municipio na API)
            regs_bruto = consultar_proposta_por_uf(uf_escolhida)
            total_baixado_uf = st.session_state.get("_telemetria_proposta_total_uf", 0)

        else:
            # /publicacao — exige modalidades
            cod_modalidades = [x.strip() for x in modalidades_str.split(",") if x.strip()]
            if not cod_modalidades:
                st.warning(
                    "Para **Propostas Encerradas / Encerradas / Todos**, informe **códigos de modalidade** "
                    "(campo na sidebar). O endpoint /publicacao exige esse parâmetro."
                )
                regs_bruto = []
            else:
                regs_bruto = consultar_publicacao_por_uf_modalidades(
                    uf=uf_escolhida,
                    codigos_modalidade=cod_modalidades,
                    dias_janela=PUBLICACAO_JANELA_DIAS,
                )
                total_baixado_uf = st.session_state.get("_telemetria_publicacao_total_uf", 0)

        # 2) Filtro client-side: primeiro status/palavra, depois municípios selecionados
        regs_status_palavra = _filtrar_client_side_status_e_palavra(regs_bruto, palavra_chave, status_label)
        regs_filtrados = _filtrar_client_side_por_municipios(regs_status_palavra, set(municipios_selecionados))

        df = normalizar_df(regs_filtrados)

        # Auditoria: municípios selecionados sem retorno
        sel_norm = set(_mun_key(n) for n in municipios_selecionados or [])
        presentes = set(_mun_key((r.get("unidadeOrgao") or {}).get("municipioNome")) for r in regs_filtrados)
        sem_resultado = [m for m in (municipios_selecionados or []) if _mun_key(m) not in presentes]

        st.subheader("Resultados")
        hoje_txt = _yyyymmdd(date.today())
        st.caption(
            f"UF **{uf_escolhida}** • Municípios selecionados **{len(municipios_selecionados)}** • "
            f"Status **{status_label}** • Palavra-chave **{palavra_chave or '—'}** • Execução **{hoje_txt}**"
        )

        st.info(f"Coleta por UF: {total_baixado_uf} item(ns) recebidos do PNCP; após filtros: {len(df)}.")

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
                file_name=f"editais_{uf_escolhida}_{status_label}_{hoje_txt}.xlsx",
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
        "Fluxo: 1) buscar por **UF** no PNCP; 2) **filtrar client-side** pelos municípios selecionados (lista IBGE).\n\n"
        "- **Estado (UF)** é obrigatório.\n"
        "- **Municípios** servem apenas para filtrar a exibição — não são enviados para a API.\n"
        "- **'Recebendo Proposta'** usa `/proposta` com `dataFinal=yyyyMMdd`.\n"
        "- **Demais status** usam `/publicacao` e exigem **códigos de modalidade** (campo na sidebar).\n"
        "- Respostas vazias (204/200 sem corpo) são tratadas como **0 resultados**."
    )
