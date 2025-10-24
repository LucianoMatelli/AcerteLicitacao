# app.py — 📑 Acerte Licitações — Buscador de Editais (UF obrigatório, municípios IBGE + turbo seletivo)
# Execução: streamlit run app.py
# Requisitos: streamlit, requests, pandas, xlsxwriter (ou openpyxl)

from __future__ import annotations
import io
import time
import json
from datetime import date, timedelta
from typing import Dict, List, Optional, Iterable

import pandas as pd
import requests
import streamlit as st

# =========================
# Config & Constantes
# =========================
st.set_page_config(page_title="📑 Acerte Licitações", page_icon="📑", layout="wide")

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"       # Recebendo Proposta (dataFinal=yyyyMMdd; aceita codigoMunicipioIbge)
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # Publicações (codigoModalidadeContratacao + datas=yyyyMMdd; aceita codigoMunicipioIbge)
PAGE_SIZE = 50

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

# Status conforme solicitado
STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]

# Janela default para /publicacao
PUBLICACAO_JANELA_DIAS = 60

# IBGE — fonte oficial
IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"

# Otimizador: até este número de municípios, consulta "turbo" por município
MUNICIPALITY_QUERY_THRESHOLD = 25


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
# IBGE — municípios por UF (com tratamento defensivo + cache)
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
# PNCP — iteração paginada (retry + parser seguro)
# =========================
def _iterar_paginas(endpoint: str, params_base: Dict[str, str], sleep_s: float = 0.03) -> Iterable[tuple[int, Optional[int], list]]:
    """
    Paginação com até 3 tentativas por página.
    Em erros HTTP ou resposta não-JSON, detalha o problema.
    Trata resposta vazia como "sem dados" (encerra sem exceção).
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
                    return  # sem dados: encerra normalmente
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
# Consultas — modos turbo (por município) e amplo (por UF)
# =========================
def _filtrar_client_side(dados: list, palavra_chave: str, ibges: List[str], status_label: str) -> list:
    ibge_set = set(_as_str(x) for x in (ibges or []))
    out = []
    for d in dados:
        uo = d.get("unidadeOrgao") or {}
        if ibge_set and _as_str(uo.get("codigoIbge")) not in ibge_set:
            continue
        if status_label != "Todos" and _classificar_status(d.get("situacaoCompraNome")) != status_label:
            continue
        if palavra_chave:
            p = palavra_chave.strip().lower()
            texto = " ".join([
                _normalize_text(d.get("objetoCompra")),
                _normalize_text(d.get("informacaoComplementar")),
                _normalize_text(uo.get("nomeUnidade")),
            ]).lower()
            if p not in texto:
                continue
        out.append(d)
    return out


def consultar_proposta_por_uf(uf: str, palavra_chave: str, ibges: List[str]) -> tuple[list, int]:
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
    filtrados = _filtrar_client_side(acumulado, palavra_chave, ibges, "Recebendo Proposta")
    return filtrados, total_baixado


def consultar_proposta_por_municipios(uf: str, palavra_chave: str, ibges: List[str]) -> tuple[list, int]:
    if not ibges:
        return [], 0
    acumulado = []
    total_baixado = 0
    barra = st.progress(0.0)
    for i, ibge in enumerate(ibges, start=1):
        params_base = {
            "uf": uf,
            "codigoMunicipioIbge": _as_str(ibge),
            "dataFinal": _yyyymmdd(date.today()),
        }
        for _, _, dados in _iterar_paginas(ENDP_PROPOSTA, params_base):
            total_baixado += len(dados)
            dados = _filtrar_client_side(dados, palavra_chave, [], "Recebendo Proposta")  # já veio por município
            acumulado.extend(dados)
        barra.progress(min(1.0, i / float(len(ibges))))
        time.sleep(0.03)
    barra.progress(1.0)
    return acumulado, total_baixado


def consultar_publicacao_por_uf_modalidades(uf: str, palavra_chave: str, ibges: List[str],
                                            status_label: str, codigos_modalidade: List[str],
                                            dias_janela: int = PUBLICACAO_JANELA_DIAS) -> tuple[list, int]:
    if not codigos_modalidade:
        return [], 0
    hoje = date.today()
    params_comuns = {
        "uf": uf,
        "dataInicial": _yyyymmdd(hoje - timedelta(days=dias_janela)),
        "dataFinal": _yyyymmdd(hoje),
    }
    acumulado, total_baixado = [], 0
    barra = st.progress(0.0)
    total_passos = len(codigos_modalidade)
    for idx, cod in enumerate(codigos_modalidade, start=1):
        params = dict(params_comuns)
        params["codigoModalidadeContratacao"] = _as_str(cod)
        for _, _, dados in _iterar_paginas(ENDP_PUBLICACAO, params):
            total_baixado += len(dados)
            dados = _filtrar_client_side(dados, palavra_chave, ibges, status_label)
            acumulado.extend(dados)
        barra.progress(min(1.0, idx / float(total_passos)))
        time.sleep(0.03)
    barra.progress(1.0)
    return acumulado, total_baixado


def consultar_publicacao_por_municipios_modalidades(uf: str, palavra_chave: str, ibges: List[str],
                                                    status_label: str, codigos_modalidade: List[str],
                                                    dias_janela: int = PUBLICACAO_JANELA_DIAS) -> tuple[list, int]:
    if not (ibges and codigos_modalidade):
        return [], 0
    hoje = date.today()
    params_base_comum = {
        "uf": uf,
        "dataInicial": _yyyymmdd(hoje - timedelta(days=dias_janela)),
        "dataFinal": _yyyymmdd(hoje),
    }
    acumulado, total_baixado = [], 0
    total_passos = len(ibges) * len(codigos_modalidade)
    passo = 0
    barra = st.progress(0.0)
    for ibge in ibges:
        for cod in codigos_modalidade:
            params = dict(params_base_comum)
            params["codigoModalidadeContratacao"] = _as_str(cod)
            params["codigoMunicipioIbge"] = _as_str(ibge)
            for _, _, dados in _iterar_paginas(ENDP_PUBLICACAO, params):
                total_baixado += len(dados)
                dados = _filtrar_client_side(dados, palavra_chave, [], status_label)  # já veio por município
                acumulado.extend(dados)
            passo += 1
            barra.progress(min(1.0, passo / float(max(1, total_passos))))
            time.sleep(0.03)
    barra.progress(1.0)
    return acumulado, total_baixado


def normalizar_df(regs: List[dict]) -> pd.DataFrame:
    linhas = []
    for d in regs:
        uo = d.get("unidadeOrgao") or {}
        linhas.append({
            "Status (bucket)": _classificar_status(d.get("situacaoCompraNome")),
            "Situação (PNCP)": d.get("situacaoCompraNome"),
            "UF": uo.get("ufSigla"),
            "Município": uo.get("municipioNome"),
            "IBGE": _as_str(uo.get("codigoIbge")),
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
# Sidebar — Filtros (jurimetria operacional, sem firula)
# =========================
st.sidebar.header("Filtros")

# Palavra chave
palavra_chave = st.sidebar.text_input("Palavra chave", value="")

# Estado (obrigatório)
uf_escolhida = st.sidebar.selectbox("Estado", options=UFS, index=UFS.index("SP"))
if not uf_escolhida:
    st.sidebar.error("Selecione um Estado (UF).")

# Municípios (lista IBGE por UF) com fallback manual
df_ibge_uf = ibge_por_uf(uf_escolhida)
opcoes_municipios = {f"{r.municipio} / {r.uf}": r.codigo_ibge for _, r in df_ibge_uf.iterrows()} if not df_ibge_uf.empty else {}

if not opcoes_municipios:
    st.sidebar.warning("Falha ao carregar municípios do IBGE. Informe IBGEs manualmente (separados por vírgula).")
    ibge_manual = st.sidebar.text_input("IBGEs (manual)", value="")
    codigos_ibge_escolhidos = [x.strip() for x in ibge_manual.split(",") if x.strip()]
else:
    municipios_labels = st.sidebar.multiselect("Municipios", options=list(opcoes_municipios.keys()))
    codigos_ibge_escolhidos = [opcoes_municipios[l] for l in municipios_labels]

# Status
status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

# Modalidades (obrigatórias para /publicacao)
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
        "ibges": codigos_ibge_escolhidos,
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
        f"- Municípios: `{len(p.get('ibges', []))}`\n"
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
        total_baixado_uf = None
        regs = []

        if status_label == "Recebendo Proposta":
            # Turbo por município quando seleção pequena; UF quando grande
            if 0 < len(codigos_ibge_escolhidos) <= MUNICIPALITY_QUERY_THRESHOLD:
                regs, total_baixado = consultar_proposta_por_municipios(
                    uf_escolhida, palavra_chave, codigos_ibge_escolhidos
                )
            else:
                regs, total_baixado = consultar_proposta_por_uf(
                    uf_escolhida, palavra_chave, codigos_ibge_escolhidos
                )
                total_baixado_uf = total_baixado
        else:
            cod_modalidades = [x.strip() for x in modalidades_str.split(",") if x.strip()]
            if not cod_modalidades:
                st.warning(
                    "Para **Propostas Encerradas / Encerradas / Todos**, informe **códigos de modalidade** "
                    "(campo na sidebar). O endpoint /publicacao exige esse parâmetro."
                )
                regs, total_baixado = [], 0
                total_baixado_uf = 0
            else:
                if 0 < len(codigos_ibge_escolhidos) <= MUNICIPALITY_QUERY_THRESHOLD:
                    regs, total_baixado = consultar_publicacao_por_municipios_modalidades(
                        uf=uf_escolhida,
                        palavra_chave=palavra_chave,
                        ibges=codigos_ibge_escolhidos,
                        status_label=status_label,
                        codigos_modalidade=cod_modalidades,
                    )
                else:
                    regs, total_baixado = consultar_publicacao_por_uf_modalidades(
                        uf=uf_escolhida,
                        palavra_chave=palavra_chave,
                        ibges=codigos_ibge_escolhidos,
                        status_label=status_label,
                        codigos_modalidade=cod_modalidades,
                    )
                    total_baixado_uf = total_baixado

        df = normalizar_df(regs)

        # Auditoria: municípios selecionados sem retorno
        selected_set = set(_as_str(x) for x in (codigos_ibge_escolhidos or []))
        presentes = set(_as_str((r.get("unidadeOrgao") or {}).get("codigoIbge")) for r in regs)
        sem_resultado = sorted(list(selected_set - presentes))
        nomes_por_ibge = {v: k for k, v in ({} if not opcoes_municipios else opcoes_municipios.items())}
        nomes_sem = [nomes_por_ibge.get(ibge, ibge) for ibge in sem_resultado]

        st.subheader("Resultados")
        hoje_txt = _yyyymmdd(date.today())
        st.caption(
            f"UF **{uf_escolhida}** • Municípios selecionados **{len(codigos_ibge_escolhidos)}** • "
            f"Status **{status_label}** • Palavra-chave **{palavra_chave or '—'}** • Execução **{hoje_txt}**"
        )

        if total_baixado_uf is not None:
            st.info(f"Coleta por UF: {total_baixado_uf} item(ns) recebidos do PNCP; após filtros: {len(df)}.")
        else:
            st.info(f"Coleta por municípios selecionados: {len(df)} item(ns) após filtros.")

        if nomes_sem:
            st.warning(f"Sem resultados para: {', '.join(nomes_sem)}")

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
        # Resposta não-JSON ou não decodificável (com tolerância a vazio implementada)
        st.error(f"Erro de parsing: {e}")
    except Exception as e:
        st.error(f"Falha inesperada: {e}")

else:
    st.info(
        "Configure os filtros na **sidebar** e clique em **Executar Pesquisa**.\n\n"
        "- **Estado (UF)** é obrigatório.\n"
        "- **Municípios** são do **IBGE**. Até 25 municípios: consulta por município (rápida). "
        "Acima disso: consulta por **UF** e filtro client-side.\n"
        "- **'Recebendo Proposta'** usa `/proposta` com `dataFinal=yyyyMMdd`.\n"
        "- **'Propostas Encerradas' / 'Encerradas' / 'Todos'** usam `/publicacao` e exigem **códigos de modalidade**.\n"
        "- Respostas vazias (204/200 sem corpo) são tratadas como **0 resultados** — sem erro."
    )
