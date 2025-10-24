# app.py — 📑 Acerte Licitações — Buscador de Editais (UF obrigatório, municípios por IBGE)
# Execução: streamlit run app.py
# Requisitos: streamlit, requests, pandas, xlsxwriter (ou openpyxl)

from __future__ import annotations
import io
import time
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

# =========================
# Config & Constantes
# =========================
st.set_page_config(page_title="📑 Acerte Licitações", page_icon="📑", layout="wide")

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"       # para “Recebendo Proposta” (não exige modalidade)
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # exige codigoModalidadeContratacao
PAGE_SIZE = 50

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]
PUBLICACAO_JANELA_DIAS = 60

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

def _classificar_status(nome: Optional[str]) -> str:
    s = _normalize_text(nome).lower()
    if "receb" in s:                          # A receber / Recebendo propostas
        return "Recebendo Proposta"
    if "julg" in s or "propostas encerradas" in s:
        return "Propostas Encerradas"
    if "encerrad" in s:
        return "Encerradas"
    return "Todos"


# =========================
# IBGE — municípios por UF (com tratamento defensivo)
# =========================
@st.cache_data(show_spinner=False, ttl=60*60*24)
def carregar_ibge_df() -> pd.DataFrame:
    """Baixa e normaliza a lista IBGE. Retorna DataFrame com (municipio, uf, codigo_ibge:str)."""
    try:
        r = requests.get(IBGE_URL, timeout=60)
        r.raise_for_status()
        data = r.json()
        rows = []
        # estrutura esperada: lista de dicts com microrregiao->mesorregiao->UF->sigla
        for m in data if isinstance(data, list) else []:
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
            raise RuntimeError("Retorno IBGE vazio ou fora do formato esperado.")
        df = df.sort_values(["uf", "municipio"]).reset_index(drop=True)
        return df
    except Exception as e:
        # fallback: retorna DF vazio; a UI permitirá entrada manual
        return pd.DataFrame(columns=["municipio", "uf", "codigo_ibge"])

@st.cache_data(show_spinner=False, ttl=60*60*24)
def ibge_por_uf(uf: str) -> pd.DataFrame:
    df = carregar_ibge_df()
    if df.empty:
        return df
    return df[df["uf"] == uf].copy()


# =========================
# PNCP — iteração paginada
# =========================
def _iterar_paginas(endpoint: str, params_base: Dict[str, str], sleep_s: float = 0.05):
    pagina = 1
    while True:
        params = dict(params_base)
        params.update({"pagina": pagina, "tamanhoPagina": PAGE_SIZE})
        r = requests.get(endpoint, params=params, timeout=60)
        # Tratamento mais informativo de erro HTTP
        try:
            r.raise_for_status()
        except requests.HTTPError as http_err:
            detalhe = ""
            try:
                detalhe = r.text[:500]
            except Exception:
                pass
            raise requests.HTTPError(f"{http_err}\nDetalhe PNCP: {detalhe}") from http_err

        payload = r.json()
        dados = payload.get("data") or []
        if not dados:
            break
        yield pagina, payload.get("totalPaginas"), dados
        numero = payload.get("numeroPagina") or pagina
        total_pag = payload.get("totalPaginas")
        if total_pag and numero >= total_pag:
            break
        pagina += 1
        time.sleep(sleep_s)


# =========================
# Consultas — conforme Status
# =========================
def consultar_proposta_por_uf(uf: str, palavra_chave: str, ibges: List[str]) -> List[dict]:
    """/proposta (dataFinal = hoje). Não requer modalidade. Usado para 'Recebendo Proposta'."""
    params_base = {"uf": uf, "dataFinal": date.today().isoformat()}
    acumulado = []
    for _, _, dados in _iterar_paginas(ENDP_PROPOSTA, params_base):
        # filtra município client-side (IBGE)
        if ibges:
            ibge_set = set(ibges)
            dados = [d for d in dados if ((d.get("unidadeOrgao") or {}).get("codigoIbge") in ibge_set)]
        # filtra palavra-chave client-side
        if palavra_chave:
            p = palavra_chave.strip().lower()
            def _hit(d):
                uo = d.get("unidadeOrgao") or {}
                texto = " ".join([
                    _normalize_text(d.get("objetoCompra")),
                    _normalize_text(d.get("informacaoComplementar")),
                    _normalize_text(uo.get("nomeUnidade")),
                ]).lower()
                return p in texto
            dados = [d for d in dados if _hit(d)]
        acumulado.extend(dados)
    return acumulado

def consultar_publicacao_por_uf_modalidades(
    uf: str,
    palavra_chave: str,
    ibges: List[str],
    status_label: str,
    codigos_modalidade: List[str],
    dias_janela: int = PUBLICACAO_JANELA_DIAS,
) -> List[dict]:
    """
    /publicacao exige codigoModalidadeContratacao.
    Rodamos 1 chamada por modalidade informada e agregamos.
    """
    if not codigos_modalidade:
        # Sem modalidade não há como cumprir o contrato da API. Retorna vazio.
        return []

    hoje = date.today()
    params_comuns = {
        "uf": uf,
        "dataInicial": (hoje - timedelta(days=dias_janela)).isoformat(),
        "dataFinal": hoje.isoformat(),
    }

    acumulado = []
    for cod in codigos_modalidade:
        params = dict(params_comuns)
        params["codigoModalidadeContratacao"] = str(cod)
        for _, _, dados in _iterar_paginas(ENDP_PUBLICACAO, params):
            # status bucket client-side
            if status_label != "Todos":
                dados = [d for d in dados if _classificar_status(d.get("situacaoCompraNome")) == status_label]
            # município (IBGE) client-side
            if ibges:
                ibge_set = set(ibges)
                dados = [d for d in dados if ((d.get("unidadeOrgao") or {}).get("codigoIbge") in ibge_set)]
            # palavra-chave client-side
            if palavra_chave:
                p = palavra_chave.strip().lower()
                def _hit(d):
                    uo = d.get("unidadeOrgao") or {}
                    texto = " ".join([
                        _normalize_text(d.get("objetoCompra")),
                        _normalize_text(d.get("informacaoComplementar")),
                        _normalize_text(uo.get("nomeUnidade")),
                    ]).lower()
                    return p in texto
                dados = [d for d in dados if _hit(d)]
            acumulado.extend(dados)
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
            "IBGE": uo.get("codigoIbge"),
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
# Sidebar — conforme seu checklist
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
    st.sidebar.warning("Falha ao carregar municípios do IBGE. Insira manualmente (IBGEs separados por vírgula).")
    ibge_manual = st.sidebar.text_input("IBGEs (manual)", value="")
    codigos_ibge_escolhidos = [x.strip() for x in ibge_manual.split(",") if x.strip()]
else:
    municipios_labels = st.sidebar.multiselect("Municipios", options=list(opcoes_municipios.keys()))
    codigos_ibge_escolhidos = [opcoes_municipios[l] for l in municipios_labels]

# Status (quatro opções)
status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

# (Novo) Modalidades — necessário quando o Status não for "Recebendo Proposta"
modalidades_str = ""
if status_label != "Recebendo Proposta":
    modalidades_str = st.sidebar.text_input(
        "Modalidades (códigos PNCP, separados por vírgula)",
        value="",
        placeholder="Ex.: 5, 6, 7  (se não preencher, só 'Recebendo Proposta' poderá ser executado)"
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
        if status_label == "Recebendo Proposta":
            regs = consultar_proposta_por_uf(uf_escolhida, palavra_chave, codigos_ibge_escolhidos)
        else:
            # exige modalidades
            cod_modalidades = [x.strip() for x in modalidades_str.split(",") if x.strip()]
            if not cod_modalidades:
                st.warning(
                    "Para executar filtros de **Propostas Encerradas / Encerradas / Todos**, "
                    "informe **códigos de modalidade** na sidebar. Sem isso, a API /publicacao retorna erro."
                )
                regs = []
            else:
                regs = consultar_publicacao_por_uf_modalidades(
                    uf=uf_escolhida,
                    palavra_chave=palavra_chave,
                    ibges=codigos_ibge_escolhidos,
                    status_label=status_label,
                    codigos_modalidade=cod_modalidades,
                )

        df = normalizar_df(regs)

        st.subheader("Resultados")
        st.caption(
            f"UF **{uf_escolhida}** • Municípios selecionados **{len(codigos_ibge_escolhidos)}** • "
            f"Status **{status_label}** • Palavra-chave **{palavra_chave or '—'}**"
        )

        if df.empty:
            st.warning("Nenhum resultado para os filtros aplicados.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            xlsx = _xlsx_bytes(df, sheet_name="editais")
            st.download_button(
                label="⬇️ Baixar XLSX",
                data=xlsx,
                file_name=f"editais_{uf_escolhida}_{status_label}_{date.today().isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except requests.HTTPError as e:
        st.error(f"Erro na API PNCP: {e}")
    except Exception as e:
        st.error(f"Falha inesperada: {e}")

else:
    st.info(
        "Configure os filtros na **sidebar** e clique em **Executar Pesquisa**.\n\n"
        "- **Estado (UF)** é obrigatório.\n"
        "- **Municípios** vêm do **IBGE**; em caso de indisponibilidade, informe os **IBGEs manualmente**.\n"
        "- **Status 'Recebendo Proposta'** usa a API `/proposta` (não exige modalidade).\n"
        "- Para **Propostas Encerradas / Encerradas / Todos**, informe **códigos de modalidade** (API `/publicacao`)."
    )
