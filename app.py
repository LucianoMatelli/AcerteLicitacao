# app.py — 📑 Acerte Licitações — O seu Buscador de Editais
# Execução:  streamlit run app.py
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
st.set_page_config(
    page_title="📑 Acerte Licitações — O seu Buscador de Editais",
    page_icon="📑",
    layout="wide",
)

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # janela [dataInicial, dataFinal] (estável)
PAGE_SIZE = 50                                           # por Swagger

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

# Status exigidos na UI (classificação client-side)
STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]
PUBLICACAO_JANELA_DIAS = 60  # janela default p/ publicações

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
    """Bucketiza 'situacaoCompraNome' para a taxonomia exigida na UI."""
    s = (_normalize_text(nome)).lower()
    if "receb" in s:                          # A Receber/Recebendo Propostas
        return "Recebendo Proposta"
    if "julg" in s or "propostas encerradas" in s:
        return "Propostas Encerradas"
    if "encerrad" in s:                       # Encerradas
        return "Encerradas"
    return "Todos"

# =========================
# IBGE — Fonte oficial para o multiselect de municípios
# =========================
IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"

@st.cache_data(show_spinner=False, ttl=60*60*24)
def carregar_ibge() -> pd.DataFrame:
    """
    Baixa a lista completa de municípios do IBGE (nome, UF, código) e retorna DataFrame.
    Colunas: municipio, uf, codigo_ibge (str).
    """
    r = requests.get(IBGE_URL, timeout=60)
    r.raise_for_status()
    data = r.json()
    rows = []
    for m in data:
        nome = m["nome"]
        uf = m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
        codigo = str(m["id"])
        rows.append({"municipio": nome, "uf": uf, "codigo_ibge": codigo})
    df = pd.DataFrame(rows).sort_values(["uf", "municipio"]).reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False, ttl=60*60*24)
def ibge_por_uf(uf: str) -> pd.DataFrame:
    df = carregar_ibge()
    return df[df["uf"] == uf].copy()

# =========================
# PNCP — Consulta
# =========================
def _iterar_paginas(endpoint: str, params_base: Dict[str, str], sleep_s: float = 0.05):
    pagina = 1
    while True:
        params = dict(params_base)
        params.update({"pagina": pagina, "tamanhoPagina": PAGE_SIZE})
        r = requests.get(endpoint, params=params, timeout=60)
        r.raise_for_status()
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

def consultar_editais(
    palavra_chave: str,
    uf: str,
    codigos_ibge: List[str],
    status_label: str,
    data_ini_iso: str,
    data_fim_iso: str,
) -> pd.DataFrame:
    """
    Estratégia pedida:
    1) Buscar no PNCP por UF (server-side) na janela temporal escolhida (/publicacao).
    2) Filtrar client-side pelos municípios selecionados (IBGE), status e palavra-chave.
    """
    params_base = {"uf": uf, "dataInicial": data_ini_iso, "dataFinal": data_fim_iso}

    barra = st.progress(0.0)
    acumulado = []
    pagina_atual = 0
    total_pag = None

    for pagina, total_pag, dados in _iterar_paginas(ENDP_PUBLICACAO, params_base):
        pagina_atual = pagina

        # Filtro por municípios (IBGE) — conforme sua estratégia
        if codigos_ibge:
            ibge_set = set(codigos_ibge)
            dados = [d for d in dados if ((d.get("unidadeOrgao") or {}).get("codigoIbge") in ibge_set)]

        # Filtro por status (classificação client-side)
        if status_label != "Todos":
            dados = [d for d in dados if _classificar_status(d.get("situacaoCompraNome")) == status_label]

        # Filtro por palavra-chave (robusto; sem depender de parâmetro textual da API)
        if palavra_chave:
            palavra = palavra_chave.strip().lower()
            def _tem_palavra(d):
                uo = d.get("unidadeOrgao") or {}
                texto = " ".join([
                    _normalize_text(d.get("objetoCompra")),
                    _normalize_text(d.get("informacaoComplementar")),
                    _normalize_text(uo.get("nomeUnidade")),
                ]).lower()
                return palavra in texto
            dados = [d for d in dados if _tem_palavra(d)]

        acumulado.extend(dados)

        # Progress
        if total_pag:
            barra.progress(min(1.0, pagina_atual / float(total_pag)))
        else:
            barra.progress(min(0.9, pagina_atual * 0.1))

    barra.progress(1.0)

    # Normalização
    linhas = []
    for d in acumulado:
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
# Sidebar — Filtros (exatos conforme seu pedido)
# =========================
st.sidebar.header("Filtros")

# 1) Palavra chave
palavra_chave = st.sidebar.text_input("Palavra chave", value="")

# 2) Estado (obrigatório)
uf_escolhida = st.sidebar.selectbox("Estado", options=UFS, index=UFS.index("SP"))
if not uf_escolhida:
    st.sidebar.error("Selecione um Estado (UF).")

# 3) Municípios (lista do IBGE, filtrada pela UF)
try:
    df_ibge_uf = ibge_por_uf(uf_escolhida)
    opcoes_municipios = {f"{row.municipio} / {row.uf}": row.codigo_ibge for _, row in df_ibge_uf.iterrows()}
except Exception as e:
    st.sidebar.warning(f"Falha ao carregar municípios do IBGE: {e}")
    opcoes_municipios = {}

municipios_selecionados_labels = st.sidebar.multiselect("Municipios", options=list(opcoes_municipios.keys()))
codigos_ibge_escolhidos = [opcoes_municipios[l] for l in municipios_selecionados_labels]

# 4) Status — quatro opções
status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

# 5) Salvar pesquisa
if "pesquisas_salvas" not in st.session_state:
    st.session_state["pesquisas_salvas"] = {}
nome_pesquisa = st.sidebar.text_input("Salvar pesquisa", value="", placeholder="Ex.: SP — Saúde — Encerradas")

if st.sidebar.button("Salvar pesquisa"):
    if not uf_escolhida:
        st.sidebar.error("Para salvar, selecione um Estado (UF).")
    else:
        st.session_state["pesquisas_salvas"][nome_pesquisa.strip() or f"Pesquisa {len(st.session_state['pesquisas_salvas'])+1}"] = {
            "palavra_chave": palavra_chave,
            "uf": uf_escolhida,
            "codigos_ibge": codigos_ibge_escolhidos,
            "status_label": status_label,
        }
        st.sidebar.success("Pesquisa salva.")

# 6) Pesquisas salvas
salvos = st.session_state.get("pesquisas_salvas", {})
escolha_salva = st.sidebar.selectbox("Pesquisas salvas", options=["—"] + list(salvos.keys()), index=0)
if escolha_salva != "—":
    p = salvos[escolha_salva]
    st.sidebar.info(
        f"**{escolha_salva}**\n\n"
        f"- Palavra chave: `{p.get('palavra_chave','')}`\n"
        f"- UF: `{p.get('uf')}`\n"
        f"- Municípios: `{len(p.get('codigos_ibge', []))}`\n"
        f"- Status: `{p.get('status_label')}`"
    )

st.sidebar.markdown("---")
executar = st.sidebar.button("Executar Pesquisa")

# =========================
# Corpo — Resultados
# =========================
st.title("📑 Acerte Licitações — O seu Buscador de Editais")

# Janela temporal padrão para /publicacao
hoje = date.today()
data_ini_iso = (hoje - timedelta(days=PUBLICACAO_JANELA_DIAS)).isoformat()
data_fim_iso = hoje.isoformat()

if executar:
    if not uf_escolhida:
        st.error("Operação cancelada: o campo **Estado** é obrigatório.")
        st.stop()

    with st.spinner("Consultando PNCP e consolidando resultados..."):
        df = consultar_editais(
            palavra_chave=palavra_chave,
            uf=uf_escolhida,
            codigos_ibge=codigos_ibge_escolhidos,
            status_label=status_label,
            data_ini_iso=data_ini_iso,
            data_fim_iso=data_fim_iso,
        )

    st.subheader("Resultados")
    st.caption(
        f"UF **{uf_escolhida}** • Municípios selecionados **{len(codigos_ibge_escolhidos)}** • "
        f"Status **{status_label}** • Palavra-chave **{palavra_chave or '—'}** • "
        f"Janela **{data_ini_iso} → {data_fim_iso}**"
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
else:
    st.info(
        "Configure os filtros na **sidebar** e clique em **Executar Pesquisa**.\n\n"
        "- **Estado (UF)** é obrigatório.\n"
        "- **Municípios** vêm do **IBGE** (filtrados pela UF) e são aplicados **client-side** após a busca por UF.\n"
        "- **Status** é classificado client-side (Recebendo Proposta, Propostas Encerradas, Encerradas, Todos).\n"
        "- Janela padrão para **Publicadas**: últimos 60 dias."
    )
