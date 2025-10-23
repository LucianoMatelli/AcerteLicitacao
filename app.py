# app.py — 📑 Acerte Licitações — O seu Buscador de Editais
# Requisitos: streamlit, requests, pandas, openpyxl/xlsxwriter
# Execução:  streamlit run app.py

from __future__ import annotations
import io
import math
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
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"       # requer dataFinal
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # requer dataInicial e dataFinal

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

STATUS_OPCOES = {
    "Recebendo propostas (janela aberta)": "proposta",
    "Publicadas (últimos 30 dias)": "publicacao",
}

PAGE_SIZE = 50  # limite máximo da API para consulta (conforme swagger)


# =========================
# Utils
# =========================
def _normalize_text(s: Optional[str]) -> str:
    return (s or "").strip()


def _xlsx_bytes(df: pd.DataFrame, sheet_name: str = "resultados") -> bytes:
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        return buffer.getvalue()


# =========================
# Data Access (API PNCP)
# =========================
@st.cache_data(show_spinner=False, ttl=60 * 60)
def listar_municipios_por_uf(uf: str, data_final_iso: str) -> List[Dict]:
    """
    Deriva os municípios a partir dos editais da UF no endpoint 'proposta'.
    Saída: lista única [{municipio, uf, codigo_ibge}].
    """
    pagina = 1
    vistos: Dict[str, Dict] = {}
    total_paginas_detectado = None

    while True:
        params = {
            "uf": uf,
            "dataFinal": data_final_iso,
            "pagina": pagina,
            "tamanhoPagina": PAGE_SIZE,
        }
        r = requests.get(ENDP_PROPOSTA, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        itens = payload.get("data") or []
        if not itens:
            break

        for it in itens:
            uo = (it.get("unidadeOrgao") or {})
            nome = uo.get("municipioNome")
            ibge = uo.get("codigoIbge")
            sigla = uo.get("ufSigla") or uf
            if nome and ibge:
                vistos.setdefault(str(ibge), {"municipio": nome, "uf": sigla, "codigo_ibge": str(ibge)})

        # paginação
        numero = payload.get("numeroPagina") or pagina
        total_paginas_detectado = payload.get("totalPaginas")
        if total_paginas_detectado and numero >= total_paginas_detectado:
            break
        pagina += 1

        # cortesia: breve respiro para não sobrecarregar
        time.sleep(0.1)

    out = list(vistos.values())
    out.sort(key=lambda x: (x["uf"], x["municipio"]))
    return out


def _iterar_paginas(endpoint: str, params_base: Dict[str, str], progresso: Optional[st.progress] = None):
    """
    Itera páginas de um endpoint (/proposta ou /publicacao), rendendo cada 'data' parcial.
    Atualiza progress bar se fornecida (estimativa com base em totalPaginas quando disponível).
    """
    pagina = 1
    total_pag = None

    while True:
        params = dict(params_base)
        params.update({"pagina": pagina, "tamanhoPagina": PAGE_SIZE})
        r = requests.get(endpoint, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        # metadados
        total_pag = total_pag or payload.get("totalPaginas")
        dados = payload.get("data") or []
        if not dados:
            break

        yield pagina, total_pag, dados

        # progress bar
        if progresso is not None and total_pag:
            progresso.progress(min(1.0, pagina / float(total_pag)))

        numero = payload.get("numeroPagina") or pagina
        if total_pag and numero >= total_pag:
            break
        pagina += 1
        time.sleep(0.05)


def consultar_editais(
    palavra_chave: str,
    uf: str,
    codigos_ibge: List[str],
    status_api: str,
) -> pd.DataFrame:
    """
    Consulta editais conforme filtros. Para 'proposta', usa dataFinal=hoje.
    Para 'publicacao', usa janela [hoje-30, hoje].
    Filtro por município é aplicado client-side (subset) — simples e robusto.
    """
    hoje = date.today()
    params_base = {"uf": uf}

    if status_api == "proposta":
        endpoint = ENDP_PROPOSTA
        params_base["dataFinal"] = hoje.isoformat()
    else:
        endpoint = ENDP_PUBLICACAO
        params_base["dataInicial"] = (hoje - timedelta(days=30)).isoformat()
        params_base["dataFinal"] = hoje.isoformat()

    if palavra_chave:
        # A API de consulta não expõe 'q' textual explícito no swagger;
        # muitas implantações usam 'objetoCompra' client-side. Aqui aplicamos filtro pós-busca (robusto).
        palavra_chave = palavra_chave.strip().lower()

    barra = st.progress(0.0)
    acumulado = []

    for pagina, total_pag, dados in _iterar_paginas(endpoint, params_base, progresso=barra):
        # filtro client-side por municípios
        if codigos_ibge:
            ibge_set = set(str(x) for x in codigos_ibge)
            dados = [d for d in dados if ((d.get("unidadeOrgao") or {}).get("codigoIbge") in ibge_set)]

        # filtro client-side por palavra-chave no objeto/descrição
        if palavra_chave:
            def _tem_palavra(d):
                alvo = " ".join([
                    _normalize_text(d.get("objetoCompra")),
                    _normalize_text(d.get("informacaoComplementar")),
                    _normalize_text((d.get("unidadeOrgao") or {}).get("nomeUnidade")),
                ]).lower()
                return palavra_chave in alvo
            dados = [d for d in dados if _tem_palavra(d)]

        acumulado.extend(dados)

        # ajuste progressivo quando total_pag é desconhecido
        if total_pag is None:
            # heurística simples
            barra.progress(min(1.0, min(0.9, pagina * 0.1)))

    barra.progress(1.0)

    # Normalização de campos-chave
    linhas = []
    for d in acumulado:
        uo = d.get("unidadeOrgao") or {}
        linha = {
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
            "Situação": d.get("situacaoCompraNome"),
            "Link Origem": d.get("linkSistemaOrigem"),
            "Controle PNCP": d.get("numeroControlePNCP"),
        }
        linhas.append(linha)

    df = pd.DataFrame(linhas)
    # Ordena por data de publicação (quando disponível) desc, depois município
    if "Publicação PNCP" in df.columns:
        df = df.sort_values(by=["Publicação PNCP", "Município"], ascending=[False, True])
    return df


# =========================
# Sidebar — Filtros & Pesquisas Salvas
# =========================
st.sidebar.header("Filtros")

# Palavra-chave
palavra_chave = st.sidebar.text_input("Palavra chave", value="")

# Estado (obrigatório)
uf_escolhida = st.sidebar.selectbox("Estado", options=UFS, index=UFS.index("SP"))
if not uf_escolhida:
    st.sidebar.error("Selecione um Estado (UF).")

# Municípios (derivados da UF via endpoint 'proposta')
# Carrega/atualiza automaticamente ao trocar a UF (com cache de 1h)
data_final_ref = date.today().isoformat()  # referência para derivar municípios
try:
    municipios_derivados = listar_municipios_por_uf(uf_escolhida, data_final_ref)
except Exception as e:
    st.sidebar.warning(f"Não foi possível derivar municípios para {uf_escolhida}: {e}")
    municipios_derivados = []

label_to_ibge = {f"{m['municipio']} / {m['uf']}": m["codigo_ibge"] for m in municipios_derivados}
municipios_selecionados_labels = st.sidebar.multiselect(
    "Municípios",
    options=list(label_to_ibge.keys()),
    default=[],
    help="Lista derivada dos editais com propostas abertas hoje para a UF selecionada.",
)
codigos_ibge_escolhidos = [label_to_ibge[l] for l in municipios_selecionados_labels]

# Status
status_label = st.sidebar.selectbox("Status", options=list(STATUS_OPCOES.keys()), index=0)
status_api = STATUS_OPCOES[status_label]

st.sidebar.markdown("---")

# Salvar pesquisa
if "pesquisas_salvas" not in st.session_state:
    st.session_state["pesquisas_salvas"] = {}  # nome -> dict params

nome_pesquisa = st.sidebar.text_input("Salvar pesquisa", value="", placeholder="Ex.: SP Educação — propostas")
if st.sidebar.button("Salvar pesquisa"):
    if not uf_escolhida:
        st.sidebar.error("Para salvar, selecione um Estado (UF).")
    else:
        params = {
            "palavra_chave": palavra_chave,
            "uf": uf_escolhida,
            "codigos_ibge": codigos_ibge_escolhidos,
            "status_api": status_api,
        }
        if nome_pesquisa.strip():
            st.session_state["pesquisas_salvas"][nome_pesquisa.strip()] = params
            st.sidebar.success(f"Pesquisa salva: {nome_pesquisa.strip()}")
        else:
            st.sidebar.error("Informe um nome para salvar a pesquisa.")

# Pesquisas salvas
salvos = st.session_state.get("pesquisas_salvas", {})
escolha_salva = st.sidebar.selectbox(
    "Pesquisas salvas",
    options=["—"] + list(salvos.keys()),
    index=0,
)
if escolha_salva != "—":
    params = salvos[escolha_salva]
    # rehidrata controles (apenas exibição informativa; não alteramos os widgets já renderizados)
    st.sidebar.info(
        f"Selecionado: **{escolha_salva}**\n\n"
        f"- Palavra chave: `{params.get('palavra_chave','')}`\n"
        f"- UF: `{params.get('uf')}`\n"
        f"- Municípios: `{len(params.get('codigos_ibge', []))}` selecionados\n"
        f"- Status: `{ 'Recebendo propostas' if params.get('status_api')=='proposta' else 'Publicadas (30d)'}`"
    )

st.sidebar.markdown("---")
executar = st.sidebar.button("Executar Pesquisa")


# =========================
# Corpo — Resultados
# =========================
st.title("📑 Acerte Licitações — O seu Buscador de Editais")

if executar:
    if not uf_escolhida:
        st.error("Operação cancelada: é obrigatório selecionar um Estado (UF).")
        st.stop()

    with st.spinner("Consultando PNCP e consolidando resultados..."):
        df = consultar_editais(
            palavra_chave=palavra_chave,
            uf=uf_escolhida,
            codigos_ibge=codigos_ibge_escolhidos,
            status_api=status_api,
        )

    st.subheader("Resultados")
    st.caption(
        f"Filtros: UF **{uf_escolhida}** • Municípios selecionados **{len(codigos_ibge_escolhidos)}** • "
        f"Status **{status_label}** • Palavra-chave **{palavra_chave or '—'}**"
    )

    # Tabela primeiro (baseline UX)
    if df.empty:
        st.warning("Nenhum resultado para os filtros aplicados.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download XLSX (apenas XLSX, conforme baseline)
        xlsx = _xlsx_bytes(df, sheet_name="editais")
        st.download_button(
            label="⬇️ Baixar XLSX",
            data=xlsx,
            file_name=f"editais_{uf_escolhida}_{status_api}_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info(
        "Configure os filtros na **sidebar** e clique em **Executar Pesquisa**.\n\n"
        "- O campo **Estado** é obrigatório.\n"
        "- A lista de **Municípios** é derivada automaticamente dos editais com propostas **abertas hoje** na UF.\n"
        "- Para **Publicadas (últimos 30 dias)**, o sistema usa uma janela de 30 dias."
    )
