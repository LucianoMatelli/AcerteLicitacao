# app.py — 📑 Acerte Licitações — UF-only + batching por datas + filtro client-side por municípios
# Execução: streamlit run app.py
# Requisitos: streamlit, requests, pandas, xlsxwriter (ou openpyxl)

from __future__ import annotations
import io
import time
import json
from datetime import date, timedelta
from typing import Dict, List, Optional, Iterable, Set, Tuple

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

PAGE_SIZE = 50                  # tamanho de página principal
ALT_PAGE_SIZE = 20              # fallback se a API ficar instável
MAX_BLANK_PAGES = 2             # encerra paginação após N páginas consecutivas vazias
RETRY_PER_PAGE = 3              # tentativas por página
SLEEP_BETWEEN_PAGES = 0.03

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]
PUBLICACAO_JANELA_DIAS = 60

# IBGE — apenas para montar a lista de municípios na sidebar
IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"

# Batching por datas no /proposta (janela móvel retroativa). Ajustável na sidebar.
PROPOSTA_DIAS_RETROATIVOS_DEFAULT = 28
PROPOSTA_PASSO_DIAS_DEFAULT = 7


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
    return _normalize_text(s).casefold()

def _uniq_key_processo(d: dict) -> Tuple:
    """
    Chave de deduplicação robusta para compras:
    - preferir 'numeroControlePNCP' se existir
    - senão: (anoCompra, sequencialCompra, cnpj do órgão)
    """
    ncp = d.get("numeroControlePNCP")
    if ncp:
        return ("NCP", _as_str(ncp))
    ano = d.get("anoCompra")
    seq = d.get("sequencialCompra")
    org = (d.get("orgaoEntidade") or {}).get("cnpj")
    return ("LEGACY", _as_str(ano), _as_str(seq), _as_str(org))


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
# PNCP — paginação (retry + scroll contínuo)
# =========================
def _paginacao(endpoint: str, params_base: Dict[str, str], page_size: int = PAGE_SIZE) -> Iterable[list]:
    """
    Itera páginas até ocorrerem MAX_BLANK_PAGES consecutivas vazias.
    Ignora 'totalPaginas' do payload (próprio PNCP pode truncar).
    Faz até RETRY_PER_PAGE tentativas por página, com backoff incremental.
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
                        break  # tenta próxima página (pode ser truncagem)
                else:
                    blank_streak = 0
                    yield dados
                    break
            except Exception as e:
                last_err = e
                time.sleep(0.3 * attempt)
        if last_err is not None:
            # fallback de page_size 50 -> 20 para contornar instabilidade
            if page_size != ALT_PAGE_SIZE:
                # recomeça esta mesma página com page_size menor
                for lote in _paginacao(endpoint, params_base, page_size=ALT_PAGE_SIZE):
                    yield lote
                return
            else:
                raise last_err

        pagina += 1
        time.sleep(SLEEP_BETWEEN_PAGES)


# =========================
# Consultas — UF-only (NÃO usar codigoMunicipioIbge)
# =========================
def _consultar_proposta_uf_na_data(uf: str, data_final: date) -> List[dict]:
    """Consulta /proposta por UF em um 'corte' de dataFinal (yyyyMMdd)."""
    params = {"uf": uf, "dataFinal": _yyyymmdd(data_final)}
    acumulado = []
    for dados in _paginacao(ENDP_PROPOSTA, params, page_size=PAGE_SIZE):
        acumulado.extend(dados)
    return acumulado

def consultar_proposta_uf_batched(uf: str, dias_retro: int, passo_dias: int) -> List[dict]:
    """
    'Amostragem expandida': executa várias coletas por UF variando dataFinal
    (ex.: hoje, hoje-7, hoje-14, hoje-21), agrega e DEDUPLICA por numeroControlePNCP/ano+seq+cnpj.
    """
    hoje = date.today()
    cortes = [hoje - timedelta(days=delta) for delta in range(0, max(1, dias_retro)+1, max(1, passo_dias))]
    barra = st.progress(0.0)
    total_lotes = len(cortes)
    registros: Dict[Tuple, dict] = {}
    total_baixado_somas = 0

    for idx, corte in enumerate(cortes, start=1):
        lote = _consultar_proposta_uf_na_data(uf, corte)
        total_baixado_somas += len(lote)
        for d in lote:
            registros[_uniq_key_processo(d)] = d  # dedup
        barra.progress(min(1.0, idx / float(total_lotes)))
        time.sleep(0.02)

    st.session_state["_telemetria_proposta_total_uf"] = total_baixado_somas
    return list(registros.values())

def consultar_publicacao_por_uf_modalidades(uf: str, codigos_modalidade: List[str],
                                            dias_janela: int = PUBLICACAO_JANELA_DIAS) -> List[dict]:
    """
    /publicacao por UF (datas yyyyMMdd) — agrega por modalidade; ignora codigoMunicipioIbge.
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
        for dados in _paginacao(ENDP_PUBLICACAO, params, page_size=PAGE_SIZE):
            total_baixado += len(dados)
            acumulado.extend(dados)
        barra.progress(min(1.0, idx / float(total_passos)))
        time.sleep(0.02)
    st.session_state["_telemetria_publicacao_total_uf"] = total_baixado
    return acumulado


# =========================
# Filtragem client-side
# =========================
def filtrar_por_status_palavra(dados: list, palavra_chave: str, status_label: str) -> list:
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

def filtrar_por_municipios(dados: list, nomes_municipios: Set[str]) -> list:
    if not nomes_municipios:
        return dados
    keys = set(_mun_key(n) for n in nomes_municipios)
    out = []
    for d in dados:
        uo = d.get("unidadeOrgao") or {}
        if _mun_key(uo.get("municipioNome")) in keys:
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
    opcoes_municipios = [f"{row.municipio} / {row.uf}" for _, row in df_ibge_uf.iterrows()]
    labels_escolhidos = st.sidebar.multiselect("Municipios", options=opcoes_municipios)
    municipios_selecionados = [lab.split(" / ")[0] for lab in labels_escolhidos]

# Status
status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

# Janela (somente para /proposta; controla a amostragem expandida)
with st.sidebar.expander("Amostragem por datas (avançado)", expanded=False):
    dias_retro = st.number_input("Dias retroativos (proposta)", min_value=0, max_value=120, value=PROPOSTA_DIAS_RETROATIVOS_DEFAULT, step=7)
    passo_dias = st.number_input("Passo (dias) entre coletas", min_value=1, max_value=30, value=PROPOSTA_PASSO_DIAS_DEFAULT, step=1)
    st.caption("Ex.: 28 dias com passo 7 ⇒ coletas em: hoje, -7, -14, -21, -28.")

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
        "dias_retro": int(dias_retro),
        "passo_dias": int(passo_dias),
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
        f"- Modalidades: `{p.get('modalidades') or '—'}`\n"
        f"- Dias retro: `{p.get('dias_retro', PROPOSTA_DIAS_RETROATIVOS_DEFAULT)}` • Passo: `{p.get('passo_dias', PROPOSTA_PASSO_DIAS_DEFAULT)}`"
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
            # 🔁 Batching por datas (UF-only), com deduplicação
            regs_bruto = consultar_proposta_uf_batched(
                uf=uf_escolhida,
                dias_retro=int(dias_retro),
                passo_dias=int(passo_dias),
            )
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

        # 📎 Filtragem client-side: status/palavra → municípios
        regs_status_palavra = filtrar_por_status_palavra(regs_bruto, palavra_chave, status_label)
        regs_filtrados = filtrar_por_municipios(regs_status_palavra, set(municipios_selecionados))

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

        st.info(
            f"Amostragem expandida (proposta): dias retro **{int(dias_retro)}**, "
            f"passo **{int(passo_dias)}** • Itens recebidos (somas dos lotes UF): **{total_baixado_uf}** • "
            f"Após deduplicação e filtros: **{len(df)}**."
        )

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
        "- **Recebendo Proposta**: coletas por UF em **vários cortes de `dataFinal`** (ex.: hoje, -7, -14, -21, -28), "
        "agregando e **deduplicando** para contornar paginação truncada do PNCP.\n"
        "- **Demais status**: `/publicacao` por UF com **dataInicial/dataFinal (yyyyMMdd)** e modalidades informadas.\n"
        "- Respostas vazias (204/200 sem corpo) são tratadas como **0 resultados**; há retry + backoff."
    )
