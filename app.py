# app.py — 📑 PNCP (versão aprovada) com COLETOR "full" por UF
# - Consulta por UF no /proposta com sharding temporal + por modalidade (anti-truncagem)
# - Filtra client-side por NOME de município (sem IBGE/códigos)
# - Mantém /publicacao para demais status (com UF + modalidades)
#
# Execução: streamlit run app.py
# Requisitos: streamlit, requests, pandas, xlsxwriter (ou openpyxl)

from __future__ import annotations
import io
import time
import json
from datetime import date, timedelta
from typing import Dict, List, Optional, Iterable, Tuple, Set

import pandas as pd
import requests
import streamlit as st

# =========================
# Config & Constantes
# =========================
st.set_page_config(page_title="📑 PNCP — Coleta por UF (full) + filtro por municípios (nome)", page_icon="📑", layout="wide")

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"       # Recebendo Proposta (dataFinal=yyyyMMdd; CONSULTA POR UF)
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"   # Publicações (codigoModalidadeContratacao + datas=yyyyMMdd; CONSULTA POR UF)

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]

# Paginação/robustez
PAGE_SIZE = 50
ALT_PAGE_SIZE = 20
MAX_BLANK_PAGES = 2
RETRY_PER_PAGE = 3
SLEEP_BETWEEN_PAGES = 0.03

# Janelas
PROPOSTA_DIAS_RETRO_DEFAULT = 35   # cobre ~5 semanas
PROPOSTA_PASSO_DIAS_DEFAULT = 5
PUBLICACAO_JANELA_DIAS = 60

# =========================
# Utils
# =========================
def _normalize(s: Optional[str]) -> str:
    return (s or "").strip()

def _casekey(s: Optional[str]) -> str:
    return _normalize(s).casefold()

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def _as_str(x) -> str:
    return "" if x is None else str(x)

def _xlsx_bytes(df: pd.DataFrame, sheet_name: str = "resultados") -> bytes:
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            df.to_excel(w, sheet_name=sheet_name, index=False)
        return buffer.getvalue()

def _classificar_status(nome: Optional[str]) -> str:
    s = _normalize(nome).lower()
    if "receb" in s: return "Recebendo Proposta"
    if "julg" in s or "propostas encerradas" in s: return "Propostas Encerradas"
    if "encerrad" in s: return "Encerradas"
    return "Todos"

def _uniq_key(d: dict) -> Tuple:
    """Deduplicação robusta."""
    ncp = d.get("numeroControlePNCP")
    if ncp: return ("NCP", str(ncp))
    ano = d.get("anoCompra"); seq = d.get("sequencialCompra")
    org = (d.get("orgaoEntidade") or {}).get("cnpj")
    return ("LEGACY", str(ano), str(seq), str(org))

# =========================
# HTTP + parser tolerante
# =========================
SESSION: Optional[requests.Session] = None

def _session() -> requests.Session:
    global SESSION
    if SESSION is None:
        s = requests.Session()
        s.headers.update({
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "PNCP-FullCollector/1.0 (Streamlit)",
            "Connection": "keep-alive",
        })
        SESSION = s
    return SESSION

def _safe_json(r: requests.Response) -> dict:
    if r.status_code == 204:
        return {"data": []}
    text = (r.text or "").strip()
    if text == "":
        return {"data": []}
    ctype = (r.headers.get("Content-Type") or "").lower()
    looks_json = ("json" in ctype) or text.startswith(("{", "["))
    if not looks_json:
        snippet = text[:600]
        raise ValueError(f"Resposta não-JSON do PNCP (Content-Type='{ctype}'). Trecho: {snippet or '<vazio>'}")
    try:
        return r.json()
    except json.JSONDecodeError as e:
        snippet = text[:600]
        raise ValueError(f"Falha ao decodificar JSON. Trecho: {snippet}") from e

# =========================
# Paginação — scroll contínuo
# =========================
def _paginacao(endpoint: str, params_base: Dict[str, str], page_size: int = PAGE_SIZE) -> Iterable[list]:
    s = _session()
    pagina, blanks = 1, 0
    while True:
        params = {**{k: _as_str(v) for k, v in params_base.items()},
                  "pagina": str(pagina), "tamanhoPagina": str(page_size)}
        last_err = None
        for attempt in range(1, RETRY_PER_PAGE + 1):
            try:
                r = s.get(endpoint, params=params, timeout=60)
                r.raise_for_status()
                payload = _safe_json(r)
                dados = payload.get("data") or []
                if not dados:
                    blanks += 1
                    if blanks >= MAX_BLANK_PAGES:
                        return
                    break
                blanks = 0
                yield dados
                break
            except Exception as e:
                last_err = e
                time.sleep(0.3 * attempt)
        if last_err is not None:
            if page_size != ALT_PAGE_SIZE:
                yield from _paginacao(endpoint, params_base, page_size=ALT_PAGE_SIZE)
                return
            else:
                raise last_err
        pagina += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

# =========================
# Coletor "full" — UF no /proposta
#   - Descobre modalidades presentes (snapshot)
#   - Sharda por modalidade e por cortes de dataFinal
#   - Une e dedup
# =========================
def _listar_modalidades_snapshot(uf: str, max_pages: int = 3) -> List[str]:
    s = _session()
    moda = set()
    for pagina in range(1, max_pages + 1):
        params = {"uf": uf, "dataFinal": _yyyymmdd(date.today()),
                  "pagina": str(pagina), "tamanhoPagina": "50"}
        r = s.get(ENDP_PROPOSTA, params=params, timeout=60)
        if r.status_code >= 400:
            break
        payload = _safe_json(r)
        dados = payload.get("data") or []
        if not dados:
            break
        for d in dados:
            mid = d.get("modalidadeId")
            if mid is not None:
                moda.add(str(mid))
    return sorted(moda)

def coletar_uf_full(uf: str, dias_retro: int, passo_dias: int, modalidades: Optional[List[str]] = None) -> List[dict]:
    # 1) snapshot de modalidades (se não vierem explicitadas)
    if not modalidades:
        modalidades = _listar_modalidades_snapshot(uf) or [None]

    # 2) cortes de data
    cortes = [date.today() - timedelta(days=d) for d in range(0, max(1, dias_retro) + 1, max(1, passo_dias))]

    registros: Dict[Tuple, dict] = {}
    total_fetch = 0
    barra = st.progress(0.0)
    total_passos = len(modalidades) * len(cortes)
    passo = 0

    for mod in modalidades:
        for corte in cortes:
            params = {"uf": uf, "dataFinal": _yyyymmdd(corte)}
            if mod is not None:
                params["codigoModalidadeContratacao"] = mod
            for lote in _paginacao(ENDP_PROPOSTA, params, page_size=PAGE_SIZE):
                total_fetch += len(lote)
                for d in lote:
                    registros[_uniq_key(d)] = d
            passo += 1
            barra.progress(min(1.0, passo / float(max(1, total_passos))))
            time.sleep(0.02)

    st.session_state["_telemetria_full_total_fetch"] = total_fetch
    st.session_state["_telemetria_full_dedup"] = len(registros)
    return list(registros.values())

# =========================
# /publicacao — UF (para demais status)
# =========================
def consultar_publicacao_por_uf_modalidades(uf: str, codigos_modalidade: List[str],
                                            dias_janela: int = PUBLICACAO_JANELA_DIAS) -> List[dict]:
    if not codigos_modalidade:
        return []
    hoje = date.today()
    base = {"uf": uf, "dataInicial": _yyyymmdd(hoje - timedelta(days=dias_janela)),
            "dataFinal": _yyyymmdd(hoje)}
    acumulado = []
    barra = st.progress(0.0)
    for i, cod in enumerate(codigos_modalidade, start=1):
        params = dict(base)
        params["codigoModalidadeContratacao"] = _as_str(cod)
        for dados in _paginacao(ENDP_PUBLICACAO, params, page_size=PAGE_SIZE):
            acumulado.extend(dados)
        barra.progress(min(1.0, i / float(len(codigos_modalidade))))
        time.sleep(0.02)
    return acumulado

# =========================
# Filtragem client-side
# =========================
def filtrar_status_palavra(dados: list, palavra: str, status_label: str) -> list:
    out = []
    for d in dados:
        if status_label != "Todos" and _classificar_status(d.get("situacaoCompraNome")) != status_label:
            continue
        if palavra:
            p = palavra.strip().lower()
            uo = d.get("unidadeOrgao") or {}
            texto = " ".join([
                _normalize(d.get("objetoCompra")),
                _normalize(d.get("informacaoComplementar")),
                _normalize(uo.get("nomeUnidade")),
            ]).lower()
            if p not in texto:
                continue
        out.append(d)
    return out

def filtrar_por_municipios_nome(dados: list, nomes_municipios: Set[str]) -> list:
    if not nomes_municipios:
        return dados
    keys = set(_casekey(n) for n in nomes_municipios)
    out = []
    for d in dados:
        uo = d.get("unidadeOrgao") or {}
        if _casekey(uo.get("municipioNome")) in keys:
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
# Sidebar
# =========================
st.sidebar.header("Filtros")

palavra_chave = st.sidebar.text_input("Palavra chave", value="")

uf_escolhida = st.sidebar.selectbox("Estado (UF) — obrigatório", options=UFS, index=UFS.index("SP"))

# Município por NOME (sem IBGE/código) — livre, um ou vários
mun_input = st.sidebar.text_area(
    "Municipios (por nome; separados por vírgula ou quebra de linha)",
    value="",
    height=90,
    placeholder="Ex.: Porto Feliz\nItapetininga\nSorocaba"
)
municipios_selecionados = [m.strip() for chunk in mun_input.split("\n") for m in chunk.split(",")]
municipios_selecionados = [m for m in municipios_selecionados if m]

status_label = st.sidebar.selectbox("Status", options=STATUS_LABELS, index=0)

with st.sidebar.expander("Amostragem /proposta (anti-truncagem)", expanded=True):
    dias_retro = st.number_input("Dias retroativos", min_value=0, max_value=120,
                                 value=PROPOSTA_DIAS_RETRO_DEFAULT, step=5,
                                 help="Cortes de dataFinal: hoje, -passo, -2*passo, ... até 'dias retro'.")
    passo_dias = st.number_input("Passo (dias)", min_value=1, max_value=30,
                                 value=PROPOSTA_PASSO_DIAS_DEFAULT, step=1)

modalidades_str = ""
if status_label != "Recebendo Proposta":
    modalidades_str = st.sidebar.text_input(
        "Modalidades (códigos PNCP, ',') — exigido para /publicacao",
        value="",
        placeholder="Ex.: 5, 6, 23"
    )

st.sidebar.markdown("---")

# Salvar/recuperar pesquisa
if "pesquisas_salvas" not in st.session_state:
    st.session_state["pesquisas_salvas"] = {}
nome_pesquisa = st.sidebar.text_input("Salvar pesquisa", value="", placeholder="Ex.: SP — Recebendo — Porto Feliz")
if st.sidebar.button("Salvar pesquisa"):
    st.session_state["pesquisas_salvas"][nome_pesquisa.strip() or f"Pesquisa {len(st.session_state['pesquisas_salvas'])+1}"] = {
        "palavra_chave": palavra_chave,
        "uf": uf_escolhida,
        "municipios": municipios_selecionados,
        "status": status_label,
        "dias_retro": int(dias_retro),
        "passo_dias": int(passo_dias),
        "modalidades": modalidades_str,
    }
    st.sidebar.success("Pesquisa salva.")

salvos = st.session_state.get("pesquisas_salvas", {})
escolha_salva = st.sidebar.selectbox("Pesquisas salvas", options=["—"] + list(salvos.keys()), index=0)
if escolha_salva != "—":
    p = salvos[escolha_salva]
    st.sidebar.info(
        f"**{escolha_salva}**\n\n"
        f"- Palavra: `{p.get('palavra_chave','')}`\n"
        f"- UF: `{p.get('uf')}`\n"
        f"- Municípios: `{', '.join(p.get('municipios', [])) or '—'}`\n"
        f"- Status: `{p.get('status')}`\n"
        f"- Dias retro: `{p.get('dias_retro', PROPOSTA_DIAS_RETRO_DEFAULT)}` • Passo: `{p.get('passo_dias', PROPOSTA_PASSO_DIAS_DEFAULT)}`\n"
        f"- Modalidades: `{p.get('modalidades') or '—'}`"
    )

st.sidebar.markdown("---")
executar = st.sidebar.button("Executar Pesquisa")

# =========================
# Corpo
# =========================
st.title("📑 PNCP — Coleta por UF (full) + filtro por municípios (nome)")

if executar:
    if not uf_escolhida:
        st.error("Operação cancelada: o campo **Estado (UF)** é obrigatório.")
        st.stop()

    try:
        if status_label == "Recebendo Proposta":
            # Coletor "full" para /proposta (UF) com anti-truncagem
            regs_uf = coletar_uf_full(
                uf=uf_escolhida,
                dias_retro=int(dias_retro),
                passo_dias=int(passo_dias),
                modalidades=None  # descobre via snapshot
            )
            total_fetch = st.session_state.get("_telemetria_full_total_fetch", 0)
            total_dedup = st.session_state.get("_telemetria_full_dedup", len(regs_uf))
            # Filtro client-side
            regs = filtrar_por_municipios_nome(regs_uf, set(municipios_selecionados))
            regs = filtrar_status_palavra(regs, palavra_chave, status_label)

            df = normalizar_df(regs)

            hoje_txt = _yyyymmdd(date.today())
            st.caption(
                f"UF **{uf_escolhida}** • Municípios **{len(municipios_selecionados)}** • Status **{status_label}** • Execução **{hoje_txt}**"
            )
            st.info(
                f"/proposta (full): itens recebidos (somatório de shards) **{total_fetch}** • "
                f"após deduplicação **{total_dedup}** • após filtros **{len(df)}**."
            )

        else:
            # /publicacao por UF + modalidades (obrigatório)
            cod_modalidades = [x.strip() for x in modalidades_str.split(",") if x.strip()]
            if not cod_modalidades:
                st.warning(
                    "Para **Propostas Encerradas / Encerradas / Todos**, informe **códigos de modalidade** na sidebar. "
                    "O endpoint /publicacao exige esse parâmetro."
                )
                df = pd.DataFrame()
            else:
                regs_uf = consultar_publicacao_por_uf_modalidades(
                    uf=uf_escolhida,
                    codigos_modalidade=cod_modalidades,
                    dias_janela=PUBLICACAO_JANELA_DIAS,
                )
                # Filtros client-side
                regs = filtrar_por_municipios_nome(regs_uf, set(municipios_selecionados))
                regs = filtrar_status_palavra(regs, palavra_chave, status_label)
                df = normalizar_df(regs)
                hoje_txt = _yyyymmdd(date.today())
                st.caption(
                    f"UF **{uf_escolhida}** • Municípios **{len(municipios_selecionados)}** • Status **{status_label}** • Execução **{hoje_txt}**"
                )
                st.info(f"/publicacao (UF): itens recebidos **{len(regs_uf)}** • após filtros **{len(df)}**.")

        # Auditoria de municípios sem retorno (após todos os filtros)
        if municipios_selecionados:
            presentes = set(_casekey((r.get("unidadeOrgao") or {}).get("municipioNome")) for r in (regs if 'regs' in locals() else []))
            faltantes = [m for m in municipios_selecionados if _casekey(m) not in presentes]
            if faltantes:
                st.warning(f"Sem resultados após filtros para: {', '.join(faltantes)}")

        # Render
        if df is None or df.empty:
            st.warning("Nenhum resultado para os filtros aplicados.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            xlsx = _xlsx_bytes(df, sheet_name="editais")
            st.download_button(
                label="⬇️ Baixar XLSX",
                data=xlsx,
                file_name=f"editais_{uf_escolhida}_{status_label}_{_yyyymmdd(date.today())}.xlsx",
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
        "Fluxo aprovado:\n"
        "1) Coleta **por UF** no endpoint **/proposta** com anti-truncagem "
        "(cortes de `dataFinal` + sharding por **modalidades** + deduplicação);\n"
        "2) **Filtragem client-side** pelos **nomes** dos municípios informados;\n"
        "3) Para outros status, usa-se **/publicacao** (UF + modalidades) e aplica-se o mesmo filtro local."
    )
