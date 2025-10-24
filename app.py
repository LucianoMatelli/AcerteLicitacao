# app.py — 📑 PNCP (versão aprovada) com COLETOR full e fallback 422
# -----------------------------------------------------------------
# Consulta principal: /v1/contratacoes/proposta (por UF)
# Estratégia anti-truncagem: sharding temporal + por modalidade + fallback sem modalidade
# Filtragem local: por nome de município, palavra-chave e status
# -----------------------------------------------------------------

from __future__ import annotations
import io
import time
import json
from datetime import date, timedelta
from typing import Dict, List, Optional, Iterable, Tuple, Set

import pandas as pd
import requests
import streamlit as st

# ================================================================
# CONFIGURAÇÃO GERAL
# ================================================================
st.set_page_config(
    page_title="📑 PNCP — Coleta Full (UF + fallback 422)",
    page_icon="📑",
    layout="wide"
)

BASE = "https://pncp.gov.br/api/consulta"
ENDP_PROPOSTA = f"{BASE}/v1/contratacoes/proposta"
ENDP_PUBLICACAO = f"{BASE}/v1/contratacoes/publicacao"

UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

STATUS_LABELS = ["Recebendo Proposta", "Propostas Encerradas", "Encerradas", "Todos"]

PAGE_SIZE = 50
ALT_PAGE_SIZE = 20
MAX_BLANK_PAGES = 2
RETRY_PER_PAGE = 3
SLEEP_BETWEEN_PAGES = 0.03

PROPOSTA_DIAS_RETRO_DEFAULT = 35
PROPOSTA_PASSO_DIAS_DEFAULT = 5
PUBLICACAO_JANELA_DIAS = 60


# ================================================================
# FUNÇÕES UTILITÁRIAS
# ================================================================
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
    ncp = d.get("numeroControlePNCP")
    if ncp: return ("NCP", str(ncp))
    ano = d.get("anoCompra"); seq = d.get("sequencialCompra")
    org = (d.get("orgaoEntidade") or {}).get("cnpj")
    return ("LEGACY", str(ano), str(seq), str(org))


# ================================================================
# SESSÃO HTTP E JSON TOLERANTE
# ================================================================
SESSION: Optional[requests.Session] = None

def _session() -> requests.Session:
    global SESSION
    if SESSION is None:
        s = requests.Session()
        s.headers.update({
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "PNCP-FullCollector/1.1",
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
        raise ValueError(f"Resposta não-JSON do PNCP: {snippet or '<vazio>'}")
    return r.json()


# ================================================================
# PAGINAÇÃO CONTÍNUA (scroll)
# ================================================================
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
                    if blanks >= MAX_BLANK_PAGES: return
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


# ================================================================
# SNAPSHOT DE MODALIDADES
# ================================================================
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
        if not dados: break
        for d in dados:
            mid = d.get("modalidadeId")
            if mid is not None:
                moda.add(str(mid))
    return sorted(moda)


# ================================================================
# COLETOR FULL (com fallback 422)
# ================================================================
def coletar_uf_full(uf: str, dias_retro: int, passo_dias: int, modalidades: Optional[List[str]] = None) -> List[dict]:
    if not modalidades:
        modalidades = _listar_modalidades_snapshot(uf) or [None]

    cortes = [date.today() - timedelta(days=d) for d in range(0, dias_retro + 1, max(1, passo_dias))]
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

            try:
                for lote in _paginacao(ENDP_PROPOSTA, params, page_size=PAGE_SIZE):
                    total_fetch += len(lote)
                    for d in lote:
                        registros[_uniq_key(d)] = d

            except requests.HTTPError as e:
                # fallback sem modalidade
                status = getattr(e.response, "status_code", None)
                if status in (400, 422) and mod is not None:
                    params_sem = {"uf": uf, "dataFinal": _yyyymmdd(corte)}
                    for lote in _paginacao(ENDP_PROPOSTA, params_sem, page_size=PAGE_SIZE):
                        total_fetch += len(lote)
                        for d in lote:
                            registros[_uniq_key(d)] = d
                else:
                    st.warning(f"Erro em shard {uf}/{corte}/{mod}: {e}")

            except Exception as e:
                st.warning(f"Shard {uf}/{corte}/{mod} falhou: {e}")

            passo += 1
            barra.progress(min(1.0, passo / float(max(1, total_passos))))
            time.sleep(0.02)

    st.session_state["_telemetria_full_total_fetch"] = total_fetch
    st.session_state["_telemetria_full_dedup"] = len(registros)
    return list(registros.values())


# ================================================================
# FILTRAGEM LOCAL E DATAFRAME
# ================================================================
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
    if not nomes_municipios: return dados
    keys = set(_casekey(n) for n in nomes_municipios)
    return [d for d in dados if _casekey((d.get("unidadeOrgao") or {}).get("municipioNome")) in keys]

def normalizar_df(regs: List[dict]) -> pd.DataFrame:
    linhas = []
    for d in regs:
        uo = d.get("unidadeOrgao") or {}
        linhas.append({
            "Status": _classificar_status(d.get("situacaoCompraNome")),
            "Situação PNCP": d.get("situacaoCompraNome"),
            "UF": uo.get("ufSigla"),
            "Município": uo.get("municipioNome"),
            "Órgão": uo.get("nomeUnidade"),
            "Modalidade": d.get("modalidadeNome"),
            "Nº Compra": d.get("numeroCompra"),
            "Objeto": d.get("objetoCompra"),
            "Publicação": d.get("dataPublicacaoPncp"),
            "Abertura": d.get("dataAberturaProposta"),
            "Encerramento": d.get("dataEncerramentoProposta"),
            "Controle PNCP": d.get("numeroControlePNCP"),
        })
    df = pd.DataFrame(linhas)
    if not df.empty:
        df = df.sort_values(by=["Publicação", "Município"], ascending=[False, True])
    return df


# ================================================================
# SIDEBAR E EXECUÇÃO
# ================================================================
st.sidebar.header("Filtros")

palavra = st.sidebar.text_input("Palavra-chave")
uf = st.sidebar.selectbox("UF (obrigatória)", options=UFS, index=UFS.index("SP"))
mun_input = st.sidebar.text_area("Municípios (nomes separados por vírgula ou quebra de linha)",
                                 placeholder="Ex.: Porto Feliz\nItapetininga")
municipios = [m.strip() for part in mun_input.split("\n") for m in part.split(",") if m.strip()]
status = st.sidebar.selectbox("Status", STATUS_LABELS, index=0)
dias_retro = st.sidebar.number_input("Dias retroativos", 0, 120, PROPOSTA_DIAS_RETRO_DEFAULT, 5)
passo_dias = st.sidebar.number_input("Passo (dias)", 1, 30, PROPOSTA_PASSO_DIAS_DEFAULT, 1)
executar = st.sidebar.button("Executar pesquisa")

st.title("📑 PNCP — Consulta Full (UF) + fallback 422")

if executar:
    try:
        regs_uf = coletar_uf_full(uf, int(dias_retro), int(passo_dias))
        regs = filtrar_por_municipios_nome(regs_uf, set(municipios))
        regs = filtrar_status_palavra(regs, palavra, status)
        df = normalizar_df(regs)

        total_fetch = st.session_state.get("_telemetria_full_total_fetch", 0)
        total_dedup = st.session_state.get("_telemetria_full_dedup", len(regs_uf))
        st.info(f"Coleta total: {total_fetch} itens • Dedup: {total_dedup} • Após filtros: {len(df)}")

        if df.empty:
            st.warning("Nenhum resultado para os filtros.")
        else:
            st.dataframe(df, use_container_width=True)
            xlsx = _xlsx_bytes(df)
            st.download_button("⬇️ Baixar XLSX", data=xlsx,
                               file_name=f"pncp_{uf}_{_yyyymmdd(date.today())}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        st.error(f"Erro: {e}")
else:
    st.caption("Use a sidebar para definir os filtros e clique em **Executar pesquisa**.")
