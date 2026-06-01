# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import html
import io
import json
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st


# ==========================
# Configuracao de pagina
# ==========================
st.set_page_config(
    page_title="Acerte Licitacoes - O seu Buscador de Editais",
    page_icon="📑",
    layout="wide",
)


# ==========================
# Constantes
# ==========================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PNCP_PATHS = [
    os.path.join(DATA_DIR, "ListaMunicipiosPNCP.csv"),
    "ListaMunicipiosPNCP.csv",
]
CSV_IBGE_PATHS = [
    os.path.join(DATA_DIR, "IBGE_Municipios.csv"),
    "IBGE_Municipios.csv",
]

ORIGIN = "https://pncp.gov.br"
API_CONSULTA_BASE = ORIGIN + "/api/consulta/v1"
API_CONSULTA_PROPOSTA = API_CONSULTA_BASE + "/contratacoes/proposta"
API_CONSULTA_PUBLICACAO = API_CONSULTA_BASE + "/contratacoes/publicacao"
API_IBGE_MUNICIPIOS_UF = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"

HEADERS = {
    "User-Agent": "AcerteLicitacoes/2.0 (+streamlit)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://pncp.gov.br/api/consulta/swagger-ui/index.html",
}

STATUS_LABELS = [
    "A Receber/Recebendo Proposta",
    "Em Julgamento/Propostas Encerradas",
    "Encerradas",
    "Todos",
]
STATUS_MAP = {
    "A Receber/Recebendo Proposta": "recebendo_proposta",
    "Em Julgamento/Propostas Encerradas": "em_julgamento",
    "Encerradas": "encerrado",
    "Todos": "",
}

UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]
UF_PLACEHOLDER = "— Selecione a UF —"

MODALIDADES_CONSULTA = list(range(1, 14))
MAX_MUNICIPIOS = 25

SAVED_SEARCHES_PATH = os.path.join(BASE_DIR, "saved_searches.json")
SAVED_TR_PATH = os.path.join(BASE_DIR, "tr_marks.json")
SAVED_NA_PATH = os.path.join(BASE_DIR, "na_marks.json")
SAVED_SEARCHES_PATHS = [
    os.path.join(DATA_DIR, "saved_searches.json"),
    SAVED_SEARCHES_PATH,
]
SAVED_TR_PATHS = [
    os.path.join(DATA_DIR, "tr_marks.json"),
    SAVED_TR_PATH,
]
SAVED_NA_PATHS = [
    os.path.join(DATA_DIR, "na_marks.json"),
    SAVED_NA_PATH,
]

DEFAULT_GITHUB_REPO = "LucianoMatelli/acerte.debug"
DEFAULT_GITHUB_BRANCH = "main"
DEFAULT_GITHUB_BASEDIR = "data"
PERSISTENCE_VERSION = "shared-data-v2"


# ==========================
# Utilitarios
# ==========================
def _norm(s: str) -> str:
    s = str(s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _safe_text(v) -> str:
    return str(v or "").strip()


def _secret(name: str, default: str = "") -> str:
    try:
        value = st.secrets.get(name, default)
    except Exception:
        value = os.getenv(name, default)
    return _safe_text(value)


def _secret_int(name: str, default: int, min_value: int = 1, max_value: Optional[int] = None) -> int:
    try:
        value = int(_secret(name, str(default)))
    except Exception:
        value = default
    value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


PAGE_SIZE_API = _secret_int("BACKUP_API_TAMANHO_PAGINA", 50, 10, 50)
MAX_PAGES_API = _secret_int("BACKUP_API_MAX_PAGINAS", 30, 1, 200)
TIMEOUT_API = _secret_int("BACKUP_API_TIMEOUT", 30, 5, 120)
PROPOSTA_DIAS_A_FRENTE = _secret_int("BACKUP_PROPOSTA_DIAS_A_FRENTE", 45, 1, 365)
PUBLICACAO_DIAS_LOOKBACK = _secret_int("BACKUP_PUBLICACAO_DIAS_LOOKBACK", 365, 1, 365)
API_RETRIES = _secret_int("BACKUP_API_RETRIES", 3, 1, 5)
API_DELAY_MS = _secret_int("BACKUP_API_DELAY_MS", 700, 0, 10000)


class PncpRequestRejected(RuntimeError):
    pass


def _is_request_rejected_error(exc: Exception | str) -> bool:
    text = str(exc).lower()
    return "request_rejected" in text or "rejeitou temporariamente" in text


def _fmt_dt_iso_to_br(value: str) -> str:
    if not value:
        return ""
    try:
        ts = pd.to_datetime(value, errors="coerce", utc=False)
        if pd.isna(ts):
            return ""
        return ts.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


def _parse_numero_controle(numero_controle: str) -> Tuple[str, str, str]:
    match = re.search(r"^(\d{14})-1-(\d+)/(\d{4})$", _safe_text(numero_controle))
    if not match:
        return "", "", ""
    cnpj = match.group(1)
    seq = match.group(2)
    ano = match.group(3)
    return cnpj, ano, seq


def _build_pncp_link(cnpj: str, ano: str, seq: str) -> str:
    cnpj = _safe_text(cnpj)
    ano = _safe_text(ano)
    seq = _safe_text(seq)
    if len(cnpj) == 14 and ano.isdigit() and seq:
        return f"{ORIGIN}/app/editais/{cnpj}/{ano}/{seq}"
    return ""


def _first_dict(*values) -> Dict:
    for value in values:
        if isinstance(value, dict):
            return value
    return {}


def _uid_from_row(row: Dict) -> str:
    candidates = _uid_candidates_from_row(row)
    return candidates[0] if candidates else ""


def _uid_candidates_from_row(row: Dict) -> List[str]:
    candidates: List[str] = []

    def _add(value: str) -> None:
        value = _safe_text(value)
        if value and value not in candidates:
            candidates.append(value)

    cnpj = _safe_text(row.get("_orgao_cnpj"))
    ano = _safe_text(row.get("_ano"))
    seq = _safe_text(row.get("_seq"))
    if len(cnpj) == 14 and ano.isdigit() and seq:
        _add(f"{cnpj}-{ano}-{seq}")
        try:
            seq_int = str(int(seq))
            _add(f"{cnpj}-{ano}-{seq_int}")
            _add(f"{cnpj}-{ano}-{seq_int.zfill(6)}")
        except Exception:
            pass

    ctrl_cnpj, ctrl_ano, ctrl_seq = _parse_numero_controle(_safe_text(row.get("_id")))
    if len(ctrl_cnpj) == 14 and ctrl_ano.isdigit() and ctrl_seq:
        _add(f"{ctrl_cnpj}-{ctrl_ano}-{ctrl_seq}")
        try:
            ctrl_seq_int = str(int(ctrl_seq))
            _add(f"{ctrl_cnpj}-{ctrl_ano}-{ctrl_seq_int}")
            _add(f"{ctrl_cnpj}-{ctrl_ano}-{ctrl_seq_int.zfill(6)}")
        except Exception:
            pass

    link = _safe_text(row.get("Link para o edital"))
    match = re.search(r"/app/editais/(\d{14})/(\d{4})/(\w+)", link)
    if match:
        _add(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")

    base = "|".join(
        [
            _safe_text(row.get("Título") or row.get("Titulo")),
            _safe_text(row.get("Cidade")),
            _safe_text(row.get("UF")),
            _safe_text(row.get("_pub_raw")),
            _safe_text(row.get("Orgão") or row.get("Orgao")),
        ]
    )
    _add(hashlib.md5(base.encode("utf-8")).hexdigest())
    return candidates


def _escape(value) -> str:
    return html.escape(_safe_text(value), quote=True)


# ==========================
# GitHub/local para salvos
# ==========================
def _infer_repo_from_git_config() -> str:
    try:
        git_cfg = os.path.join(BASE_DIR, ".git", "config")
        if not os.path.exists(git_cfg):
            return ""
        with open(git_cfg, "r", encoding="utf-8") as f:
            txt = f.read()
        match = re.search(r"url\s*=\s*(.+github\.com[:/][^/\s]+/[^/\s]+)(?:\.git)?", txt, flags=re.IGNORECASE)
        if not match:
            return ""
        url = match.group(1).strip().replace("\\", "/")
        repo_match = re.search(r"github\.com[:/]([^/\s]+/[^/\s]+?)(?:\.git)?$", url, flags=re.IGNORECASE)
        return repo_match.group(1) if repo_match else ""
    except Exception:
        return ""


def _github_repo() -> str:
    return (
        _secret("GITHUB_REPO_TEST")
        or _secret("GITHUB_REPO")
        or _secret("GITHUB_REPOSITORY")
        or _infer_repo_from_git_config()
        or DEFAULT_GITHUB_REPO
    )


def _github_branch() -> str:
    return _secret("GITHUB_BRANCH_TEST") or _secret("GITHUB_BRANCH") or DEFAULT_GITHUB_BRANCH


def _github_basedir() -> str:
    basedir_test = _secret("GITHUB_BASEDIR_TEST")
    if basedir_test:
        return basedir_test
    return _secret("GITHUB_BASEDIR", DEFAULT_GITHUB_BASEDIR) or DEFAULT_GITHUB_BASEDIR


def _github_token() -> str:
    return _secret("GITHUB_TOKEN")


def _github_path(filename: str) -> Tuple[str, str, str]:
    repo = _github_repo()
    branch = _github_branch()
    basedir = _github_basedir().strip("/")
    path = f"{basedir}/{filename}" if basedir else filename
    return repo, branch, path


def _gh_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "AcerteLicitacoes",
    }
    token = _github_token()
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def _gh_get_json(filename: str) -> Tuple[Optional[dict], Optional[str]]:
    repo, branch, path = _github_path(filename)
    if not repo:
        return None, None

    if _github_token():
        try:
            url = f"https://api.github.com/repos/{repo}/contents/{path}"
            r = requests.get(url, params={"ref": branch}, headers=_gh_headers(), timeout=20)
            if r.status_code == 404:
                return None, None
            if 200 <= r.status_code < 300:
                js = r.json()
                raw = base64.b64decode(js.get("content", "")).decode("utf-8")
                return json.loads(raw), js.get("sha")
        except Exception:
            pass

    try:
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
        r = requests.get(raw_url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            js = r.json()
            if isinstance(js, dict):
                return js, None
    except Exception:
        pass

    return None, None


def _gh_put_json(filename: str, payload: dict, sha: Optional[str]) -> None:
    token = _github_token()
    repo, branch, path = _github_path(filename)
    if not token or not repo:
        raise RuntimeError("GITHUB_TOKEN ou GITHUB_REPO ausente")

    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    content_b64 = base64.b64encode(
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("utf-8")
    body = {
        "message": f"chore: atualizar {path} via app",
        "content": content_b64,
        "branch": branch,
        "committer": {
            "name": _secret("GITHUB_COMMITTER_NAME", "PNCP Bot"),
            "email": _secret("GITHUB_COMMITTER_EMAIL", "bot@acertelicitacoes.local"),
        },
    }
    if sha:
        body["sha"] = sha

    r = requests.put(url, headers=_gh_headers(), json=body, timeout=30)
    r.raise_for_status()


def _read_json_from_paths(paths: List[str]) -> Optional[dict]:
    for path in paths:
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            continue
    return None


def _write_json_to_paths(paths: List[str], payload: dict) -> None:
    target = ""
    for path in paths:
        try:
            if os.path.exists(path):
                target = path
                break
        except Exception:
            continue
    if not target:
        target = paths[0]
    parent = os.path.dirname(target)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    with open(target, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _load_json(filename: str, local_paths: List[str]) -> Dict:
    remote, _ = _gh_get_json(filename)
    if isinstance(remote, dict):
        return remote
    local = _read_json_from_paths(local_paths)
    return local if isinstance(local, dict) else {}


def _persist_json(filename: str, local_paths: List[str], payload: Dict) -> None:
    try:
        _, sha = _gh_get_json(filename)
        _gh_put_json(filename, payload, sha)
        return
    except Exception as exc:
        st.warning(f"Nao consegui salvar no GitHub; usando arquivo local. Detalhe: {exc}")

    try:
        _write_json_to_paths(local_paths, payload)
    except Exception as exc:
        st.error(f"Falha ao salvar localmente: {exc}")


def _load_saved_searches() -> Dict[str, Dict]:
    return _load_json("saved_searches.json", SAVED_SEARCHES_PATHS)


def _persist_saved_searches(payload: Dict[str, Dict]) -> None:
    _persist_json("saved_searches.json", SAVED_SEARCHES_PATHS, payload)


def _load_marks(filename: str, local_paths: List[str]) -> Dict[str, bool]:
    data = _load_json(filename, local_paths)
    return {str(k): bool(v) for k, v in data.items()}


def _persist_marks(filename: str, local_paths: List[str], payload: Dict[str, bool]) -> None:
    _persist_json(filename, local_paths, payload)


# ==========================
# IBGE online
# ==========================
@st.cache_data(ttl=86400, show_spinner=False)
def load_municipios_ibge(uf: str) -> pd.DataFrame:
    uf = _safe_text(uf).upper()
    if uf not in UFS:
        return pd.DataFrame(columns=["nome", "uf", "codigo_ibge", "label", "nome_norm"])

    url = API_IBGE_MUNICIPIOS_UF.format(uf=uf)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    rows = r.json()

    out: List[Dict[str, str]] = []
    for item in rows if isinstance(rows, list) else []:
        nome = _safe_text(item.get("nome"))
        codigo = _safe_text(item.get("id"))
        if nome and codigo:
            out.append(
                {
                    "nome": nome,
                    "uf": uf,
                    "codigo_ibge": codigo,
                    "label": f"{nome} / {uf}",
                    "nome_norm": _norm(nome),
                }
            )

    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame(columns=["nome", "uf", "codigo_ibge", "label", "nome_norm"])
    df.sort_values("nome", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def resolver_municipio_ibge(nome: str, uf: str) -> Optional[Dict[str, str]]:
    df = load_municipios_ibge(uf)
    if df.empty:
        return None
    hit = df[df["nome_norm"] == _norm(nome)]
    if hit.empty:
        return None
    row = hit.iloc[0]
    return {
        "nome": _safe_text(row.get("nome")),
        "uf": _safe_text(row.get("uf")).upper(),
        "codigo_ibge": _safe_text(row.get("codigo_ibge")),
    }


@st.cache_data(show_spinner=False)
def load_municipios_pncp_catalog() -> pd.DataFrame:
    """Catalogo opcional usado so para compatibilizar pesquisas salvas com o app original."""
    empty = pd.DataFrame(columns=["nome", "uf", "codigo_pncp", "codigo_ibge", "nome_norm"])
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    seps = [",", ";", "\t", "|"]

    def _pick(cols_norm: Dict[str, str], names: List[str]) -> Optional[str]:
        for name in names:
            if name in cols_norm:
                return cols_norm[name]
        return None

    for path in CSV_PNCP_PATHS:
        if not os.path.exists(path):
            continue
        for enc in encodings:
            for sep in seps:
                try:
                    df = pd.read_csv(path, dtype=str, sep=sep, encoding=enc, engine="python", on_bad_lines="skip")
                    if df is None or df.empty:
                        continue
                    cols_norm = {_norm(c): c for c in df.columns}
                    col_nome = _pick(cols_norm, ["municipio", "municipio_nome", "nome", "cidade"])
                    col_codigo = _pick(cols_norm, ["codigo_pncp", "id", "codigo", "cod_pncp"])
                    col_uf = _pick(cols_norm, ["uf", "estado", "sigla_uf"])
                    col_ibge = _pick(cols_norm, ["codigo_ibge", "codigoibge", "cod_ibge", "codigo_municipio_ibge"])
                    if not col_nome or not col_codigo:
                        continue

                    out = pd.DataFrame(
                        {
                            "nome": df[col_nome].astype(str).str.strip(),
                            "uf": df[col_uf].astype(str).str.strip().str.upper() if col_uf else "",
                            "codigo_pncp": df[col_codigo].astype(str).str.strip(),
                            "codigo_ibge": df[col_ibge].astype(str).str.strip() if col_ibge else "",
                        }
                    )
                    out["nome_norm"] = out["nome"].map(_norm)
                    out = out[out["codigo_pncp"] != ""].drop_duplicates(subset=["uf", "nome_norm", "codigo_pncp"])
                    return out.reset_index(drop=True)
                except Exception:
                    continue
    return empty


def resolver_codigo_pncp(nome: str, uf: str, codigo_ibge: str = "") -> str:
    codigo_ibge = _safe_text(codigo_ibge)
    try:
        df = load_municipios_pncp_catalog()
    except Exception:
        df = pd.DataFrame()
    if df.empty:
        return codigo_ibge

    nome_norm = _norm(nome)
    uf = _safe_text(uf).upper()
    candidates = df.copy()

    if codigo_ibge and "codigo_ibge" in candidates.columns:
        by_ibge = candidates[candidates["codigo_ibge"] == codigo_ibge]
        if not by_ibge.empty:
            if "uf" in by_ibge.columns and uf:
                by_ibge_uf = by_ibge[by_ibge["uf"].str.upper() == uf]
                if not by_ibge_uf.empty:
                    return _safe_text(by_ibge_uf.iloc[0].get("codigo_pncp"))
            return _safe_text(by_ibge.iloc[0].get("codigo_pncp"))

    if "uf" in candidates.columns and uf:
        by_uf = candidates[candidates["uf"].str.upper() == uf]
        if not by_uf.empty:
            candidates = by_uf

    by_name = candidates[candidates["nome_norm"] == nome_norm]
    if not by_name.empty:
        return _safe_text(by_name.iloc[0].get("codigo_pncp"))

    return codigo_ibge


# ==========================
# API PNCP Consulta
# ==========================
def _items_from_api(js) -> List[Dict]:
    if isinstance(js, dict) and isinstance(js.get("data"), list):
        return js["data"]
    if isinstance(js, list):
        return js
    return []


def _get_api_page(url: str, params: Dict[str, object]) -> Tuple[List[Dict], int]:
    last_error: Optional[Exception] = None
    for attempt in range(API_RETRIES):
        try:
            if API_DELAY_MS > 0:
                time.sleep(API_DELAY_MS / 1000)
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT_API)
            if r.status_code >= 500 and attempt < API_RETRIES - 1:
                time.sleep(0.6 * (attempt + 1))
                continue

            if r.status_code in (204, 404):
                return [], 0
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {(r.text or '')[:180]}")

            body = (r.text or "").strip()
            if not body:
                return [], 0
            body_lower = body.lower()
            if "request rejected" in body_lower or "support id" in body_lower:
                raise PncpRequestRejected("request_rejected: PNCP rejeitou temporariamente a requisicao")

            try:
                js = r.json()
            except Exception as exc:
                last_error = exc
                if attempt < API_RETRIES - 1:
                    time.sleep(0.6 * (attempt + 1))
                    continue
                ctype = _safe_text(r.headers.get("content-type"))
                raise RuntimeError(
                    f"invalid_json HTTP {r.status_code} content-type {ctype}: {body[:180]}"
                ) from exc

            total_pages = 0
            if isinstance(js, dict):
                try:
                    total_pages = int(js.get("totalPaginas") or 0)
                except Exception:
                    total_pages = 0
            return _items_from_api(js), total_pages
        except PncpRequestRejected as exc:
            last_error = exc
            if attempt < API_RETRIES - 1:
                time.sleep(3.0 * (attempt + 1))
                continue
            raise
        except Exception as exc:
            last_error = exc
            if attempt < API_RETRIES - 1:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise RuntimeError(f"request_error: {exc}") from exc

    raise RuntimeError(f"request_error: {last_error}")


def _iter_pages(url: str, base_params: Dict[str, object]) -> List[Dict]:
    items: List[Dict] = []
    total_pages = 0
    for page in range(1, MAX_PAGES_API + 1):
        params = dict(base_params)
        params["pagina"] = page
        params["tamanhoPagina"] = PAGE_SIZE_API
        page_items, total_pages = _get_api_page(url, params)
        if not page_items:
            break
        items.extend(page_items)
        if total_pages and page >= total_pages:
            break
    return items


def _status_match_publicacao(item: Dict, status_value: str) -> bool:
    if not status_value:
        return True

    situacao = _safe_text(item.get("situacaoCompraId"))
    fim = pd.to_datetime(item.get("dataEncerramentoProposta"), errors="coerce", utc=False)
    now = pd.Timestamp.now()

    recebendo = bool(pd.notna(fim) and fim >= now)
    encerrada_por_data = bool(pd.notna(fim) and fim < now)
    cancelada_ou_final = situacao in {"2", "3", "4"}

    if status_value == "recebendo_proposta":
        return situacao == "1" and recebendo
    if status_value == "em_julgamento":
        return situacao == "1" and encerrada_por_data
    if status_value == "encerrado":
        return cancelada_ou_final or encerrada_por_data
    return True


def _buscar_publicacao_municipio(uf: str, codigo_ibge: str) -> Tuple[List[Dict], List[str]]:
    data_final = datetime.now().strftime("%Y%m%d")
    data_inicial = (datetime.now() - timedelta(days=PUBLICACAO_DIAS_LOOKBACK)).strftime("%Y%m%d")
    rows: List[Dict] = []
    erros: List[str] = []

    for modalidade in MODALIDADES_CONSULTA:
        try:
            rows.extend(
                _iter_pages(
                    API_CONSULTA_PUBLICACAO,
                    {
                        "dataInicial": data_inicial,
                        "dataFinal": data_final,
                        "codigoModalidadeContratacao": modalidade,
                        "uf": uf,
                        "codigoMunicipioIbge": codigo_ibge,
                    },
                )
            )
        except Exception as exc:
            if _is_request_rejected_error(exc):
                erros.append("PNCP rejeitou temporariamente consultas por publicacao")
                break
            erros.append(f"modalidade {modalidade}: {exc}")

    return rows, erros


def _normalizar_item(item: Dict, municipio_ref: Dict[str, str]) -> Dict:
    orgao = _first_dict(item.get("orgaoSubRogado"), item.get("orgaoEntidade"))
    unidade = _first_dict(item.get("unidadeSubRogada"), item.get("unidadeOrgao"))

    numero_controle = _safe_text(item.get("numeroControlePNCP"))
    ctrl_cnpj, ctrl_ano, ctrl_seq = _parse_numero_controle(numero_controle)

    cnpj = _safe_text(orgao.get("cnpj")) or ctrl_cnpj
    ano = _safe_text(item.get("anoCompra")) or ctrl_ano
    seq = _safe_text(item.get("sequencialCompra")) or ctrl_seq
    numero = _safe_text(item.get("numeroCompra"))
    processo = _safe_text(item.get("processo"))
    tipo = _safe_text(item.get("tipoInstrumentoConvocatorioNome")) or "Edital"

    if numero and ano:
        titulo = f"{tipo} n° {numero}/{ano}"
    elif numero:
        titulo = f"{tipo} n° {numero}"
    else:
        titulo = numero_controle or "(Sem titulo)"
    if processo:
        titulo = f"{titulo} | Processo {processo}"

    cidade = _safe_text(unidade.get("municipioNome")) or _safe_text(municipio_ref.get("nome"))
    uf = _safe_text(unidade.get("ufSigla")).upper() or _safe_text(municipio_ref.get("uf")).upper()
    pub_raw = _safe_text(item.get("dataPublicacaoPncp")) or _safe_text(item.get("dataInclusao"))
    fim_raw = _safe_text(item.get("dataEncerramentoProposta"))

    return {
        "municipio_codigo": _safe_text(municipio_ref.get("codigo_pncp")) or _safe_text(municipio_ref.get("codigo_ibge")),
        "municipio_codigo_ibge": _safe_text(municipio_ref.get("codigo_ibge")),
        "Cidade": cidade,
        "UF": uf,
        "Título": titulo,
        "Objeto": _safe_text(item.get("objetoCompra")),
        "Link para o edital": _build_pncp_link(cnpj, ano, seq),
        "Modalidade": _safe_text(item.get("modalidadeNome")),
        "Tipo": tipo,
        "Tipo (documento)": tipo,
        "Orgão": _safe_text(orgao.get("razaoSocial")),
        "Unidade": _safe_text(unidade.get("nomeUnidade")),
        "Esfera": _safe_text(orgao.get("esferaId")),
        "Publicação": _fmt_dt_iso_to_br(pub_raw),
        "Fim do envio de proposta": _fmt_dt_iso_to_br(fim_raw),
        "numero_processo": processo,
        "_pub_raw": pub_raw,
        "_orgao_cnpj": cnpj,
        "_ano": ano,
        "_seq": seq,
        "_id": numero_controle,
    }


def _dedupe_key(item: Dict) -> str:
    key = _safe_text(item.get("numeroControlePNCP"))
    if key:
        return key
    orgao = _first_dict(item.get("orgaoEntidade"))
    raw = "|".join(
        [
            _safe_text(orgao.get("cnpj")),
            _safe_text(item.get("anoCompra")),
            _safe_text(item.get("sequencialCompra")),
            _safe_text(item.get("numeroCompra")),
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def buscar_municipio_api(municipio: Dict[str, str], status_value: str, q: str) -> Tuple[List[Dict], List[str]]:
    codigo_ibge = _safe_text(municipio.get("codigo_ibge"))
    uf = _safe_text(municipio.get("uf")).upper()
    registros: List[Dict] = []
    erros: List[str] = []
    vistos = set()
    aplicar_filtro_publicacao = status_value != "recebendo_proposta"

    try:
        if status_value == "recebendo_proposta":
            data_final = (datetime.now() + timedelta(days=PROPOSTA_DIAS_A_FRENTE)).strftime("%Y%m%d")
            erro_proposta = ""
            try:
                rows = _iter_pages(
                    API_CONSULTA_PROPOSTA,
                    {
                        "dataFinal": data_final,
                        "uf": uf,
                        "codigoMunicipioIbge": codigo_ibge,
                    },
                )
            except Exception as exc_agregado:
                rows = []
                erros_modalidade: List[str] = []
                if _is_request_rejected_error(exc_agregado):
                    erro_proposta = "PNCP rejeitou temporariamente a consulta por excesso/bloqueio de requisicoes"
                else:
                    for modalidade in MODALIDADES_CONSULTA:
                        try:
                            rows.extend(
                                _iter_pages(
                                    API_CONSULTA_PROPOSTA,
                                    {
                                        "dataFinal": data_final,
                                        "codigoModalidadeContratacao": modalidade,
                                        "uf": uf,
                                        "codigoMunicipioIbge": codigo_ibge,
                                    },
                                )
                            )
                        except Exception as exc_modalidade:
                            if _is_request_rejected_error(exc_modalidade):
                                erros_modalidade.append("PNCP rejeitou temporariamente consultas por modalidade")
                                break
                            erros_modalidade.append(f"modalidade {modalidade}: {exc_modalidade}")
                if not rows:
                    detalhe = "; ".join(erros_modalidade[:3])
                    if not erro_proposta:
                        erro_proposta = f"consulta por proposta falhou ({exc_agregado}); {detalhe}"

            if not rows:
                if _is_request_rejected_error(erro_proposta):
                    erros.append(f"{municipio.get('nome')} / {uf}: {erro_proposta}. Tente novamente em alguns minutos.")
                else:
                    rows_publicacao, erros_publicacao = _buscar_publicacao_municipio(uf, codigo_ibge)
                    rows = rows_publicacao
                    aplicar_filtro_publicacao = True
                    if not rows_publicacao and erros_publicacao:
                        detalhe = "; ".join(erros_publicacao[:3])
                        prefixo = f"{erro_proposta}; " if erro_proposta else ""
                        erros.append(f"{municipio.get('nome')} / {uf}: {prefixo}recuperacao por publicacao falhou; {detalhe}")
        else:
            rows, erros_publicacao = _buscar_publicacao_municipio(uf, codigo_ibge)
            if not rows and erros_publicacao:
                detalhe = "; ".join(erros_publicacao[:3])
                erros.append(f"{municipio.get('nome')} / {uf}: consulta por publicacao falhou; {detalhe}")
    except Exception as exc:
        erros.append(f"{municipio.get('nome')} / {uf}: {exc}")
        rows = []

    q_norm = _norm(q)
    for item in rows:
        if aplicar_filtro_publicacao and not _status_match_publicacao(item, status_value):
            continue

        key = _dedupe_key(item)
        if key in vistos:
            continue
        vistos.add(key)

        registro = _normalizar_item(item, municipio)

        if q_norm:
            alvo = _norm(
                " ".join(
                    [
                        registro.get("Título", ""),
                        registro.get("Objeto", ""),
                        registro.get("Orgão", ""),
                        registro.get("Modalidade", ""),
                    ]
                )
            )
            if q_norm not in alvo:
                continue

        registros.append(registro)

    return registros, erros


def coletar_por_assinatura(signature: dict) -> Tuple[List[Dict], List[str]]:
    # Resultado de busca e instabilidade do PNCP nao devem ficar presos em cache.
    # O estado da tela ja guarda a ultima coleta ate o usuario clicar em Pesquisar de novo.
    registros: List[Dict] = []
    erros: List[str] = []

    for municipio in signature.get("municipios_meta", []):
        rows, err = buscar_municipio_api(
            municipio=municipio,
            status_value=_safe_text(signature.get("status")),
            q=_safe_text(signature.get("q")),
        )
        registros.extend(rows)
        erros.extend(err)

    if not registros:
        return [], erros

    df = pd.DataFrame(registros)
    try:
        df["_pub_dt"] = pd.to_datetime(df["_pub_raw"], errors="coerce", utc=False)
    except Exception:
        df["_pub_dt"] = pd.NaT
    df.sort_values("_pub_dt", ascending=False, na_position="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df.to_dict("records"), erros


# ==========================
# Estado e sidebar
# ==========================
def _ensure_session_state() -> None:
    if "selected_municipios" not in st.session_state:
        st.session_state.selected_municipios = []
    force_reload_persistence = st.session_state.get("persistence_version") != PERSISTENCE_VERSION
    if force_reload_persistence or "saved_searches" not in st.session_state:
        st.session_state.saved_searches = _load_saved_searches()
    if "sidebar_inputs" not in st.session_state:
        st.session_state.sidebar_inputs = {
            "palavra_chave": "",
            "status_label": STATUS_LABELS[0],
            "uf": UF_PLACEHOLDER,
            "save_name": "",
            "selected_saved": None,
        }
    if "uf_prev" not in st.session_state:
        st.session_state.uf_prev = UF_PLACEHOLDER
    if "municipio_nonce" not in st.session_state:
        st.session_state.municipio_nonce = 0
    if "results_df" not in st.session_state:
        st.session_state.results_df = None
    if "results_signature" not in st.session_state:
        st.session_state.results_signature = None
    if "result_errors" not in st.session_state:
        st.session_state.result_errors = []
    if "card_page" not in st.session_state:
        st.session_state.card_page = 1
    if "page_size_cards" not in st.session_state:
        st.session_state.page_size_cards = 10
    if force_reload_persistence or "tr_marks" not in st.session_state:
        st.session_state.tr_marks = _load_marks("tr_marks.json", SAVED_TR_PATHS)
    if force_reload_persistence or "na_marks" not in st.session_state:
        st.session_state.na_marks = _load_marks("na_marks.json", SAVED_NA_PATHS)
    st.session_state.persistence_version = PERSISTENCE_VERSION


def _normalize_municipio_payload(m: Dict, fallback_uf: str = "") -> Optional[Dict[str, str]]:
    nome = _safe_text(m.get("nome") or m.get("municipio") or m.get("Cidade"))
    uf = _safe_text(m.get("uf") or m.get("UF") or fallback_uf).upper()
    codigo_pncp = _safe_text(m.get("codigo_pncp") or m.get("codigoPNCP") or m.get("codigo") or m.get("id"))
    codigo_ibge = _safe_text(
        m.get("codigo_ibge")
        or m.get("codigoIbge")
        or m.get("ibge")
        or m.get("codigo_municipio_ibge")
        or m.get("municipio_ibge")
    )

    if nome and uf:
        if not codigo_ibge.isdigit():
            try:
                ibge = resolver_municipio_ibge(nome, uf)
            except Exception:
                ibge = None
            codigo_ibge = _safe_text(ibge.get("codigo_ibge")) if ibge else ""
            if ibge:
                nome = _safe_text(ibge.get("nome")) or nome
                uf = _safe_text(ibge.get("uf")).upper() or uf
            elif codigo_pncp.isdigit():
                codigo_ibge = codigo_pncp
        if not codigo_pncp:
            codigo_pncp = resolver_codigo_pncp(nome, uf, codigo_ibge)
        if codigo_ibge:
            return {"nome": nome, "uf": uf, "codigo_pncp": codigo_pncp, "codigo_ibge": codigo_ibge}
    return None


def _add_municipio(row: Dict[str, str]) -> None:
    if len(st.session_state.selected_municipios) >= MAX_MUNICIPIOS:
        st.warning(f"Limite de {MAX_MUNICIPIOS} municipios por pesquisa atingido.")
        return
    codigo = _safe_text(row.get("codigo_ibge"))
    if not codigo:
        return
    if codigo in [m.get("codigo_ibge") for m in st.session_state.selected_municipios]:
        return
    nome = _safe_text(row.get("nome"))
    uf = _safe_text(row.get("uf")).upper()
    codigo_pncp = resolver_codigo_pncp(nome, uf, codigo)
    st.session_state.selected_municipios.append(
        {
            "codigo_pncp": codigo_pncp,
            "nome": nome,
            "uf": uf,
            "codigo_ibge": codigo,
        }
    )


def _normalized_selected_municipios(fallback_uf: str = "") -> List[Dict[str, str]]:
    normalized_items: List[Dict[str, str]] = []
    seen = set()
    for raw in st.session_state.get("selected_municipios", []):
        if not isinstance(raw, dict):
            continue
        normalized = _normalize_municipio_payload(raw, fallback_uf=fallback_uf)
        if not normalized:
            continue
        key = normalized.get("codigo_ibge") or f"{normalized.get('uf')}::{_norm(normalized.get('nome'))}"
        if key in seen:
            continue
        seen.add(key)
        normalized_items.append(normalized)
    return normalized_items


def _sidebar() -> bool:
    st.sidebar.header("Filtros")

    palavra = st.sidebar.text_input(
        "Palavra-chave (titulo/objeto/orgao):",
        value=st.session_state.sidebar_inputs["palavra_chave"],
        key="palavra_chave_input",
    )
    status_label = st.sidebar.radio(
        "Status",
        STATUS_LABELS,
        index=STATUS_LABELS.index(st.session_state.sidebar_inputs["status_label"])
        if st.session_state.sidebar_inputs["status_label"] in STATUS_LABELS
        else 0,
        key="status_radio",
    )

    uf_options = [UF_PLACEHOLDER] + UFS
    uf = st.sidebar.selectbox(
        "Estado (UF) - Obrigatorio:",
        uf_options,
        index=uf_options.index(st.session_state.sidebar_inputs["uf"])
        if st.session_state.sidebar_inputs["uf"] in uf_options
        else 0,
        key="uf_select",
    )

    if uf != st.session_state.uf_prev:
        st.session_state.uf_prev = uf
        st.session_state.municipio_nonce += 1

    st.sidebar.markdown("**Municipios (max. 25)**")
    if uf == UF_PLACEHOLDER:
        st.sidebar.info("Selecione uma UF para habilitar os municipios.")
        mun_df = pd.DataFrame()
        chosen = "—"
        add_clicked = False
    else:
        try:
            mun_df = load_municipios_ibge(uf)
            labels = ["—"] + mun_df["label"].tolist()
            chosen = st.sidebar.selectbox(
                "Adicionar municipio:",
                labels,
                index=0,
                key=f"municipio_select_{st.session_state.municipio_nonce}",
            )
            add_clicked = st.sidebar.button(
                "Adicionar municipio",
                use_container_width=True,
                key=f"add_mun_btn_{st.session_state.municipio_nonce}",
            )
        except Exception as exc:
            st.sidebar.error(f"Falha ao carregar municipios do IBGE: {exc}")
            mun_df = pd.DataFrame()
            chosen = "—"
            add_clicked = False

    if add_clicked:
        if chosen == "—":
            st.sidebar.warning("Selecione um municipio antes de adicionar.")
        else:
            hit = mun_df[mun_df["label"] == chosen]
            if not hit.empty:
                _add_municipio(hit.iloc[0].to_dict())

    if st.session_state.selected_municipios:
        st.sidebar.caption("Selecionados:")
        keep = []
        for m in st.session_state.selected_municipios:
            c1, c2 = st.sidebar.columns([0.82, 0.18])
            with c1:
                st.markdown(f"- **{m['nome']}**")
            with c2:
                if st.button("x", key=f"rm_{m.get('codigo_ibge')}", help=f"Remover {m.get('nome')}"):
                    pass
                else:
                    keep.append(m)
        if len(keep) != len(st.session_state.selected_municipios):
            st.session_state.selected_municipios = keep
            st.rerun()

    st.sidebar.subheader("Pesquisa salva")
    save_name = st.sidebar.text_input(
        "Nome da pesquisa",
        value=st.session_state.sidebar_inputs["save_name"],
        key="save_name_input",
    )
    col_s1, col_s2 = st.sidebar.columns(2)
    with col_s1:
        salvar = st.button("Salvar", use_container_width=True, key="btn_salvar")
    with col_s2:
        excluir = st.button("Excluir", use_container_width=True, key="btn_excluir")

    if excluir:
        name = save_name.strip()
        if name and name in st.session_state.saved_searches:
            del st.session_state.saved_searches[name]
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' excluida.")
        else:
            st.sidebar.error("Informe o nome exato de uma pesquisa salva.")

    if salvar:
        name = save_name.strip()
        if not name:
            st.sidebar.error("Informe um nome para salvar.")
        else:
            municipios_para_salvar = _normalized_selected_municipios(fallback_uf=uf)
            st.session_state.selected_municipios = municipios_para_salvar
            st.session_state.saved_searches[name] = {
                "palavra_chave": palavra,
                "status_label": status_label,
                "uf": uf,
                "municipios": municipios_para_salvar,
            }
            _persist_saved_searches(st.session_state.saved_searches)
            st.sidebar.success(f"Pesquisa '{name}' salva.")

    st.sidebar.subheader("Pesquisas salvas")
    saved_names = sorted(st.session_state.saved_searches.keys())
    selected_saved = st.sidebar.selectbox("Carregar pesquisa", ["—"] + saved_names, index=0, key="select_saved")
    carregar = st.sidebar.button("Carregar", use_container_width=True, key="btn_carregar")

    if carregar and selected_saved and selected_saved != "—":
        payload = st.session_state.saved_searches.get(selected_saved, {})
        municipios: List[Dict[str, str]] = []
        fallback_uf = _safe_text(payload.get("uf")).upper()
        raw_municipios = payload.get("municipios") or payload.get("selected_municipios") or []
        for raw in raw_municipios:
            if isinstance(raw, dict):
                normalized = _normalize_municipio_payload(raw, fallback_uf=fallback_uf)
                if normalized:
                    municipios.append(normalized)
            elif isinstance(raw, str) and fallback_uf:
                normalized = resolver_municipio_ibge(raw, fallback_uf)
                if normalized:
                    municipios.append(normalized)

        st.session_state.sidebar_inputs["palavra_chave"] = _safe_text(payload.get("palavra_chave"))
        saved_status = _safe_text(payload.get("status_label"))
        st.session_state.sidebar_inputs["status_label"] = saved_status if saved_status in STATUS_LABELS else STATUS_LABELS[0]
        st.session_state.sidebar_inputs["uf"] = _safe_text(payload.get("uf")) or UF_PLACEHOLDER
        st.session_state.sidebar_inputs["save_name"] = selected_saved
        st.session_state.selected_municipios = municipios
        st.session_state.uf_prev = st.session_state.sidebar_inputs["uf"]
        st.session_state.municipio_nonce += 1
        st.sidebar.success(f"Pesquisa '{selected_saved}' carregada.")
        st.rerun()

    st.session_state.sidebar_inputs["palavra_chave"] = palavra
    st.session_state.sidebar_inputs["status_label"] = status_label
    st.session_state.sidebar_inputs["uf"] = uf
    st.session_state.sidebar_inputs["save_name"] = save_name
    st.session_state.sidebar_inputs["selected_saved"] = selected_saved

    pesquisar = st.sidebar.button("Pesquisar", use_container_width=True, type="primary", key="btn_pesquisar")
    if pesquisar and uf == UF_PLACEHOLDER:
        st.sidebar.error("Selecione uma UF para pesquisar.")
        pesquisar = False
    return pesquisar


def _cb_prev(total_pages: int) -> None:
    st.session_state.card_page = max(1, int(st.session_state.get("card_page", 1)) - 1)


def _cb_next(total_pages: int) -> None:
    st.session_state.card_page = min(total_pages, int(st.session_state.get("card_page", 1)) + 1)


def _cb_page_size_change() -> None:
    st.session_state.card_page = 1


# ==========================
# UI principal
# ==========================
def _inject_css() -> None:
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] {
          background: #eef4ff !important;
          border-right: 1px solid #dfe8ff;
          min-width: 360px !important;
          max-width: 360px !important;
        }
        section[data-testid="stSidebar"] * { color: #112a52 !important; }
        section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
          color: #ffffff !important;
          background: #1f4ba8 !important;
          border: 1px solid #173a83 !important;
        }
        header[data-testid="stHeader"] { background: transparent !important; box-shadow: none !important; height: 3rem; }
        div.block-container { padding-top: 2.1rem; background: #f7faff; padding-bottom: 2rem; }
        .stDownloadButton > button {
          color: #ffffff !important;
          background: #1f4ba8 !important;
          border: 1px solid #173a83 !important;
          font-size: 0.7rem !important;
          padding: 0.28rem 0.6rem !important;
        }
        .ac-card {
          background: #f8fbff;
          border: 1.25px solid #cad9f3;
          border-radius: 18px;
          padding: 1.05rem 1.2rem;
          margin-bottom: 1rem;
          box-shadow: 0 8px 20px rgba(20, 45, 110, 0.06);
        }
        .ac-card h3 { margin-top: 0; margin-bottom: 0.25rem; font-size: 1.08rem; color: #0b1b36; }
        .ac-muted { color: #415477; font-size: 0.92rem; }
        .ac-badge {
          background: #eaf1ff; border: 1px solid #bcd0f7; color: #0b3b8a;
          padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.82rem;
        }
        .ac-flag {
          background: #e3f9e5; border: 1px solid #57b26a; color: #1b6f37;
          padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.82rem; margin-left: 0.5rem;
        }
        .ac-flag-na {
          background: #fde8e7; border: 1px solid #dc5a5a; color: #9b1c1c;
          padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.82rem; margin-left: 0.5rem;
        }
        .ac-link {
          text-decoration:none; padding:0.46rem 0.85rem; border-radius:10px;
          border:1px solid #96b3e9; color:#0b3b8a;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.title("Acerte Licitacoes - O seu Buscador de Editais")
    st.caption(
        "Selecione os filtros desejados, escolha o Estado (UF) e adicione ate 25 municipios. "
        "Os editais serao exibidos abaixo em ordem de publicacao."
    )

    _inject_css()
    _ensure_session_state()
    disparar_busca = _sidebar()

    status_value = STATUS_MAP.get(st.session_state.sidebar_inputs["status_label"], "")
    palavra_chave = _safe_text(st.session_state.sidebar_inputs["palavra_chave"])
    st.session_state.selected_municipios = _normalized_selected_municipios(
        fallback_uf=_safe_text(st.session_state.sidebar_inputs["uf"])
    )
    data_final_proposta = (datetime.now() + timedelta(days=PROPOSTA_DIAS_A_FRENTE)).strftime("%Y%m%d")
    data_final_publicacao = datetime.now().strftime("%Y%m%d")
    signature = {
        "municipios": [_safe_text(m.get("codigo_ibge")) for m in st.session_state.selected_municipios],
        "municipios_meta": [
            {
                "codigo_pncp": _safe_text(m.get("codigo_pncp")),
                "nome": _safe_text(m.get("nome")),
                "uf": _safe_text(m.get("uf")).upper(),
                "codigo_ibge": _safe_text(m.get("codigo_ibge")),
            }
            for m in st.session_state.selected_municipios
        ],
        "status": status_value,
        "q": palavra_chave.lower(),
        "api": "pncp_consulta_v1",
        "proposta_dias_a_frente": PROPOSTA_DIAS_A_FRENTE,
        "data_final_proposta": data_final_proposta,
        "data_final_publicacao": data_final_publicacao,
    }

    if disparar_busca:
        if not signature["municipios"]:
            st.warning("Selecione pelo menos um municipio para pesquisar.")
            st.stop()
        with st.spinner("Consultando API oficial PNCP..."):
            records, errors = coletar_por_assinatura(signature)
        st.session_state.results_df = records
        st.session_state.result_errors = errors
        st.session_state.results_signature = signature
        st.session_state.card_page = 1
    else:
        if st.session_state.results_df is None:
            st.info("Configure os filtros e clique em **Pesquisar**.")
            st.stop()
        records = st.session_state.results_df
        if st.session_state.results_signature and signature != st.session_state.results_signature:
            st.warning("Filtros alterados apos a ultima coleta. Clique em **Pesquisar** para atualizar.")

    if st.session_state.result_errors:
        with st.expander("Avisos da coleta"):
            for err in st.session_state.result_errors:
                st.warning(err)

    df = pd.DataFrame(records)
    st.subheader(f"Resultados ({len(df)})")
    if df.empty:
        st.info("Nenhum resultado encontrado com os criterios atuais.")
        return

    if "_pub_dt" not in df.columns:
        try:
            df["_pub_dt"] = pd.to_datetime(df["_pub_raw"], errors="coerce", utc=False)
        except Exception:
            df["_pub_dt"] = pd.NaT
        df.sort_values("_pub_dt", ascending=False, na_position="last", inplace=True)
        df.reset_index(drop=True, inplace=True)

    st.selectbox(
        "Itens por pagina",
        [10, 20, 50],
        index=[10, 20, 50].index(st.session_state.get("page_size_cards", 10))
        if st.session_state.get("page_size_cards", 10) in [10, 20, 50]
        else 0,
        key="page_size_cards",
        on_change=_cb_page_size_change,
    )

    page_size = int(st.session_state.get("page_size_cards", 10))
    total_items = len(df)
    total_pages = max(1, (total_items + page_size - 1) // page_size)

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_a:
        st.button(
            "Anterior",
            key="prev_top",
            disabled=st.session_state.get("card_page", 1) <= 1,
            on_click=_cb_prev,
            kwargs={"total_pages": total_pages},
        )
    with col_b:
        st.markdown(f"**Pagina {st.session_state.get('card_page', 1)} de {total_pages}**")
    with col_c:
        st.button(
            "Proxima",
            key="next_top",
            disabled=st.session_state.get("card_page", 1) >= total_pages,
            on_click=_cb_next,
            kwargs={"total_pages": total_pages},
        )

    start = (st.session_state.get("card_page", 1) - 1) * page_size
    end = start + page_size
    page_df = df.iloc[start:end].copy()

    for _, row in page_df.iterrows():
        row_dict = row.to_dict()
        uid_candidates = _uid_candidates_from_row(row_dict)
        uid = uid_candidates[0]
        tr_flag = any(bool(st.session_state.tr_marks.get(candidate, False)) for candidate in uid_candidates)
        na_flag = any(bool(st.session_state.na_marks.get(candidate, False)) for candidate in uid_candidates)

        col_spacer, col_cb_tr, col_cb_na = st.columns([6, 1.3, 1.3])
        with col_cb_tr:
            new_tr = st.checkbox("TR Elaborado", value=tr_flag, key=f"tr_{uid}")
        with col_cb_na:
            new_na = st.checkbox("Não Atende", value=na_flag, key=f"na_{uid}")

        changed = False
        if new_tr != tr_flag:
            for candidate in uid_candidates:
                st.session_state.tr_marks[candidate] = bool(new_tr)
            tr_flag = bool(new_tr)
            changed = True
        if new_na != na_flag:
            for candidate in uid_candidates:
                st.session_state.na_marks[candidate] = bool(new_na)
            na_flag = bool(new_na)
            changed = True
        if changed:
            _persist_marks("tr_marks.json", SAVED_TR_PATHS, st.session_state.tr_marks)
            _persist_marks("na_marks.json", SAVED_NA_PATHS, st.session_state.na_marks)
            st.rerun()

        link = _safe_text(row.get("Link para o edital"))
        tr_badge = '<span class="ac-flag">TR Elaborado</span>' if tr_flag else ""
        na_badge = '<span class="ac-flag-na">Não Atende</span>' if na_flag else ""
        processo = _safe_text(row.get("numero_processo"))
        processo_html = f'<div class="ac-muted">Processo: {_escape(processo)}</div>' if processo else "<div></div>"

        html_card = f"""
        <div class="ac-card">
            <h3>{_escape(row.get("Título") or row.get("Titulo"))} {tr_badge} {na_badge}</h3>
            <div class="ac-muted">
                <span class="ac-badge">{_escape(row.get("Cidade"))} / {_escape(row.get("UF"))}</span>
                &nbsp;•&nbsp;
                <strong>Publicação:</strong> {_escape(row.get("Publicação") or row.get("Publicacao"))}
                &nbsp;|&nbsp;
                <strong>Fim do envio:</strong> {_escape(row.get("Fim do envio de proposta"))}
            </div>
            <div style="margin-top:0.55rem;"><strong>Objeto:</strong> {_escape(row.get("Objeto"))}</div>
            <div style="display:flex; gap:1rem; margin-top:0.55rem; flex-wrap:wrap;">
                <div><strong>Modalidade:</strong> {_escape(row.get("Modalidade"))}</div>
                <div><strong>Tipo:</strong> {_escape(row.get("Tipo"))}</div>
                <div><strong>Órgão:</strong> {_escape(row.get("Orgão") or row.get("Orgao"))}</div>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; margin-top:0.7rem;">
                {processo_html}
                {f'<a class="ac-link" href="{_escape(link)}" target="_blank">Abrir edital</a>' if link else ''}
            </div>
        </div>
        """
        st.markdown(html_card, unsafe_allow_html=True)

    col_a2, col_b2, col_c2 = st.columns([1, 2, 1])
    with col_a2:
        st.button(
            "Anterior",
            key="prev_bottom",
            disabled=st.session_state.get("card_page", 1) <= 1,
            on_click=_cb_prev,
            kwargs={"total_pages": total_pages},
        )
    with col_b2:
        st.markdown(f"**Pagina {st.session_state.get('card_page', 1)} de {total_pages}**")
    with col_c2:
        st.button(
            "Proxima",
            key="next_bottom",
            disabled=st.session_state.get("card_page", 1) >= total_pages,
            on_click=_cb_next,
            kwargs={"total_pages": total_pages},
        )

    st.divider()

    drop_cols = [c for c in ["_pub_raw", "_pub_dt", "_orgao_cnpj", "_ano", "_seq", "_id"] if c in df.columns]
    export_df = df.drop(columns=drop_cols, errors="ignore").copy()

    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="PNCP")
    xlsx_bytes = xlsx_buf.getvalue()

    st.markdown("### Baixar planilha")
    st.download_button(
        "Baixar XLSX",
        data=xlsx_bytes,
        file_name=f"pncp_resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary",
    )


if __name__ == "__main__":
    main()
