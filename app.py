# app.py
from __future__ import annotations

import io
import time
import requests
import pandas as pd
import streamlit as st
from typing import Dict, List, Tuple

# ==========================
# Configuração básica
# ==========================
st.set_page_config(page_title="Acerte Licitações — O seu Buscador de Editais", layout="wide")

BASE_API = "https://pncp.gov.br/api/search/"
ORIGIN = "https://pncp.gov.br"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Referer": "https://pncp.gov.br/app/editais",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# ==========================
# Municípios padrão
# ==========================
MUNICIPIOS_PADRAO: List[Dict[str, str]] = [
    {"nome": "Porto Feliz", "codigo": "3721"},
    {"nome": "Conchas", "codigo": "3405"},
    {"nome": "Torre de Pedra", "codigo": "3878"},
    {"nome": "Porangaba", "codigo": "3720"},
    {"nome": "Guareí", "codigo": "3477"},
    {"nome": "Quadra", "codigo": "3735"},
    {"nome": "Angatuba", "codigo": "3292"},
    {"nome": "Capão Bonito", "codigo": "3385"},
    {"nome": "Campina do Monte Alegre", "codigo": "3375"},
    {"nome": "Pilar do Sul", "codigo": "3694"},
    {"nome": "Sarapuí", "codigo": "3838"},
    {"nome": "Alambari", "codigo": ""},  # sem código → pula
    {"nome": "Capela do Alto", "codigo": "3386"},
    {"nome": "Itapetininga", "codigo": "3523"},
    {"nome": "São Miguel Arcanjo", "codigo": "3829"},
    {"nome": "Cesário Lange", "codigo": "3399"},
    {"nome": "Itararé", "codigo": "3533"},
    {"nome": "Buri", "codigo": "3359"},
]

# ==========================
# Utilitários
# ==========================
def _items_from_json(js) -> List[Dict]:
    if isinstance(js, dict):
        for k in ["items", "results", "conteudo", "licitacoes", "data", "documents", "documentos"]:
            v = js.get(k)
            if isinstance(v, list):
                return v
    if isinstance(js, list):
        return js
    return []

def _fmt_dt_iso_to_br(dt: str) -> str:
    if not dt:
        return ""
    try:
        ts = pd.to_datetime(dt, errors="coerce", utc=False)
        if pd.isna(ts):
            return ""
        return ts.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""

def _full_url(item_url: str) -> str:
    if not item_url:
        return ""
    if item_url.startswith("http"):
        return item_url
    return ORIGIN.rstrip("/") + "/" + item_url.lstrip("/")

def _build_pncp_link(item: Dict) -> str:
    """
    Preferência: https://pncp.gov.br/app/editais/{orgao_cnpj}/{ano}/{numero_sequencial}
    Fallback: converte qualquer '/compras/' para '/app/editais/'.
    """
    cnpj = str(item.get("orgao_cnpj", "") or "").strip()
    ano = str(item.get("ano", "") or "").strip()
    seq = str(item.get("numero_sequencial", "") or "").strip()
    if len(cnpj) == 14 and ano.isdigit() and seq:
        return f"{ORIGIN}/app/editais/{cnpj}/{ano}/{seq}"

    raw = item.get("item_url", "") or ""
    url = _full_url(raw)
    url = url.replace("/app/compras/", "/app/editais/").replace("/compras/", "/app/editais/")
    return url

# ==========================
# Coleta via API
# ==========================
def consultar_pncp_por_municipio(
    municipio_id: str,
    status: str = "recebendo_proposta",
    tam_pagina: int = 100,
    delay_s: float = 0.05,
) -> List[Dict]:
    out: List[Dict] = []
    pagina = 1
    while True:
        params = {
            "tipos_documento": "edital",
            "ordenacao": "-data",
            "pagina": pagina,
            "tam_pagina": tam_pagina,
            "municipios": municipio_id,
        }
        if status:
            params["status"] = status

        r = requests.get(BASE_API, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        js = r.json()
        itens = _items_from_json(js)

        if not itens:
            break

        out.extend(itens)
        if len(itens) < tam_pagina:
            break

        pagina += 1
        time.sleep(delay_s)
    return out

def montar_registro(item: Dict) -> Dict:
    return {
        "municipio_nome": item.get("municipio_nome", ""),
        "uf": item.get("uf", ""),
        "Title": item.get("title", ""),
        "description": item.get("description", ""),
        "item_url": _build_pncp_link(item),  # link correto
        "document_type": item.get("document_type", ""),
        "orgao_nome": item.get("orgao_nome", ""),
        "unidade_nome": item.get("unidade_nome", ""),
        "esfera_nome": item.get("esfera_nome", ""),
        "modalidade_licitacao_nome": item.get("modalidade_licitacao_nome", ""),
        "data_publicacao_pncp": _fmt_dt_iso_to_br(item.get("data_publicacao_pncp") or ""),
        "data_fim_vigencia": _fmt_dt_iso_to_br(item.get("data_fim_vigencia") or ""),
        "tipo_nome": item.get("tipo_nome", ""),
    }

def coletar_todos_os_municipios(
    municipios: List[Dict[str, str]],
    status: str,
    tam_pagina: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    registros: List[Dict] = []
    falhas: List[Dict] = []

    total = len(municipios)
    for i, mun in enumerate(municipios, 1):
        nome = mun.get("nome", "")
        codigo = mun.get("codigo", "")
        if not codigo:
            falhas.append({"municipio": nome, "motivo": "Sem código PNCP"})
            continue

        with st.spinner(f"Consultando {nome} ({codigo}) [{i}/{total}]…"):
            try:
                itens = consultar_pncp_por_municipio(codigo, status=status, tam_pagina=tam_pagina)
                for it in itens:
                    registros.append(montar_registro(it))
                st.success(f"{nome}: {len(itens)} item(ns) encontrado(s).")
            except Exception as e:
                falhas.append({"municipio": nome, "motivo": str(e)})

    df_ok = pd.DataFrame(registros)
    df_fail = pd.DataFrame(falhas)
    return df_ok, df_fail

# ==========================
# UI
# ==========================
st.title("📑 Acerte Licitações — O seu Buscador de Editais")

with st.expander("ℹ️ Como funciona?", expanded=False):
    st.markdown(
        "- Busca diretamente no endpoint **/api/search** do PNCP - Portal Nacional de Contratações Públicas"
        "- Retorna as informações de todos os editais dos municípios
        "- Os campos exibidos vêm da própria API oficial do governo."
    )

col_a, col_b, col_c = st.columns([2, 1.2, 1.2])
with col_a:
    status = st.selectbox(
        "Status (PNCP)",
        options=["recebendo_proposta", "divulgado", "em_andamento", "concluido", ""],
        index=0,
        help="Recomendo 'recebendo_proposta' para editais abertos.",
    )
with col_b:
    filtro_texto = st.text_input(
        "Filtro (busca por palavras no título/descrição, opcional)",
        value="",
        placeholder="ex.: material gráfico, merenda…",
    )

st.markdown("**Municípios (nome — código PNCP):**")
st.code(", ".join([f"{m['nome']}({m['codigo']})" for m in MUNICIPIOS_PADRAO]), language="text")

btn = st.button("🔎 Executar busca", type="primary")

if btn:
    t0 = time.time()
    df, df_fail = coletar_todos_os_municipios(MUNICIPIOS_PADRAO, status=status, tam_pagina=tam_pagina)

    # Remover colunas opcionais antigas se existirem
    for drop_col in ["hora_encerramento", "encerramento_envio_proposta"]:
        if drop_col in df.columns:
            df = df.drop(columns=[drop_col])

    # Filtro simples por texto (título/descrição)
    if filtro_texto.strip() and not df.empty:
        mask = (
            df["Title"].fillna("").str.contains(filtro_texto, case=False, na=False)
            | df["description"].fillna("").str.contains(filtro_texto, case=False, na=False)
        )
        df = df[mask].copy()

    # Ordenar por data_publicacao_pncp (se houver)
    if not df.empty and "data_publicacao_pncp" in df.columns:
        try:
            _tmp = pd.to_datetime(df["data_publicacao_pncp"], format="%d/%m/%Y %H:%M", errors="coerce")
            df = df.assign(_ord=_tmp).sort_values("_ord", ascending=False, na_position="last").drop(columns=["_ord"])
        except Exception:
            pass

    # ===== Renomeação de colunas =====
    rename_map = {
        "municipio_nome": "Cidade",
        "uf": "UF",
        "Title": "Título",
        "description": "Objeto",
        "item_url": "Link para o edital",
        "document_type": "Tipo (documento)",
        "orgao_nome": "Orgão",
        "unidade_nome": "Unidade",
        "esfera_nome": "Esfera",
        "modalidade_licitacao_nome": "Modalidade",
        "data_publicacao_pncp": "Publicação",
        "data_fim_vigencia": "Fim do envio de proposta",
        "tipo_nome": "Tipo",
    }
    df = df.rename(columns=rename_map)

    # Ordem de exibição sugerida
    desired_order = [
        "Cidade", "UF", "Título", "Objeto", "Link para o edital",
        "Modalidade", "Tipo", "Tipo (documento)", "Orgão", "Unidade",
        "Esfera", "Publicação", "Fim do envio de proposta"
    ]
    df = df[[c for c in desired_order if c in df.columns]]

    st.subheader(f"Resultados ({len(df)})")
    if df.empty:
        st.info("Nenhum resultado encontrado com os critérios atuais.")
    else:
        # DataFrame com link clicável
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Link para o edital": st.column_config.LinkColumn(
                    "Link para o edital",
                    display_text="Abrir edital"
                )
            },
        )

        # --- Downloads (CSV / XLSX) ---
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        xlsx_buf = io.BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as wr:
            df.to_excel(wr, index=False, sheet_name="PNCP")
        xlsx_bytes = xlsx_buf.getvalue()

        col_d, col_e = st.columns(2)
        with col_d:
            st.download_button(
                "⬇️ Baixar CSV",
                data=csv_bytes,
                file_name="pncp_resultados.csv",
                mime="text/csv",
            )
        with col_e:
            st.download_button(
                "⬇️ Baixar XLSX",
                data=xlsx_bytes,
                file_name="pncp_resultados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    # Falhas
    if not df_fail.empty:
        st.subheader("⚠️ Falhas")
        st.dataframe(df_fail, use_container_width=True, hide_index=True)

    st.caption(f"⏱️ Tempo total: {time.time() - t0:.1f}s")
else:
    st.info("Clique em **🔎 Executar busca** para iniciar a coleta.")
