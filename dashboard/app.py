"""
Dashboard Executivo - Despesas Públicas Queimados
Layout moderno com múltiplos filtros e gráficos
Elaborado por Christian Basilio
"""

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import tempfile

# ============================================
# CONFIGURAÇÃO
# ============================================

PROJECT_ID = "monitorpublico"
DATASET = "staging_marts_queimados"
TABLE = "gastos_por_secretaria"
TABLE_DETALHADA = "registros_detalhados"

# ============================================
# CONEXÃO COM BIGQUERY
# ============================================

def get_credentials():
    # 1. Primeiro tenta variável de ambiente (para deploy no Render/Heroku)
    gcp_credentials = os.environ.get("GCP_CREDENTIALS")
    if gcp_credentials:
        try:
            cred_dict = json.loads(gcp_credentials)
            return service_account.Credentials.from_service_account_info(cred_dict)
        except Exception as e:
            print(f"Erro ao carregar credenciais da variável de ambiente: {e}")

    # 2. Tenta arquivo local (para desenvolvimento)
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for f in os.listdir(base_path):
        if f.startswith("monitorpublico") and f.endswith(".json"):
            return service_account.Credentials.from_service_account_file(os.path.join(base_path, f))

    key_path = os.path.join(base_path, "key.json")
    if os.path.exists(key_path):
        return service_account.Credentials.from_service_account_file(key_path)

    raise RuntimeError("Credenciais não encontradas")


def load_data():
    credentials = get_credentials()
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """

    df = client.query(query).to_dataframe()

    # Conversões
    df["total_despesa"] = pd.to_numeric(df["total_despesa"], errors="coerce").fillna(0)
    df["valor_liquido"] = pd.to_numeric(df["valor_liquido"], errors="coerce").fillna(0)
    df["total_estornado"] = pd.to_numeric(df["total_estornado"], errors="coerce").fillna(0)
    df["total_retido"] = pd.to_numeric(df["total_retido"], errors="coerce").fillna(0)
    df["total_registros"] = pd.to_numeric(df["total_registros"], errors="coerce").fillna(0)
    df["ano_exercicio"] = pd.to_numeric(df["ano_exercicio"], errors="coerce")
    df["data_despesa"] = pd.to_datetime(df["data_despesa"], errors="coerce")
    df["ano_mes"] = df["data_despesa"].dt.to_period("M").astype(str)
    df["mes"] = df["data_despesa"].dt.month
    df["mes_nome"] = df["data_despesa"].dt.strftime("%b")

    return df


def load_data_detalhada():
    """Carrega registros individuais (não agregados)"""
    credentials = get_credentials()
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE_DETALHADA}`
    """

    df = client.query(query).to_dataframe()

    # Conversões básicas
    df["valor_despesa"] = pd.to_numeric(df["valor_despesa"], errors="coerce").fillna(0)
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
    df["data_despesa"] = pd.to_datetime(df["data_despesa"], errors="coerce")

    return df


# ============================================
# CARREGAR DADOS
# ============================================

try:
    df = load_data()
    print("✅ Dados agregados carregados do BigQuery")
except Exception as e:
    print(f"❌ Erro ao carregar dados agregados: {e}")
    df = pd.DataFrame()

try:
    df_registros = load_data_detalhada()
    print(f"✅ Registros detalhados carregados: {len(df_registros)} registros")
except Exception as e:
    print(f"❌ Erro ao carregar registros detalhados: {e}")
    df_registros = pd.DataFrame()

# ============================================
# APP
# ============================================

app = dash.Dash(__name__, title="Monitor Público - Queimados")

# ============================================
# CORES (paleta institucional)
# ============================================

COLORS = {
    "bg": "#f0f4fd",
    "card": "#ffffff",
    "primary": "#5b6ee1",
    "primary_dark": "#4a5bc7",
    "secondary": "#f4a261",
    "accent": "#7c3aed",
    "success": "#10b981",
    "danger": "#ef4444",
    "text": "#1e293b",
    "text_light": "#64748b",
    "border": "#e2e8f0",
    "header_bg": "#5b6ee1"
}

# ============================================
# LOGO SVG (base64 encoded)
# ============================================

LOGO_SVG_BASE64 = "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNDIiIGhlaWdodD0iNDIiIHZpZXdCb3g9IjAgMCA0MiA0MiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KICA8Y2lyY2xlIGN4PSIyMSIgY3k9IjIxIiByPSIyMCIgc3Ryb2tlPSJ3aGl0ZSIgc3Ryb2tlLXdpZHRoPSIyIiBmaWxsPSJyZ2JhKDI1NSwyNTUsMjU1LDAuMTUpIi8+CiAgPHJlY3QgeD0iMTAiIHk9IjI0IiB3aWR0aD0iNSIgaGVpZ2h0PSIxMCIgcng9IjEiIGZpbGw9IndoaXRlIi8+CiAgPHJlY3QgeD0iMTgiIHk9IjE4IiB3aWR0aD0iNSIgaGVpZ2h0PSIxNiIgcng9IjEiIGZpbGw9IndoaXRlIi8+CiAgPHJlY3QgeD0iMjYiIHk9IjEyIiB3aWR0aD0iNSIgaGVpZ2h0PSIyMiIgcng9IjEiIGZpbGw9IndoaXRlIi8+CiAgPGNpcmNsZSBjeD0iMjgiIGN5PSIxNCIgcj0iNiIgc3Ryb2tlPSJ3aGl0ZSIgc3Ryb2tlLXdpZHRoPSIyIiBmaWxsPSJub25lIi8+CiAgPGxpbmUgeDE9IjMyIiB5MT0iMTgiIHgyPSIzNiIgeTI9IjIyIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIvPgo8L3N2Zz4="

# ============================================
# ESTILOS
# ============================================

card_style = {
    "backgroundColor": COLORS["card"],
    "borderRadius": "16px",
    "padding": "20px",
    "boxShadow": "0 2px 8px rgba(91, 110, 225, 0.08)",
    "border": f"1px solid {COLORS['border']}"
}

kpi_card_style = {
    **card_style,
    "textAlign": "center",
    "minHeight": "130px",
    "display": "flex",
    "flexDirection": "column",
    "justifyContent": "center",
    "alignItems": "center",
    "position": "relative",
    "overflow": "hidden"
}

title_style = {
    "fontSize": "11px",
    "fontWeight": "600",
    "color": COLORS["text_light"],
    "textTransform": "uppercase",
    "letterSpacing": "1px",
    "marginBottom": "15px"
}

kpi_value_style = {
    "fontSize": "32px",
    "fontWeight": "700",
    "margin": "10px 0 5px 0"
}

kpi_label_style = {
    "fontSize": "12px",
    "color": COLORS["text_light"],
    "fontWeight": "500"
}

# ============================================
# FUNÇÕES AUXILIARES
# ============================================

def fmt_currency(v):
    """Formata valor em moeda brasileira"""
    if v >= 1_000_000_000:
        return f"R$ {v/1_000_000_000:.2f}B"
    elif v >= 1_000_000:
        return f"R$ {v/1_000_000:.2f}M"
    elif v >= 1_000:
        return f"R$ {v/1_000:.1f}K"
    else:
        return f"R$ {v:,.0f}"

def fmt_number(v):
    """Formata número grande para exibição"""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_label(v):
    """Formata valor para label do gráfico (B para bilhões, M para milhões)"""
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B"
    else:
        return f"{v/1_000_000:.1f}M"

# ============================================
# LAYOUT
# ============================================

app.layout = html.Div([

    # ========== HEADER ==========
    html.Div([
        html.Div([
            # Logo + Nome
            html.Div([
                html.Img(
                    src=LOGO_SVG_BASE64,
                    style={
                        "width": "42px",
                        "height": "42px"
                    }
                ),
                html.Div([
                    html.H1("Monitor Público", style={
                        "fontSize": "22px",
                        "fontWeight": "700",
                        "color": "white",
                        "margin": "0",
                        "letterSpacing": "0.5px"
                    }),
                    html.P("Queimados • Despesas Pagas", style={
                        "fontSize": "12px",
                        "color": "rgba(255,255,255,0.8)",
                        "margin": "2px 0 0 0"
                    })
                ], style={"marginLeft": "12px"})
            ], style={"display": "flex", "alignItems": "center"})
        ], style={"flex": "1"}),

        # Filtros no header
        html.Div([
            dcc.Dropdown(
                id="filtro-ano",
                options=[{"label": str(int(ano)), "value": int(ano)}
                         for ano in sorted(df["ano_exercicio"].dropna().unique(), reverse=True)] if not df.empty else [],
                value=list(df["ano_exercicio"].dropna().unique()) if not df.empty else [],
                multi=True,
                placeholder="Ano",
                style={"width": "250px", "fontSize": "13px"},
                className="filter-dropdown"
            ),
            dcc.Dropdown(
                id="filtro-secretaria",
                options=[{"label": s[:35], "value": s} for s in sorted(df["secretaria_padronizada"].dropna().unique())] if not df.empty else [],
                value=[],
                multi=True,
                placeholder="Secretaria",
                style={"width": "250px", "fontSize": "13px"},
                className="filter-dropdown"
            ),
            dcc.Dropdown(
                id="filtro-funcao",
                options=[{"label": f, "value": f} for f in sorted(df["funcao"].dropna().unique())] if not df.empty else [],
                value=[],
                multi=True,
                placeholder="Função",
                style={"width": "250px", "fontSize": "13px"},
                className="filter-dropdown"
            ),
            dcc.Dropdown(
                id="filtro-fonte",
                options=[{"label": str(f)[:30], "value": f} for f in sorted(df["fonte"].dropna().unique())] if not df.empty and "fonte" in df.columns else [],
                value=[],
                multi=True,
                placeholder="Fonte",
                style={"width": "250px", "fontSize": "13px"},
                className="filter-dropdown"
            ),
        ], style={"display": "flex", "gap": "10px", "alignItems": "center"})

    ], style={
        "display": "flex",
        "justifyContent": "space-between",
        "alignItems": "center",
        "padding": "15px 30px",
        "background": f"linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['primary_dark']} 100%)",
        "boxShadow": "0 4px 20px rgba(91, 110, 225, 0.3)"
    }),

    # ========== CONTEÚDO PRINCIPAL ==========
    html.Div([

        # ========== LINHA 1: KPIs ==========
        html.Div([
            # KPI 1 - Total Despesas Pagas
            html.Div([
                html.Div(style={
                    "position": "absolute",
                    "top": "-20px",
                    "right": "-20px",
                    "width": "80px",
                    "height": "80px",
                    "borderRadius": "50%",
                    "background": f"linear-gradient(135deg, {COLORS['primary']}20 0%, {COLORS['primary']}05 100%)"
                }),
                html.Div(id="kpi-total", style={**kpi_value_style, "color": COLORS["primary"]}),
                html.Div("Total Despesas Pagas", style=kpi_label_style)
            ], style=kpi_card_style),

            # KPI 2 - Média Mensal
            html.Div([
                html.Div(style={
                    "position": "absolute",
                    "top": "-20px",
                    "right": "-20px",
                    "width": "80px",
                    "height": "80px",
                    "borderRadius": "50%",
                    "background": f"linear-gradient(135deg, {COLORS['secondary']}20 0%, {COLORS['secondary']}05 100%)"
                }),
                html.Div(id="kpi-media", style={**kpi_value_style, "color": COLORS["secondary"]}),
                html.Div("Média Mensal de Pagamentos", style=kpi_label_style)
            ], style=kpi_card_style),

            # KPI 3 - Total de Registros
            html.Div([
                html.Div(style={
                    "position": "absolute",
                    "top": "-20px",
                    "right": "-20px",
                    "width": "80px",
                    "height": "80px",
                    "borderRadius": "50%",
                    "background": f"linear-gradient(135deg, {COLORS['accent']}20 0%, {COLORS['accent']}05 100%)"
                }),
                html.Div(id="kpi-registros", style={**kpi_value_style, "color": COLORS["accent"]}),
                html.Div("Total de Pagamentos Efetuados", style=kpi_label_style)
            ], style=kpi_card_style),

        ], style={
            "display": "grid",
            "gridTemplateColumns": "repeat(3, 1fr)",
            "gap": "20px",
            "marginBottom": "20px"
        }),

        # ========== LINHA 2: Gráficos principais ==========
        html.Div([
            # Gráfico Evolução Temporal
            html.Div([
                html.Div("Evolução Mensal das Despesas Pagas", style=title_style),
                dcc.Graph(id="grafico-temporal", config={"displayModeBar": False})
            ], style={**card_style, "flex": "2"}),

            # Gráfico Tipo (Barras)
            html.Div([
                html.Div("Despesas Pagas por Tipo de Credor", style=title_style),
                dcc.Graph(id="grafico-tipo", config={"displayModeBar": False})
            ], style={**card_style, "flex": "1"}),

        ], style={
            "display": "flex",
            "gap": "20px",
            "marginBottom": "20px"
        }),

        # ========== LINHA 3: Secretarias e Unidade Orçamentária ==========
        html.Div([
            # Barras - Todas Secretarias (com scroll)
            html.Div([
                html.Div("Despesas Pagas por Secretaria", style=title_style),
                html.Div([
                    dcc.Graph(id="grafico-secretarias", config={"displayModeBar": False})
                ], style={"maxHeight": "400px", "overflowY": "auto"})
            ], style={**card_style, "flex": "1"}),

            # Barras - Unidade Orçamentária (com scroll)
            html.Div([
                html.Div("Despesas Pagas por Unidade Orçamentária", style=title_style),
                html.Div([
                    dcc.Graph(id="grafico-unidade", config={"displayModeBar": False})
                ], style={"maxHeight": "400px", "overflowY": "auto"})
            ], style={**card_style, "flex": "1"}),

        ], style={
            "display": "flex",
            "gap": "20px",
            "marginBottom": "20px"
        }),

        # ========== LINHA 4: Função e Modalidade ==========
        html.Div([
            # Barras - Por Função (com scroll)
            html.Div([
                html.Div("Despesas Pagas por Função", style=title_style),
                html.Div([
                    dcc.Graph(id="grafico-funcao", config={"displayModeBar": False})
                ], style={"maxHeight": "350px", "overflowY": "auto"})
            ], style={**card_style, "flex": "1"}),

            # Barras - Modalidade Licitação (com scroll)
            html.Div([
                html.Div("Despesas Pagas por Modalidade de Licitação", style=title_style),
                html.Div([
                    dcc.Graph(id="grafico-modalidade", config={"displayModeBar": False})
                ], style={"maxHeight": "350px", "overflowY": "auto"})
            ], style={**card_style, "flex": "1"}),

        ], style={
            "display": "flex",
            "gap": "20px",
            "marginBottom": "20px"
        }),

        # ========== LINHA 5: Tabela Agregada ==========
        html.Div([
            html.Div("Despesas Pagas por Secretaria, Função e Subfunção", style=title_style),
            html.Div(id="tabela-container")
        ], style=card_style),

        # ========== LINHA 6: Tabela de Registros Detalhados ==========
        html.Div([
            html.Div([
                html.Span("Registros Detalhados", style={
                    **title_style,
                    "display": "inline-block",
                    "marginBottom": "0",
                    "marginRight": "15px"
                }),
                html.Span("Dados não agregados • Visualize cada registro individualmente", style={
                    "fontSize": "11px",
                    "color": COLORS["text_light"],
                    "fontStyle": "italic"
                })
            ], style={"marginBottom": "15px"}),
            html.Div(id="tabela-detalhada-container", style={"overflowX": "auto"})
        ], style={**card_style, "marginTop": "20px"}),

        # ========== SOBRE O PROJETO ==========
        html.Div([
            html.Div([
                html.Div([
                    html.H3("Sobre o Projeto", style={
                        "fontSize": "18px",
                        "fontWeight": "700",
                        "color": COLORS["text"],
                        "marginBottom": "15px",
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "10px"
                    }),
                    html.P([
                        "O ",
                        html.Strong("Monitor Público"),
                        " é um projeto ",
                        html.Strong("open source"),
                        " desenvolvido com o objetivo de promover a ",
                        html.Strong("transparência"),
                        " e o ",
                        html.Strong("controle social"),
                        " das despesas pagas pelo município de Queimados-RJ."
                    ], style={
                        "fontSize": "14px",
                        "color": COLORS["text"],
                        "lineHeight": "1.7",
                        "marginBottom": "12px"
                    }),
                    html.P(
                        "Através da coleta e análise automatizada dos dados do Portal da Transparência, "
                        "esta ferramenta permite que qualquer cidadão acompanhe os pagamentos efetuados "
                        "pela prefeitura, identificando para onde os recursos públicos estão sendo direcionados.",
                        style={
                            "fontSize": "14px",
                            "color": COLORS["text_light"],
                            "lineHeight": "1.7",
                            "marginBottom": "15px"
                        }
                    ),
                    html.Div([
                        html.Span("Código Aberto", style={
                            "backgroundColor": f"{COLORS['primary']}15",
                            "padding": "6px 14px",
                            "borderRadius": "20px",
                            "fontSize": "12px",
                            "marginRight": "8px",
                            "color": COLORS["primary"],
                            "fontWeight": "600",
                            "border": f"1px solid {COLORS['primary']}30"
                        }),
                        html.Span("Dados Públicos", style={
                            "backgroundColor": f"{COLORS['secondary']}15",
                            "padding": "6px 14px",
                            "borderRadius": "20px",
                            "fontSize": "12px",
                            "marginRight": "8px",
                            "color": COLORS["secondary"],
                            "fontWeight": "600",
                            "border": f"1px solid {COLORS['secondary']}30"
                        }),
                        html.Span("Atualização Automática", style={
                            "backgroundColor": f"{COLORS['accent']}15",
                            "padding": "6px 14px",
                            "borderRadius": "20px",
                            "fontSize": "12px",
                            "color": COLORS["accent"],
                            "fontWeight": "600",
                            "border": f"1px solid {COLORS['accent']}30"
                        }),
                    ], style={"marginTop": "5px"}),
                ], style={"flex": "1"}),
            ], style={
                "display": "flex",
                "alignItems": "flex-start"
            })
        ], style={
            **card_style,
            "marginTop": "20px",
            "background": f"linear-gradient(135deg, {COLORS['card']} 0%, #f8faff 100%)",
            "borderLeft": f"4px solid {COLORS['primary']}"
        }),

        # ========== FOOTER ==========
        html.Div([
            html.Div([
                html.P([
                    "Elaborado por ",
                    html.Strong("Christian Basilio", style={"color": COLORS["primary"]}),
                ], style={
                    "color": COLORS["text_light"],
                    "fontSize": "13px",
                    "margin": "0"
                }),
                html.P([
                    "Monitor Público 2026 • Todos os dados são obtidos do Portal da Transparência de Queimados"
                ], style={
                    "color": COLORS["text_light"],
                    "fontSize": "11px",
                    "margin": "5px 0 0 0",
                    "opacity": "0.8"
                })
            ], style={"textAlign": "center"})
        ], style={
            "marginTop": "30px",
            "paddingTop": "20px",
            "borderTop": f"1px solid {COLORS['border']}"
        })

    ], style={
        "padding": "20px 30px",
        "backgroundColor": COLORS["bg"],
        "minHeight": "calc(100vh - 80px)"
    })

], style={
    "fontFamily": "'Inter', 'Segoe UI', sans-serif",
    "backgroundColor": COLORS["bg"],
    "minHeight": "100vh"
})


# ============================================
# CALLBACKS
# ============================================

@app.callback(
    [
        Output("kpi-total", "children"),
        Output("kpi-media", "children"),
        Output("kpi-registros", "children"),
        Output("grafico-temporal", "figure"),
        Output("grafico-tipo", "figure"),
        Output("grafico-secretarias", "figure"),
        Output("grafico-unidade", "figure"),
        Output("grafico-funcao", "figure"),
        Output("grafico-modalidade", "figure"),
        Output("tabela-container", "children"),
        Output("tabela-detalhada-container", "children"),
    ],
    [
        Input("filtro-ano", "value"),
        Input("filtro-secretaria", "value"),
        Input("filtro-funcao", "value"),
        Input("filtro-fonte", "value"),
    ]
)
def update_dashboard(anos, secretarias, funcoes, fontes):

    df_f = df.copy()

    if anos:
        df_f = df_f[df_f["ano_exercicio"].isin(anos)]
    if secretarias:
        df_f = df_f[df_f["secretaria_padronizada"].isin(secretarias)]
    if funcoes:
        df_f = df_f[df_f["funcao"].isin(funcoes)]
    if fontes and "fonte" in df_f.columns:
        df_f = df_f[df_f["fonte"].isin(fontes)]

    # ========== KPIs ==========
    total = df_f["total_despesa"].sum()
    registros = df_f["total_registros"].sum()

    # Calcular média mensal
    meses_unicos = df_f["ano_mes"].nunique()
    media_mensal = total / meses_unicos if meses_unicos > 0 else 0

    kpi_total = fmt_currency(total)
    kpi_media = fmt_currency(media_mensal)
    kpi_registros = f"{int(registros):,}".replace(",", ".")

    # ========== Gráfico Temporal ==========
    df_temp = df_f.groupby("ano_mes", as_index=False).agg({
        "total_despesa": "sum"
    }).sort_values("ano_mes")

    fig_temporal = go.Figure()
    fig_temporal.add_trace(go.Scatter(
        x=df_temp["ano_mes"], y=df_temp["total_despesa"],
        mode="lines", name="Despesas Pagas",
        line=dict(color=COLORS["primary"], width=3, shape="spline"),
        fill="tozeroy", fillcolor="rgba(91, 110, 225, 0.15)",
        hovertemplate="<b>%{x}</b><br>Despesas Pagas: %{customdata}<extra></extra>",
        customdata=[fmt_number(v) for v in df_temp["total_despesa"]]
    ))
    fig_temporal.update_layout(
        margin=dict(t=10, b=40, l=50, r=20),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=COLORS["text_light"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"], tickfont=dict(size=10, color=COLORS["text_light"])),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
        height=250,
        showlegend=False
    )

    # ========== Gráfico Tipo (Barras) ==========
    df_tipo = df_f.groupby("tipo", as_index=False)["total_despesa"].sum()
    df_tipo["tipo_label"] = df_tipo["tipo"].map({"J": "Pessoa Jurídica", "F": "Pessoa Física"}).fillna(df_tipo["tipo"])
    df_tipo = df_tipo.sort_values("total_despesa", ascending=True)
    df_tipo["valor_fmt"] = df_tipo["total_despesa"].apply(fmt_number)

    fig_tipo = go.Figure(go.Bar(
        x=df_tipo["total_despesa"], y=df_tipo["tipo_label"],
        orientation="h",
        marker=dict(color=[COLORS["primary"], COLORS["secondary"]][:len(df_tipo)], cornerradius=4),
        hovertemplate="<b>%{y}</b><br>Despesas Pagas: %{customdata}<extra></extra>",
        customdata=df_tipo["valor_fmt"]
    ))
    fig_tipo.update_layout(
        margin=dict(t=10, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=11, color=COLORS["text"])),
        height=250
    )

    # ========== Gráfico Secretarias (TODAS com scroll) ==========
    df_sec = df_f.groupby("secretaria_padronizada", as_index=False)["total_despesa"].sum()
    df_sec = df_sec.sort_values("total_despesa", ascending=True)
    df_sec["valor_fmt"] = df_sec["total_despesa"].apply(fmt_number)
    df_sec["label"] = df_sec["total_despesa"].apply(fmt_label)

    altura_sec = max(300, len(df_sec) * 28)

    fig_sec = go.Figure(go.Bar(
        x=df_sec["total_despesa"], y=df_sec["secretaria_padronizada"],
        orientation="h",
        marker=dict(color=COLORS["primary"], cornerradius=4),
        hovertemplate="<b>%{y}</b><br>Despesas Pagas: %{customdata}<extra></extra>",
        text=df_sec["label"],
        textposition="outside",
        textfont=dict(size=9, color=COLORS["text_light"]),
        customdata=df_sec["valor_fmt"]
    ))
    fig_sec.update_layout(
        margin=dict(t=10, b=10, l=10, r=60),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=9, color=COLORS["text_light"])),
        height=altura_sec
    )

    # ========== Gráfico Unidade Orçamentária (com scroll) ==========
    if "unidade_orcamentaria" in df_f.columns:
        df_unid = df_f.groupby("unidade_orcamentaria", as_index=False)["total_despesa"].sum()
        df_unid = df_unid.sort_values("total_despesa", ascending=True)
        df_unid["valor_fmt"] = df_unid["total_despesa"].apply(fmt_number)
        df_unid["label"] = df_unid["total_despesa"].apply(fmt_label)

        altura_unid = max(300, len(df_unid) * 28)

        fig_unid = go.Figure(go.Bar(
            x=df_unid["total_despesa"], y=df_unid["unidade_orcamentaria"],
            orientation="h",
            marker=dict(color=COLORS["secondary"], cornerradius=4),
            hovertemplate="<b>%{y}</b><br>Despesas Pagas: %{customdata}<extra></extra>",
            text=df_unid["label"],
            textposition="outside",
            textfont=dict(size=9, color=COLORS["text_light"]),
            customdata=df_unid["valor_fmt"]
        ))
        fig_unid.update_layout(
            margin=dict(t=10, b=10, l=10, r=60),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, showticklabels=False),
            yaxis=dict(showgrid=False, tickfont=dict(size=9, color=COLORS["text_light"])),
            height=altura_unid
        )
    else:
        fig_unid = go.Figure()
        fig_unid.add_annotation(text="Dados de Unidade Orçamentária não disponíveis",
                                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig_unid.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")

    # ========== Gráfico Função (Barras com scroll) ==========
    df_func = df_f.groupby("funcao", as_index=False)["total_despesa"].sum()
    df_func = df_func.sort_values("total_despesa", ascending=True)
    df_func["valor_fmt"] = df_func["total_despesa"].apply(fmt_number)
    df_func["label"] = df_func["total_despesa"].apply(fmt_label)

    altura_func = max(250, len(df_func) * 28)

    fig_func = go.Figure(go.Bar(
        x=df_func["total_despesa"], y=df_func["funcao"],
        orientation="h",
        marker=dict(color=COLORS["accent"], cornerradius=4),
        hovertemplate="<b>%{y}</b><br>Despesas Pagas: %{customdata}<extra></extra>",
        text=df_func["label"],
        textposition="outside",
        textfont=dict(size=9, color=COLORS["text_light"]),
        customdata=df_func["valor_fmt"]
    ))
    fig_func.update_layout(
        margin=dict(t=10, b=10, l=10, r=60),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=9, color=COLORS["text_light"])),
        height=altura_func
    )

    # ========== Gráfico Modalidade (Barras com scroll) ==========
    df_mod = df_f.groupby("modalidade_licitacao", as_index=False)["total_despesa"].sum()
    df_mod = df_mod.sort_values("total_despesa", ascending=True)
    df_mod["valor_fmt"] = df_mod["total_despesa"].apply(fmt_number)
    df_mod["label"] = df_mod["total_despesa"].apply(fmt_label)

    altura_mod = max(250, len(df_mod) * 28)

    fig_mod = go.Figure(go.Bar(
        x=df_mod["total_despesa"], y=df_mod["modalidade_licitacao"],
        orientation="h",
        marker=dict(color=COLORS["success"], cornerradius=4),
        hovertemplate="<b>%{y}</b><br>Despesas Pagas: %{customdata}<extra></extra>",
        text=df_mod["label"],
        textposition="outside",
        textfont=dict(size=9, color=COLORS["text_light"]),
        customdata=df_mod["valor_fmt"]
    ))
    fig_mod.update_layout(
        margin=dict(t=10, b=10, l=10, r=60),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=9, color=COLORS["text_light"])),
        height=altura_mod
    )

    # ========== Tabela Agregada ==========
    df_tab = df_f.groupby(
        ["secretaria_padronizada", "funcao", "subfuncao"], as_index=False
    )["total_despesa"].sum()
    df_tab = df_tab.sort_values("total_despesa", ascending=False).head(15)
    df_tab = df_tab.rename(columns={"total_despesa": "despesas_pagas"})
    max_val = df_tab["despesas_pagas"].max() if not df_tab.empty else 1

    tabela = dash_table.DataTable(
        data=df_tab.to_dict("records"),
        columns=[
            {"name": "Secretaria", "id": "secretaria_padronizada"},
            {"name": "Função", "id": "funcao"},
            {"name": "Subfunção", "id": "subfuncao"},
            {"name": "Despesas Pagas", "id": "despesas_pagas", "type": "numeric",
             "format": {"specifier": ",.2f", "locale": {"symbol": ["R$ ", ""], "group": ".", "decimal": ","}}},
        ],
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "left", "padding": "12px 15px", "fontSize": "12px",
            "backgroundColor": "transparent", "color": COLORS["text"],
            "border": "none", "borderBottom": f"1px solid {COLORS['border']}"
        },
        style_header={
            "backgroundColor": COLORS["bg"], "color": COLORS["text_light"],
            "fontWeight": "600", "fontSize": "11px", "textTransform": "uppercase",
            "borderBottom": f"2px solid {COLORS['primary']}"
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": COLORS["bg"]}
        ] + [
            {
                "if": {"filter_query": f"{{despesas_pagas}} = {row['despesas_pagas']}", "column_id": "despesas_pagas"},
                "background": f"linear-gradient(90deg, rgba(91, 110, 225, 0.3) {(row['despesas_pagas']/max_val)*100}%, transparent {(row['despesas_pagas']/max_val)*100}%)",
                "fontWeight": "600", "color": COLORS["primary"]
            } for _, row in df_tab.iterrows()
        ],
        page_size=10,
        sort_action="native"
    )

    # ========== Tabela Detalhada (Registros Individuais) ==========
    # Usa a tabela de registros detalhados (não agregados)
    df_det = df_registros.copy()

    # Aplica os mesmos filtros
    if anos:
        df_det = df_det[df_det["ano"].isin(anos)]
    if secretarias:
        df_det = df_det[df_det["secretaria"].isin(secretarias)]
    if funcoes:
        df_det = df_det[df_det["funcao"].isin(funcoes)]
    if fontes and "fonte" in df_det.columns:
        df_det = df_det[df_det["fonte"].isin(fontes)]

    # Colunas para exibição (nomes originais do banco)
    colunas_exibir = [
        "data_despesa", "ano", "secretaria", "funcao", "subfuncao",
        "programa", "elemento_despesa", "despesa_descricao",
        "favorecido", "modalidade_licitacao", "fonte", "tipo", "valor_despesa"
    ]

    # Filtra apenas colunas que existem
    colunas_existentes = [c for c in colunas_exibir if c in df_det.columns]

    # Prepara os dados
    df_detalhado = df_det[colunas_existentes].copy()
    df_detalhado = df_detalhado.sort_values(
        ["data_despesa", "valor_despesa"],
        ascending=[False, False]
    )

    # Formata a data para exibição
    if "data_despesa" in df_detalhado.columns:
        df_detalhado["data_despesa"] = pd.to_datetime(df_detalhado["data_despesa"]).dt.strftime("%d/%m/%Y")

    # Mapeia tipo para nome legível
    if "tipo" in df_detalhado.columns:
        df_detalhado["tipo"] = df_detalhado["tipo"].map({"J": "PJ", "F": "PF"}).fillna(df_detalhado["tipo"])

    # Define as colunas da tabela com nomes amigáveis
    colunas_tabela_detalhada = []
    mapeamento_nomes = {
        "data_despesa": "Data",
        "ano": "Ano",
        "secretaria": "Secretaria",
        "funcao": "Função",
        "subfuncao": "Subfunção",
        "programa": "Programa",
        "elemento_despesa": "Elemento",
        "despesa_descricao": "Descrição",
        "favorecido": "Favorecido",
        "modalidade_licitacao": "Modalidade",
        "fonte": "Fonte",
        "tipo": "Tipo",
        "valor_despesa": "Valor Pago"
    }

    for col in colunas_existentes:
        col_config = {"name": mapeamento_nomes.get(col, col), "id": col}
        if col == "valor_despesa":
            col_config["type"] = "numeric"
            col_config["format"] = {"specifier": ",.2f", "locale": {"symbol": ["R$ ", ""], "group": ".", "decimal": ","}}
        colunas_tabela_detalhada.append(col_config)

    # Contador de registros
    total_registros_tabela = len(df_detalhado)

    tabela_detalhada = html.Div([
        # Info do total de registros
        html.Div([
            html.Span(f"Total: ", style={"color": COLORS["text_light"], "fontSize": "12px"}),
            html.Span(f"{total_registros_tabela:,}".replace(",", "."), style={
                "color": COLORS["primary"],
                "fontWeight": "700",
                "fontSize": "14px"
            }),
            html.Span(" registros", style={"color": COLORS["text_light"], "fontSize": "12px"})
        ], style={"marginBottom": "10px"}),

        # Tabela
        dash_table.DataTable(
            data=df_detalhado.to_dict("records"),
            columns=colunas_tabela_detalhada,
            style_table={"overflowX": "auto", "minWidth": "100%"},
            style_cell={
                "textAlign": "left",
                "padding": "8px 10px",
                "fontSize": "10px",
                "backgroundColor": "transparent",
                "color": COLORS["text"],
                "border": "none",
                "borderBottom": f"1px solid {COLORS['border']}",
                "whiteSpace": "normal",
                "height": "auto",
                "minWidth": "70px",
                "maxWidth": "180px"
            },
            style_cell_conditional=[
                {"if": {"column_id": "secretaria"}, "minWidth": "120px", "maxWidth": "200px"},
                {"if": {"column_id": "despesa_descricao"}, "minWidth": "120px", "maxWidth": "250px"},
                {"if": {"column_id": "favorecido"}, "minWidth": "120px", "maxWidth": "200px"},
                {"if": {"column_id": "data_despesa"}, "minWidth": "80px", "maxWidth": "90px"},
                {"if": {"column_id": "ano"}, "minWidth": "45px", "maxWidth": "55px"},
                {"if": {"column_id": "tipo"}, "minWidth": "35px", "maxWidth": "45px"},
                {"if": {"column_id": "valor_despesa"}, "minWidth": "90px", "maxWidth": "120px", "fontWeight": "600"},
            ],
            style_header={
                "backgroundColor": COLORS["primary"],
                "color": "white",
                "fontWeight": "600",
                "fontSize": "9px",
                "textTransform": "uppercase",
                "letterSpacing": "0.5px",
                "padding": "10px 8px"
            },
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#f8fafc"},
                {"if": {"column_id": "valor_despesa"}, "color": COLORS["primary"]}
            ],
            page_size=5,
            page_action="native",
            sort_action="native",
            sort_mode="multi",
            filter_action="native",
            filter_options={"placeholder_text": "Filtrar..."},
        )
    ])

    return (
        kpi_total, kpi_media, kpi_registros,
        fig_temporal, fig_tipo, fig_sec, fig_unid, fig_func, fig_mod, tabela, tabela_detalhada
    )


# ============================================
# SERVIDOR PARA DEPLOY (Render/Heroku/etc)
# ============================================

server = app.server

# ============================================
# EXECUTAR
# ============================================

if __name__ == "__main__":
    print("🚀 Dashboard Executivo - Monitor Público")
    print("📊 Acesse: http://localhost:8050")
    print("👤 Elaborado por Christian Basilio")
    app.run(debug=True, host="0.0.0.0", port=8050)
