import dash
from dash import dcc, html, dash_table, ctx
from dash.dependencies import Input, Output, State, ALL
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import dash_bootstrap_components as dbc
import re
import joblib

# =============================================================================
# 0. CONFIGURAÇÕES GLOBAIS E CARREGAMENTO DE MODELOS
# =============================================================================

# --- Carregar o modelo de ML de Produtividade ---
try:
    modelo_produtividade = joblib.load('yield_prediction_model.joblib')
    df_ml_dataset = pd.read_csv('ml_dataset_produtividade.csv')
    print("Modelo de previsão de produtividade carregado com sucesso.")
except FileNotFoundError:
    print("AVISO: Arquivo do modelo de produtividade não encontrado. O simulador de IA será desabilitado.")
    modelo_produtividade = None
    df_ml_dataset = pd.DataFrame()

# --- Carregar os dados de Previsão de Preços ---
try:
    df_previsao_precos = pd.read_csv('previsao_precos_mercado.csv')
    df_previsao_precos['ds'] = pd.to_datetime(df_previsao_precos['ds'])
    print("Arquivo de previsão de preços carregado com sucesso.")
except FileNotFoundError:
    df_previsao_precos = pd.DataFrame()
    print("AVISO: Arquivo 'previsao_precos_mercado.csv' não encontrado. As previsões de preço não estarão disponíveis.")


OPENWEATHER_API_KEY = "cd679ff6e2bb5bc8b49cb85755d617f4"
DEFAULT_LATITUDE = -13.05
DEFAULT_LONGITUDE = -55.9
DEFAULT_CITY_NAME = "Lucas do Rio Verde, BR"

PRECOS_VENDA = {
    "Soja": 1.10,
    "Milho": 0.85,
    "Algodão": 8.50
}

# =============================================================================
# 1. CARREGAMENTO E PREPARAÇÃO DOS DADOS
# =============================================================================

engine = create_engine("sqlite:///gestao_agricola.db")
query_completa = """
SELECT
    f.nome as fazenda, t.id as talhao_id, t.identificador as talhao, t.area_ha, s.id as safra_id,
    s.data_plantio, s.data_colheita_real, s.produtividade_kg_ha, c.nome as cultura,
    a.tipo_atividade, a.produto_utilizado, a.quantidade_aplicada_ha, a.unidade,
    a.data_execucao, a.custo_total_ha, a.operador, m.nome as maquina
FROM fazendas f
JOIN talhoes t ON f.id = t.fazenda_id
JOIN safras s ON t.id = s.talhao_id
JOIN culturas c ON s.cultura_id = c.id
LEFT JOIN atividades_agricolas a ON s.id = a.safra_id
LEFT JOIN maquinas m ON a.maquina_id = m.id
WHERE s.produtividade_kg_ha IS NOT NULL ORDER BY t.identificador, s.data_plantio;
"""
df_completo = pd.read_sql(query_completa, engine)
for col in ['data_plantio', 'data_colheita_real', 'data_execucao']:
    df_completo[col] = pd.to_datetime(df_completo[col], errors='coerce')
df_completo['season'] = df_completo['data_plantio'].dt.month.apply(lambda x: 'Season A (Safrinha)' if x <= 6 else 'Season B (Verão)')
df_completo['ano_safra_num'] = df_completo['data_plantio'].dt.year

custo_por_safra = df_completo.groupby('safra_id')['custo_total_ha'].sum().reset_index().rename(columns={'custo_total_ha': 'custo_total_safra_ha'})
df_agricola = pd.merge(df_completo.drop_duplicates(subset=['safra_id']), custo_por_safra, on='safra_id', how='left')
df_agricola['receita_ha_potencial'] = df_agricola['produtividade_kg_ha'] * df_agricola['cultura'].map(PRECOS_VENDA)
df_agricola['lucro_ha_potencial'] = df_agricola['receita_ha_potencial'] - df_agricola['custo_total_safra_ha']

try:
    df_vendas = pd.read_sql("SELECT * FROM contratos_venda", engine)
    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
    df_agricola_vendas = pd.merge(df_agricola, df_vendas[['safra_id', 'preco_venda_kg', 'quantidade_kg']], on='safra_id', how='left')
    df_agricola_vendas['receita_realizada_ha'] = (df_agricola_vendas['quantidade_kg'] * df_agricola_vendas['preco_venda_kg']) / df_agricola_vendas['area_ha']
    df_agricola_vendas['lucro_realizado_ha'] = df_agricola_vendas['receita_realizada_ha'] - df_agricola_vendas['custo_total_safra_ha']
    df_agricola['lucro_ha'] = df_agricola_vendas['lucro_realizado_ha'].fillna(df_agricola['lucro_ha_potencial'])
except Exception:
    df_agricola['lucro_ha'] = df_agricola['lucro_ha_potencial']
    df_vendas = pd.DataFrame()

df_agricola['ano_safra'] = df_agricola['ano_safra_num'].astype(str)

try:
    df_precos_mercado = pd.read_sql("SELECT * FROM precos_mercado", engine)
    df_precos_mercado['data'] = pd.to_datetime(df_precos_mercado['data'])
except Exception as e:
    df_precos_mercado = pd.DataFrame()

min_date_allowed = min(df_vendas['data_venda'].min(), df_precos_mercado['data'].min()) if not df_vendas.empty and not df_precos_mercado.empty else datetime.now() - timedelta(days=365*5)
max_date_allowed = max(df_vendas['data_venda'].max(), df_precos_mercado['data'].max()) if not df_vendas.empty and not df_precos_mercado.empty else datetime.now()
start_date_default = max_date_allowed - relativedelta(months=12)


try:
    df_solo_raw = pd.read_sql("SELECT * FROM analises_solo", engine)
    df_solo_raw['data_analise'] = pd.to_datetime(df_solo_raw['data_analise'])
    df_agricola_sorted = df_agricola.sort_values('data_plantio')
    df_solo_sorted = df_solo_raw.sort_values('data_analise')
    df_agricola = pd.merge_asof(
        df_agricola_sorted, df_solo_sorted,
        left_on='data_plantio', right_on='data_analise',
        by='talhao_id', direction='backward'
    )
except Exception as e:
    pass

try:
    df_clima = pd.read_csv('Dados_Climaticos_INMET.csv')
    print("Dados de 'Dados_Climaticos_INMET.csv' carregados com sucesso.")
    df_clima.rename(columns={'PRECIPITACAO_TOTAL_HORARIO_mm': 'precipitacao_mm', 'TEMPERATURA_AR_BULBO_SECO_HORARIA_C': 'temperatura_c'}, inplace=True)
    df_clima['DATETIME'] = pd.to_datetime(df_clima['DATETIME'])
    df_clima['ano'] = df_clima['DATETIME'].dt.year
    df_clima['mes'] = df_clima['DATETIME'].dt.month
    df_clima_anual_geral = df_clima.groupby('ano').agg(precipitacao_mm=('precipitacao_mm', 'sum'), temp_max=('temperatura_c', 'max'), temp_min=('temperatura_c', 'min'), temp_media=('temperatura_c', 'mean')).reset_index()
except Exception as e:
    print(f"Aviso: Não foi possível carregar 'Dados_Climaticos_INMET.csv'. Erro: {e}")
    df_clima = pd.DataFrame()


try:
    df_oni = pd.read_csv('oni_data.csv')
    df_agricola['ano'] = df_agricola['data_plantio'].dt.year
    df_agricola['mes'] = df_agricola['data_plantio'].dt.month
    df_agricola = pd.merge(df_agricola, df_oni, on=['ano', 'mes'], how='left')
    df_agricola.drop(columns=['ano', 'mes'], inplace=True)
    print("Dados de ENOS (El Niño/La Niña) integrados com sucesso.")
except FileNotFoundError:
    print("Aviso: Arquivo oni_data.csv não encontrado.")
    df_agricola['fase_enos'] = 'Não Disponível'


# =============================================================================
# 2. ESTILOS E INICIALIZAÇÃO DO APP
# =============================================================================
colors = {'background': '#1E1E1E', 'text': '#FFFFFF', 'grid': '#333333', 'primary': '#00AEEF', 'card_background': '#252525', 'user_message_bg': '#00AEEF', 'bot_message_bg': '#333333'}
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY, dbc.icons.FONT_AWESOME], suppress_callback_exceptions=True)
app.title = 'Dashboard Agrícola'
server = app.server

# =============================================================================
# 3. FUNÇÕES AUXILIARES E LAYOUTS
# =============================================================================
def get_coords_for_city(city_name: str):
    if OPENWEATHER_API_KEY == "SUA_CHAVE_DE_API_VAI_AQUI":
        return None, "API Key não configurada."
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},BR&limit=1&appid={OPENWEATHER_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data:
            location = data[0]
            full_name = f"{location.get('name', '')}, {location.get('state', '')}"
            return (location['lat'], location['lon'], full_name), None
        return None, f"Cidade '{city_name}' não encontrada."
    except requests.exceptions.RequestException as e:
        return None, f"Erro de conexão com a API de geocoding: {e}"

def get_weather_forecast(lat, lon):
    if OPENWEATHER_API_KEY == "SUA_CHAVE_DE_API_VAI_AQUI":
        return None, "Por favor, insira uma chave de API da OpenWeatherMap no início do script."
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric&lang=pt_br"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.RequestException as e:
        return None, f"Erro de conexão com a API de meteorologia: {e}"

def create_mini_figure():
    return go.Figure().update_layout(
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=0, b=0), height=80,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )

filtros_gerais_horizontal = dbc.Row([
    dbc.Col([
        html.Label('Ano da Safra:', style={'fontWeight': 'bold', 'fontSize': '0.9rem'}),
        dcc.Dropdown(id='filtro-ano', options=[{'label': 'Todos', 'value': 'todos'}] + [{'label': str(ano), 'value': ano} for ano in sorted(df_agricola['ano_safra'].unique())])
    ], md=3),
    dbc.Col([
        html.Label('Season:', style={'fontWeight': 'bold', 'fontSize': '0.9rem'}),
        dcc.Dropdown(id='filtro-season', options=[{'label': 'Todas', 'value': 'todas'}, {'label': 'Season A (Safrinha)', 'value': 'Season A (Safrinha)'}, {'label': 'Season B (Verão)', 'value': 'Season B (Verão)'}])
    ], md=3),
    dbc.Col([
        html.Label('Fazenda:', style={'fontWeight': 'bold', 'fontSize': '0.9rem'}),
        dcc.Dropdown(id='filtro-fazenda', options=[{'label': 'Todas', 'value': 'todas'}] + [{'label': f, 'value': f} for f in df_agricola['fazenda'].unique()])
    ], md=3),
    dbc.Col([
        html.Label('Cultura:', style={'fontWeight': 'bold', 'fontSize': '0.9rem'}),
        dcc.Dropdown(id='filtro-cultura', options=[{'label': 'Todas', 'value': 'todas'}] + [{'label': c, 'value': c} for c in df_agricola['cultura'].unique()])
    ], md=3)
], className="g-2")

filtros_gerais_vertical = html.Div([
    html.H3('Filtros', style={'textAlign': 'center'}),
    html.Label('Ano da Safra:', style={'fontWeight': 'bold'}),
    dcc.Dropdown(id='filtro-ano', options=[{'label': 'Todos', 'value': 'todos'}] + [{'label': str(ano), 'value': ano} for ano in sorted(df_agricola['ano_safra'].unique())]),
    html.Br(),
    html.Label('Season:', style={'fontWeight': 'bold'}),
    dcc.Dropdown(id='filtro-season', options=[{'label': 'Todas', 'value': 'todas'}, {'label': 'Season A (Safrinha)', 'value': 'Season A (Safrinha)'}, {'label': 'Season B (Verão)', 'value': 'Season B (Verão)'}]),
    html.Br(),
    html.Label('Fazenda:', style={'fontWeight': 'bold'}),
    dcc.Dropdown(id='filtro-fazenda', options=[{'label': 'Todas', 'value': 'todas'}] + [{'label': f, 'value': f} for f in df_agricola['fazenda'].unique()]),
    html.Br(),
    html.Label('Cultura:', style={'fontWeight': 'bold'}),
    dcc.Dropdown(id='filtro-cultura', options=[{'label': 'Todas', 'value': 'todas'}] + [{'label': c, 'value': c} for c in df_agricola['cultura'].unique()]),
])

filtros_clima = html.Div([
    html.H3('Filtros Climáticos', style={'textAlign': 'center'}),
    html.Label('Ano:'),
    dcc.Dropdown(id='filtro-ano-clima', options=[{'label': str(ano), 'value': ano} for ano in sorted(df_clima['ano'].unique(), reverse=True)] if not df_clima.empty else [], value=df_clima['ano'].max() if not df_clima.empty else None),
])

sidebar = html.Div(
    id='sidebar',
    style={'position': 'fixed', 'width': '250px', 'height': '100%', 'padding': '20px', 'backgroundColor': colors['card_background'], 'overflowY': 'auto'},
    children=[
        html.H2(html.A('Análises', href='/', style={'textDecoration': 'none', 'color': 'inherit'})),
        html.Hr(),
        dbc.Nav([
            dbc.NavLink('Painel de Controle', href='/', active="exact"),
            dbc.NavLink('Visão Agrícola', href='/agricola', active="exact"),
            dbc.NavLink('Análise de Risco', href='/risco', active="exact"),
            dbc.NavLink('Análise de Talhões', href='/talhoes', active="exact"),
            dbc.NavLink('Análise de Solo', href='/solo', active="exact"),
            dbc.NavLink('Análise Operacional', href='/operacional', active="exact"),
            dbc.NavLink('Análise Climática', href='/clima', active="exact"),
            dbc.NavLink('Cenários Climáticos (ENOS)', href='/enos', active="exact"),
            dbc.NavLink('Previsão com IA', href='/ia', active="exact"),
            dbc.NavLink('Simulador de Safra (IA)', href='/ia-predicao', active="exact", className="mt-2 border border-primary rounded"),
        ], vertical=True, pills=True),
        html.Hr(),
        html.Div(id='filtros-container')
    ]
)

layout_painel_principal = html.Div([
    dbc.Container(html.H1('Painel de Controle Principal', className="display-4 text-center py-3"), fluid=True),
    html.Div(children=filtros_gerais_horizontal, style={'padding': '10px 20px'}),
    html.Hr(),
    dbc.Row([
        dbc.Col(dbc.Card([dbc.CardHeader("🌾 Desempenho Agrícola"), dbc.CardBody([html.H4(id='kpi-prod-media', className="card-title"), html.P(id='kpi-area-total', className="card-subtitle"), dcc.Graph(id='mini-grafico-prod-cultura', figure=create_mini_figure())], className="text-center"), dbc.CardFooter(dbc.Button("Ver Análise Detalhada", href="/agricola", color="primary", outline=True, size="sm", className="w-100"))]), lg=4, md=6, className="mb-4"),
        dbc.Col(dbc.Card([dbc.CardHeader("💰 Análise Financeira"), dbc.CardBody([html.H4(id='kpi-lucro-medio', className="card-title"), html.P(id='kpi-custo-medio', className="card-subtitle"), dcc.Graph(id='mini-grafico-lucro-evolucao', figure=create_mini_figure())], className="text-center"), dbc.CardFooter(dbc.Button("Ver Análise Detalhada", href="/risco", color="primary", outline=True, size="sm", className="w-100"))]), lg=4, md=6, className="mb-4"),
        dbc.Col(dbc.Card([dbc.CardHeader("📈 Risco & Mercado"), dbc.CardBody([html.H4(id='kpi-custo-oportunidade', className="card-title"), html.P(id='kpi-preco-mercado-soja', className="card-subtitle"), dcc.Graph(id='mini-grafico-mercado', figure=create_mini_figure())], className="text-center"), dbc.CardFooter(dbc.Button("Ver Análise Detalhada", href="/risco", color="primary", outline=True, size="sm", className="w-100"))]), lg=4, md=6, className="mb-4"),
        dbc.Col(dbc.Card([dbc.CardHeader("🌱 Saúde do Solo"), dbc.CardBody([html.H4(id='kpi-ph-medio', className="card-title"), html.P(id='kpi-fosforo-medio', className="card-subtitle"), dcc.Graph(id='mini-grafico-solo', figure=create_mini_figure())], className="text-center"), dbc.CardFooter(dbc.Button("Ver Análise Detalhada", href="/solo", color="primary", outline=True, size="sm", className="w-100"))]), lg=4, md=6, className="mb-4"),
        dbc.Col(dbc.Card([dbc.CardHeader("🚜 Eficiência Operacional"), dbc.CardBody([html.H4(id='kpi-maquina-top-custo', className="card-title"), html.P(id='kpi-operador-top-prod', className="card-subtitle"), dcc.Graph(id='mini-grafico-custo-atividade', figure=create_mini_figure())], className="text-center"), dbc.CardFooter(dbc.Button("Ver Análise Detalhada", href="/operacional", color="primary", outline=True, size="sm", className="w-100"))]), lg=4, md=6, className="mb-4"),
        dbc.Col(dbc.Card([dbc.CardHeader("🌦️ Clima & Previsão"), dbc.CardBody([html.H4(id='kpi-chuva-anual', className="card-title"), html.P(id='kpi-temp-media-anual', className="card-subtitle"), dcc.Graph(id='mini-grafico-clima', figure=create_mini_figure())], className="text-center"), dbc.CardFooter(dbc.Button("Ver Análise Detalhada", href="/clima", color="primary", outline=True, size="sm", className="w-100"))]), lg=4, md=6, className="mb-4"),
    ])
], style={'padding': '10px'})

layout_agricola = html.Div(children=[
    html.H1('Visão Geral - Gestão Agrícola', style={'textAlign': 'center'}),
    html.Div(id='cards-kpi-agricola', style={'display': 'flex', 'justifyContent': 'space-around', 'padding': '20px 0'}),
    html.Div(className='charts-container', children=[
        html.Div(style={'display': 'flex'}, children=[
            dcc.Graph(id='grafico-prod-cultura', style={'width': '50%'}),
            dcc.Graph(id='grafico-prod-fazenda', style={'width': '50%'})
        ]),
        html.Div(children=[dcc.Graph(id='grafico-evolucao-prod')])
    ])
])

layout_risco = html.Div([
    html.H1("Análise de Risco e Mercado", style={'textAlign': 'center'}),
    html.Div(id="cards-kpi-risco", style={'display': 'flex', 'justifyContent': 'space-around', 'padding': '10px 0'}),
    dbc.Row([
        dbc.Col([
            html.Label("Selecione o Período:", style={'fontWeight': 'bold'}),
            dcc.DatePickerRange(
                id='date-picker-risco',
                min_date_allowed=min_date_allowed,
                max_date_allowed=max_date_allowed,
                initial_visible_month=max_date_allowed,
                start_date=start_date_default,
                end_date=max_date_allowed,
                display_format='DD/MM/YYYY'
            )
        ], width={'size': 6, 'offset': 3}, className="mb-3 text-center")
    ]),
    dcc.Graph(id="grafico-mercado-vendas"),
    html.Hr(),
    dcc.Graph(id="grafico-vendas-boxplot")
])

layout_talhoes = html.Div([
    html.H1('Análise de Desempenho dos Talhões', style={'textAlign': 'center'}),
    html.P('Compare os talhões com maior e menor lucratividade média com base nos filtros selecionados.', style={'textAlign': 'center'}),
    html.Div(id='comparativo-talhoes-container', style={'display': 'flex', 'justifyContent': 'space-around', 'gap': '20px', 'padding': '20px 0'}),
    dbc.Modal(id='modal-detalhes-talhao', size='lg', centered=True, scrollable=True)
])

layout_solo = html.Div([
    html.H1("Correlação: Solo vs. Desempenho", style={'textAlign': 'center'}),
    html.P("Cruze os dados da análise de solo com a lucratividade para identificar padrões.", style={'textAlign': 'center'}),
    dbc.Row([
        dbc.Col([
            html.Label("Selecione o Indicador do Eixo X (Solo):"),
            dcc.Dropdown(id='dropdown-eixo-x-solo', options=[{'label': 'pH do Solo', 'value': 'ph'}, {'label': 'Fósforo (ppm)', 'value': 'fosforo_ppm'}, {'label': 'Potássio (ppm)', 'value': 'potassio_ppm'}, {'label': 'Matéria Orgânica (%)', 'value': 'materia_organica_percent'}], value='ph')
        ], width=6),
        dbc.Col([
            html.Label("Selecione o Indicador do Eixo Y (Resultado):"),
            dcc.Dropdown(id='dropdown-eixo-y-solo', options=[{'label': 'Lucro por Hectare (R$)', 'value': 'lucro_ha'}], value='lucro_ha')
        ], width=6)
    ], style={'marginBottom': '20px'}),
    dcc.Graph(id='grafico-correlacao-solo')
])

# <<< MUDANÇA: Novo layout operacional com painel de alertas e tabela >>>
layout_operacional = html.Div([
    html.H1("Análise de Eficiência Operacional", style={'textAlign': 'center'}),
    dbc.Row([
        dbc.Col(dcc.Graph(id='grafico-custo-maquina'), width=6),
        dbc.Col(dcc.Graph(id='grafico-prod-operador'), width=6)
    ]),
    html.Hr(className="my-4"),
    dbc.Row([
        dbc.Col([
            html.H4("Painel de Alertas de Anomalias", className="text-center"),
            html.P("Operações com custos fora do padrão histórico (±2 desvios padrão) são sinalizadas abaixo.", className="text-center text-muted"),
            dcc.Loading(
                id="loading-anomalias",
                children=[
                    html.Div(id='alert-panel-operacional', className="mb-4"),
                    dash_table.DataTable(
                        id='anomaly-table-operacional',
                        style_cell={'textAlign': 'left', 'backgroundColor': colors['card_background'], 'color': colors['text']},
                        style_header={
                            'backgroundColor': colors['primary'],
                            'fontWeight': 'bold',
                            'color': 'white'
                        },
                        style_data={
                            'border': f"1px solid {colors['grid']}"
                        },
                        style_table={'overflowX': 'auto'}
                    )
                ],
                type="default"
            )
        ])
    ])
])

if not df_clima.empty:
    fig_clima_prec_anual_geral = px.bar(df_clima_anual_geral, x='ano', y='precipitacao_mm', title='Visão Geral: Precipitação Total por Ano (mm)', text_auto='.0f')
    fig_clima_prec_anual_geral.update_layout(plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], xaxis=dict(gridcolor=colors['grid']), yaxis=dict(gridcolor=colors['grid']))
    fig_clima_prec_anual_geral.update_traces(marker_color=colors['primary'])
    fig_clima_temp_anual_geral = go.Figure()
    fig_clima_temp_anual_geral.add_trace(go.Scatter(x=df_clima_anual_geral['ano'], y=df_clima_anual_geral['temp_max'], name='Temp. Máxima', mode='lines', line=dict(color='red')))
    fig_clima_temp_anual_geral.add_trace(go.Scatter(x=df_clima_anual_geral['ano'], y=df_clima_anual_geral['temp_media'], name='Temp. Média', mode='lines', line=dict(color='orange')))
    fig_clima_temp_anual_geral.add_trace(go.Scatter(x=df_clima_anual_geral['ano'], y=df_clima_anual_geral['temp_min'], name='Temp. Mínima', mode='lines', line=dict(color='lightblue')))
    fig_clima_temp_anual_geral.update_layout(title='Visão Geral: Temperaturas Anuais (°C)', plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], xaxis=dict(gridcolor=colors['grid']), yaxis=dict(gridcolor=colors['grid']), yaxis_title='Temperatura (°C)')
else:
    fig_clima_prec_anual_geral, fig_clima_temp_anual_geral = go.Figure(), go.Figure()

layout_clima = html.Div(children=[
    html.H1('Análise de Dados Climáticos - INMET', style={'textAlign': 'center'}),
    html.Div(id='cards-kpi-clima', style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', 'padding': '20px 0'}),
    html.Div(className='charts-container', children=[
        dcc.Graph(id='grafico-clima-mensal'),
        html.Hr(style={'borderColor': colors['grid']}),
        html.Div(style={'display': 'flex'}, children=[
            dcc.Graph(figure=fig_clima_prec_anual_geral, style={'width': '50%'}),
            dcc.Graph(figure=fig_clima_temp_anual_geral, style={'width': '50%'})
        ])
    ])
])

layout_enos = html.Div([
    html.H1("Análise de Produtividade por Cenário ENOS", style={'textAlign': 'center'}),
    html.P("Compare a produtividade histórica das culturas em anos de El Niño, La Niña e condições Neutras.", style={'textAlign': 'center'}),
    dcc.Graph(id='grafico-enos-boxplot'),
    html.Hr(),
    dcc.Graph(id='grafico-enos-temporal')
])

layout_ia = html.Div([
    html.H1('Chatbot de Previsão do Tempo', style={'textAlign': 'center'}),
    html.Div(id='chat-display', style={'height': '65vh', 'overflowY': 'auto', 'padding': '10px', 'borderRadius': '5px', 'border': 'none'}),
    dcc.Loading(id="loading-ia", type="default", children=html.Div(id="loading-output-ia"), color=colors['primary']),
    html.Div([
        dcc.Input(id='chat-input', placeholder='Pergunte sobre a previsão do tempo...', style={'width': '85%', 'padding': '10px', 'marginRight': '1%'}, n_submit=0),
        html.Button('Enviar', id='send-button', n_clicks=0, style={'width': '14%'})
    ], style={'display': 'flex', 'marginTop': '10px'})
])

def create_input_control(label, control_id, control_type, options=None, min_val=0, max_val=100, step=1, value=50):
    if control_type == 'dropdown':
        control = dcc.Dropdown(id=control_id, options=options, value=options[0]['value'], style={'color': '#000'})
    else:
        control = dcc.Slider(id=control_id, min=min_val, max=max_val, step=step, value=value, marks=None, tooltip={"placement": "bottom", "always_visible": True})
    return dbc.Col([html.Label(label, style={'fontWeight': 'bold'}), control], md=4)

if modelo_produtividade is not None:
    importances = modelo_produtividade.named_steps['regressor'].feature_importances_
    ohe_features = modelo_produtividade.named_steps['preprocessor'].named_transformers_['cat'].get_feature_names_out(['cultura', 'fase_enos'])
    numeric_features = df_ml_dataset.drop(columns=['produtividade_kg_ha', 'cultura', 'fase_enos']).columns
    feature_names = np.concatenate([ohe_features, numeric_features])
    df_importance = pd.DataFrame({'Feature': feature_names, 'Importance': importances}).sort_values('Importance', ascending=True)
    fig_importance = px.bar(df_importance, x='Importance', y='Feature', orientation='h', title='Importância de Cada Fator para o Modelo')
    fig_importance.update_layout(plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'])
    layout_simulador_ia = html.Div([
        html.H1("Simulador de Produtividade com IA", style={'textAlign': 'center'}),
        html.P("Ajuste os parâmetros abaixo para simular as condições da safra e veja a previsão de produtividade do modelo.", style={'textAlign': 'center'}),
        dbc.Row([
            dbc.Col([
                html.H4("Parâmetros de Simulação", style={'textAlign': 'center'}),
                html.Hr(),
                dbc.Row([
                    create_input_control('Cultura', 'sim-cultura', 'dropdown', options=[{'label': c, 'value': c} for c in df_ml_dataset['cultura'].unique()]),
                    create_input_control('Fase ENOS', 'sim-enos', 'dropdown', options=[{'label': e, 'value': e} for e in df_ml_dataset['fase_enos'].unique()]),
                    create_input_control('Área (ha)', 'sim-area', 'slider', min_val=round(df_ml_dataset['area_ha'].min(),0), max_val=round(df_ml_dataset['area_ha'].max(),0), value=round(df_ml_dataset['area_ha'].mean(),0), step=1),
                ], className="mb-3"),
                dbc.Row([
                    create_input_control('pH do Solo', 'sim-ph', 'slider', min_val=df_ml_dataset['ph'].min(), max_val=df_ml_dataset['ph'].max(), value=round(df_ml_dataset['ph'].mean(),2), step=0.1),
                    create_input_control('Fósforo (ppm)', 'sim-fosforo', 'slider', min_val=round(df_ml_dataset['fosforo_ppm'].min(),0), max_val=round(df_ml_dataset['fosforo_ppm'].max(),0), value=round(df_ml_dataset['fosforo_ppm'].mean(),0), step=1),
                    create_input_control('Potássio (ppm)', 'sim-potassio', 'slider', min_val=round(df_ml_dataset['potassio_ppm'].min(),0), max_val=round(df_ml_dataset['potassio_ppm'].max(),0), value=round(df_ml_dataset['potassio_ppm'].mean(),0), step=5),
                ], className="mb-3"),
                dbc.Row([
                    create_input_control('Matéria Org. (%)', 'sim-materia-org', 'slider', min_val=df_ml_dataset['materia_organica_percent'].min(), max_val=df_ml_dataset['materia_organica_percent'].max(), value=round(df_ml_dataset['materia_organica_percent'].mean(),2), step=0.1),
                    create_input_control('Chuva no Ciclo (mm)', 'sim-chuva', 'slider', min_val=round(df_ml_dataset['precipitacao_total_ciclo'].min(),0), max_val=round(df_ml_dataset['precipitacao_total_ciclo'].max(),0), value=round(df_ml_dataset['precipitacao_total_ciclo'].mean(),0), step=50),
                ])
            ], md=8),
            dbc.Col(dbc.Card([dbc.CardHeader("Resultado da Previsão"), dbc.CardBody([html.H2("Produtividade Estimada", className="card-title text-center"), html.H1(id='resultado-previsao', className="card-text text-center text-primary", style={'fontSize': '4rem'}), html.P("kg/ha", className="text-center")])], className="h-100"), md=4)
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(dcc.Graph(id='grafico-contexto-previsao'), md=6),
            dbc.Col(dcc.Graph(figure=fig_importance), md=6)
        ])
    ])
else:
    layout_simulador_ia = html.Div([
        dbc.Alert("O modelo de Machine Learning não foi carregado. Execute o script 'train_and_save_model.py' e reinicie o dashboard.", color="danger")
    ])

app.layout = html.Div(style={'backgroundColor': colors['background'], 'color': colors['text'], 'fontFamily': 'Arial'}, children=[
    dcc.Store(id='chat-history-store', data=[]),
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='filtro-ano-store', data='todos'),
    dcc.Store(id='filtro-season-store', data='todos'),
    dcc.Store(id='filtro-fazenda-store', data='todos'),
    dcc.Store(id='filtro-cultura-store', data='todos'),
    sidebar,
    html.Div(id='page-content', style={'marginLeft': '270px', 'padding': '20px'})
])

# =============================================================================
# 4. CALLBACKS
# =============================================================================

# ... (outros callbacks permanecem inalterados)
@app.callback(
    [Output('page-content', 'children'), Output('filtros-container', 'children'), Output('sidebar', 'style'), Output('page-content', 'style')],
    [Input('url', 'pathname')]
)
def display_page_and_filters(pathname):
    sidebar_style_visible = {'position': 'fixed', 'width': '250px', 'height': '100%', 'padding': '20px', 'backgroundColor': colors['card_background'], 'overflowY': 'auto'}
    content_style_with_sidebar = {'marginLeft': '270px', 'padding': '20px'}
    sidebar_style_hidden = {'display': 'none'}
    content_style_full_width = {'marginLeft': '0px', 'padding': '20px'}
    paginas_com_filtros_gerais = {'/agricola': layout_agricola, '/risco': layout_risco, '/talhoes': layout_talhoes, '/solo': layout_solo, '/operacional': layout_operacional, '/enos': layout_enos}

    if pathname == '/ia-predicao': return layout_simulador_ia, [], sidebar_style_visible, content_style_with_sidebar
    if pathname == '/': return layout_painel_principal, [], sidebar_style_hidden, content_style_full_width
    if pathname in paginas_com_filtros_gerais: return paginas_com_filtros_gerais[pathname], filtros_gerais_vertical, sidebar_style_visible, content_style_with_sidebar
    elif pathname == '/clima': return layout_clima, filtros_clima, sidebar_style_visible, content_style_with_sidebar
    elif pathname == '/ia': return layout_ia, [], sidebar_style_visible, content_style_with_sidebar
    return layout_painel_principal, [], sidebar_style_hidden, content_style_full_width

def create_sync_callback(filter_id):
    @app.callback(Output(f'{filter_id}-store', 'data'), Input(filter_id, 'value'), prevent_initial_call=True)
    def update_store(value): return value if value is not None else 'todos'
    @app.callback(Output(filter_id, 'value', allow_duplicate=True), Input('url', 'pathname'), State(f'{filter_id}-store', 'data'), prevent_initial_call=True)
    def update_dropdown_from_store(pathname, data): return data
for filtro in ['filtro-ano', 'filtro-season', 'filtro-fazenda', 'filtro-cultura']: create_sync_callback(filtro)

@app.callback(
    [Output('resultado-previsao', 'children'), Output('grafico-contexto-previsao', 'figure')],
    [Input('sim-cultura', 'value'), Input('sim-enos', 'value'), Input('sim-area', 'value'), Input('sim-ph', 'value'), Input('sim-fosforo', 'value'), Input('sim-potassio', 'value'), Input('sim-materia-org', 'value'), Input('sim-chuva', 'value')]
)
def update_prediction(cultura, enos, area, ph, fosforo, potassio, materia_org, chuva):
    if modelo_produtividade is None:
        return "N/A", go.Figure().update_layout(title_text="Modelo de IA não carregado", paper_bgcolor=colors['background'], plot_bgcolor=colors['card_background'], font_color=colors['text'])
    input_data = pd.DataFrame({
        'cultura': [cultura], 'area_ha': [area], 'ph': [ph], 'fosforo_ppm': [fosforo], 'potassio_ppm': [potassio],
        'materia_organica_percent': [materia_org], 'fase_enos': [enos], 'precipitacao_total_ciclo': [chuva],
        'temperatura_media_ciclo': [df_ml_dataset['temperatura_media_ciclo'].mean()],
        'temperatura_max_ciclo': [df_ml_dataset['temperatura_max_ciclo'].mean()],
        'dias_calor_extremo_ciclo': [df_ml_dataset['dias_calor_extremo_ciclo'].mean()]
    })
    input_data = input_data[df_ml_dataset.drop(columns='produtividade_kg_ha').columns]
    predicao = modelo_produtividade.predict(input_data)[0]
    df_cultura_historico = df_ml_dataset[df_ml_dataset['cultura'] == cultura]
    fig_contexto = go.Figure()
    fig_contexto.add_trace(go.Histogram(x=df_cultura_historico['produtividade_kg_ha'], name='Dados Históricos', marker_color='#333333'))
    fig_contexto.add_vline(x=predicao, line_width=3, line_dash="dash", line_color=colors['primary'], annotation_text="Sua Previsão", annotation_position="top left")
    fig_contexto.update_layout(title=f'Previsão vs. Histórico para {cultura}', xaxis_title='Produtividade (kg/ha)', yaxis_title='Frequência (Nº de Safras)', plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'])
    return f"{predicao:,.0f}", fig_contexto

@app.callback(
    [Output('grafico-enos-boxplot', 'figure'), Output('grafico-enos-temporal', 'figure')],
    [Input('filtro-ano-store', 'data'), Input('filtro-season-store', 'data'), Input('filtro-fazenda-store', 'data'), Input('filtro-cultura-store', 'data')]
)
def update_enos_analysis(ano, season, fazenda, cultura):
    dff = df_agricola.copy()
    if ano is not None and ano != 'todos': dff = dff[dff['ano_safra'] == ano]
    if season is not None and season != 'todos': dff = dff[dff['season'] == season]
    if fazenda is not None and fazenda != 'todos': dff = dff[dff['fazenda'] == fazenda]
    if cultura is not None and cultura != 'todos': dff = dff[dff['cultura'] == cultura]
    dff = dff[dff['fase_enos'] != 'Não Disponível'].dropna(subset=['produtividade_kg_ha'])
    if dff.empty:
        fig_box = go.Figure().update_layout(title='Dados insuficientes para exibir o gráfico', plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'])
        fig_temporal = go.Figure().update_layout(title='Dados insuficientes para exibir o gráfico', plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'])
        return fig_box, fig_temporal
    fig_box = px.box(dff, x='cultura', y='produtividade_kg_ha', color='fase_enos', title='Distribuição da Produtividade por Cultura e Cenário ENOS', labels={'produtividade_kg_ha': 'Produtividade (kg/ha)', 'cultura': 'Cultura', 'fase_enos': 'Cenário Climático'}, color_discrete_map={'El Nino': '#E74C3C', 'La Nina': '#3498DB', 'Neutro': '#95A5A6'}, category_orders={"fase_enos": ["La Nina", "Neutro", "El Nino"]})
    fig_box.update_layout(plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], legend_title_text='Cenário no Plantio')
    df_temporal = dff.groupby(['ano_safra', 'cultura', 'fase_enos'])['produtividade_kg_ha'].mean().reset_index()
    fig_temporal = px.line(df_temporal, x='ano_safra', y='produtividade_kg_ha', color='cultura', line_dash='fase_enos', markers=True, title="Evolução Anual da Produtividade por Cultura e Cenário ENOS", labels={'produtividade_kg_ha': 'Produtividade Média (kg/ha)', 'ano_safra': 'Ano da Safra', 'fase_enos': 'Cenário Climático', 'cultura': 'Cultura'}, symbol='fase_enos', color_discrete_map={'Soja': '#2ECC71', 'Milho': '#F1C40F', 'Algodão': '#ECF0F1'}, line_dash_map={'El Nino': 'dot', 'La Nina': 'dash', 'Neutro': 'solid'}, category_orders={"fase_enos": ["La Nina", "Neutro", "El Nino"]})
    fig_temporal.update_layout(plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], legend_title_text='Legenda')
    fig_temporal.update_xaxes(type='category')
    return fig_box, fig_temporal

@app.callback(
    [Output('cards-kpi-risco', 'children'), Output('grafico-mercado-vendas', 'figure'), Output('grafico-vendas-boxplot', 'figure')],
    [Input('filtro-ano-store', 'data'),
     Input('filtro-season-store', 'data'),
     Input('filtro-fazenda-store', 'data'),
     Input('filtro-cultura-store', 'data'),
     Input('date-picker-risco', 'start_date'),
     Input('date-picker-risco', 'end_date')]
)
def update_risco_mercado(ano, season, fazenda, cultura, start_date, end_date):
    if df_vendas.empty or df_precos_mercado.empty:
        empty_fig = go.Figure().update_layout(plot_bgcolor=colors['background'], paper_bgcolor=colors['background'], font_color=colors['text'])
        alert = dbc.Alert("Dados de vendas ou de mercado não disponíveis.", color="warning")
        return alert, empty_fig, empty_fig

    dff_vendas = pd.merge(df_agricola, df_vendas.rename(columns={'preco_venda_kg': 'preco_venda_contrato', 'quantidade_kg': 'qtd_vendida'}), on='safra_id', how='inner')
    dff_vendas_filtrado = dff_vendas.copy()

    if ano is not None and ano != 'todos': dff_vendas_filtrado = dff_vendas_filtrado[dff_vendas_filtrado['ano_safra'] == ano]
    if season is not None and season != 'todos': dff_vendas_filtrado = dff_vendas_filtrado[dff_vendas_filtrado['season'] == season]
    if fazenda is not None and fazenda != 'todos': dff_vendas_filtrado = dff_vendas_filtrado[dff_vendas_filtrado['fazenda'] == fazenda]
    if cultura is not None and cultura != 'todos': dff_vendas_filtrado = dff_vendas_filtrado[dff_vendas_filtrado['cultura'] == cultura]
    
    if start_date and end_date:
        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
        dff_vendas_filtrado = dff_vendas_filtrado[
            (dff_vendas_filtrado['data_venda'] >= start_date_dt) & (dff_vendas_filtrado['data_venda'] <= end_date_dt)
        ]

    if dff_vendas_filtrado.empty:
        empty_fig = go.Figure().update_layout(title="Sem dados para os filtros selecionados", plot_bgcolor=colors['background'], paper_bgcolor=colors['background'], font_color=colors['text'])
        alert = dbc.Alert("Nenhum contrato de venda encontrado para os filtros.", color="warning")
        return alert, empty_fig, empty_fig

    for col in ['qtd_vendida', 'preco_venda_contrato', 'produtividade_kg_ha', 'area_ha', 'custo_total_safra_ha']:
        dff_vendas_filtrado[col] = pd.to_numeric(dff_vendas_filtrado[col], errors='coerce').fillna(0)

    cultura_analisada = cultura if cultura is not None and cultura != 'todos' else (dff_vendas_filtrado['cultura'].mode()[0] if not dff_vendas_filtrado.empty else 'Soja')

    df_mercado_filtrado = df_precos_mercado[df_precos_mercado['cultura_nome'] == cultura_analisada].sort_values('data').copy()
    
    if start_date and end_date:
        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
        df_mercado_filtrado = df_mercado_filtrado[
            (df_mercado_filtrado['data'] >= start_date_dt) & (df_mercado_filtrado['data'] <= end_date_dt)
        ]

    fig_temporal = px.line(df_mercado_filtrado, x='data', y='preco_fecho_kg', title=f'Mercado vs. Vendas Realizadas: {cultura_analisada}', labels={'data': 'Data', 'preco_fecho_kg': 'Preço de Mercado (R$/kg)'})

    vendas_cultura_analisada = dff_vendas_filtrado[dff_vendas_filtrado['cultura'] == cultura_analisada].sort_values('data_venda').copy()

    if not vendas_cultura_analisada.empty and not df_mercado_filtrado.empty:
        vendas_com_mercado = pd.merge_asof(
            vendas_cultura_analisada,
            df_mercado_filtrado[['data', 'preco_fecho_kg']],
            left_on='data_venda',
            right_on='data',
            direction='nearest'
        )
        vendas_com_mercado['preco_fecho_kg'] = vendas_com_mercado['preco_fecho_kg'].fillna(0)

        colors_performance = np.where(vendas_com_mercado['preco_venda_contrato'] > vendas_com_mercado['preco_fecho_kg'], 'limegreen', 'crimson')
        min_size, max_size = 8, 50
        qtd_min, qtd_max = vendas_com_mercado['qtd_vendida'].min(), vendas_com_mercado['qtd_vendida'].max()
        if qtd_max > qtd_min:
            sizes = np.interp(vendas_com_mercado['qtd_vendida'], (qtd_min, qtd_max), (min_size, max_size))
        else:
            sizes = [min_size] * len(vendas_com_mercado)

        hover_text = [
            f"<b>Talhão:</b> {row.talhao}<br>" +
            f"<b>Data:</b> {row.data_venda.strftime('%d/%m/%Y')}<br>" +
            f"<b>Preço Venda:</b> R$ {row.preco_venda_contrato:,.2f}/kg<br>" +
            f"<b>Preço Mercado:</b> R$ {row.preco_fecho_kg:,.2f}/kg<br>" +
            f"<b>Quantidade:</b> {row.qtd_vendida:,.0f} kg"
            for index, row in vendas_com_mercado.iterrows()
        ]

        fig_temporal.add_trace(go.Scatter(
            x=vendas_com_mercado['data_venda'], y=vendas_com_mercado['preco_venda_contrato'],
            mode='markers', name='Contratos Fechados', hovertext=hover_text, hoverinfo='text',
            marker=dict(symbol='star', color=colors_performance, size=sizes, opacity=0.7, line=dict(width=1, color='rgba(255, 255, 255, 0.8)'))
        ))

    if not df_previsao_precos.empty:
        df_previsao_filtrado = df_previsao_precos[df_previsao_precos['cultura_nome'] == cultura_analisada]
        if start_date and end_date:
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df_previsao_filtrado = df_previsao_filtrado[(df_previsao_filtrado['ds'] >= start_date_dt) & (df_previsao_filtrado['ds'] <= end_date_dt)]

        fig_temporal.add_trace(go.Scatter(x=df_previsao_filtrado['ds'], y=df_previsao_filtrado['yhat'], mode='lines', line=dict(dash='dash', color='yellow'), name='Previsão de Preço'))
        fig_temporal.add_trace(go.Scatter(x=df_previsao_filtrado['ds'], y=df_previsao_filtrado['yhat_upper'], mode='lines', line=dict(width=0), fillcolor='rgba(255, 255, 0, 0.15)', showlegend=False))
        fig_temporal.add_trace(go.Scatter(x=df_previsao_filtrado['ds'], y=df_previsao_filtrado['yhat_lower'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(255, 255, 0, 0.15)', name='Intervalo de Confiança'))

    total_custo = (vendas_cultura_analisada['custo_total_safra_ha'] * vendas_cultura_analisada['area_ha']).sum()
    total_producao = (vendas_cultura_analisada['produtividade_kg_ha'] * vendas_cultura_analisada['area_ha']).sum()

    if total_producao > 0:
        custo_medio_kg = total_custo / total_producao
        fig_temporal.add_hline(y=custo_medio_kg, line_dash="dot", line_color="orange",
                              annotation_text=f"Custo Médio: R$ {custo_medio_kg:.2f}/kg",
                              annotation_position="bottom right")

    fig_temporal.update_layout(
        plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig_temporal.update_xaxes(rangeslider_visible=False)

    fig_boxplot = px.box(dff_vendas_filtrado, x='cultura', y='preco_venda_contrato', color='cultura', title=f'Distribuição do Preço de Venda por Cultura (Período Selecionado)', labels={'cultura': 'Cultura', 'preco_venda_contrato': 'Preço de Venda (R$/kg)'})
    fig_boxplot.update_layout(plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], showlegend=False)

    def criar_card_risco(titulo, valor, formato): return dbc.Card(dbc.CardBody([html.H4(titulo, className="card-title"), html.P(formato.format(valor), className="card-text", style={'fontSize': 24, 'color': colors['primary']})]))
    receita_realizada = (dff_vendas_filtrado['qtd_vendida'] * dff_vendas_filtrado['preco_venda_contrato']).sum()
    producao_total = (dff_vendas_filtrado['produtividade_kg_ha'] * dff_vendas_filtrado['area_ha']).sum()
    df_mercado_kpi = df_precos_mercado[df_precos_mercado['cultura_nome'] == cultura_analisada]
    preco_maximo_mercado = df_mercado_kpi['preco_fecho_kg'].max() if not df_mercado_kpi.empty else 0
    receita_potencial = producao_total * preco_maximo_mercado
    custo_oportunidade = receita_potencial - receita_realizada if receita_potencial > receita_realizada else 0
    cards = dbc.CardGroup([criar_card_risco("Receita Realizada", receita_realizada, "R$ {:,.2f}"), criar_card_risco("Receita Potencial Máx.", receita_potencial, "R$ {:,.2f}"), criar_card_risco("Custo de Oportunidade", custo_oportunidade, "R$ {:,.2f}")])

    return cards, fig_temporal, fig_boxplot

@app.callback(
    [Output('chat-history-store', 'data'), Output('chat-display', 'children'), Output('chat-input', 'value'), Output('loading-output-ia', 'children')],
    [Input('send-button', 'n_clicks'), Input('chat-input', 'n_submit')],
    [State('chat-input', 'value'), State('chat-history-store', 'data')]
)
def handle_chat(n_clicks, n_submit, user_input, chat_history):
    if (n_clicks == 0 and n_submit is None) or not user_input:
        return dash.no_update
    chat_history.append({'sender': 'user', 'message': user_input})
    bot_response = ""
    city_name_query = None
    triggers = ['em', 'para', 'de']
    words = user_input.lower().split()
    last_trigger_index = -1
    for i, word in enumerate(words):
        if word in triggers: last_trigger_index = i
    if last_trigger_index != -1 and last_trigger_index < len(words) - 1: city_name_query = " ".join(words[last_trigger_index + 1:])
    lat, lon, city_name_display = DEFAULT_LATITUDE, DEFAULT_LONGITUDE, DEFAULT_CITY_NAME
    error_msg = None
    if city_name_query:
        coords, error_msg = get_coords_for_city(city_name_query)
        if coords: lat, lon, city_name_display = coords
    if error_msg: bot_response = f"**Erro:** {error_msg}"
    else:
        forecast_data, error_message = get_weather_forecast(lat, lon)
        if error_message: bot_response = f"**Erro:** {error_message}"
        else:
            try:
                if "amanhã" in user_input.lower():
                    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
                    forecast_tomorrow = next((item for item in forecast_data['list'] if item['dt_txt'].startswith(tomorrow)), None)
                    if forecast_tomorrow:
                        temp, desc, pop = forecast_tomorrow['main']['temp'], forecast_tomorrow['weather'][0]['description'], forecast_tomorrow['pop'] * 100
                        bot_response = f"A previsão para amanhã em **{city_name_display}** é:\n- **Condição:** {desc.capitalize()}\n- **Temperatura:** {temp:.1f}°C\n- **Chuva:** {pop:.0f}% de chance"
                    else: bot_response = f"Não consegui encontrar a previsão para amanhã em {city_name_display}."
                elif "próximos dias" in user_input.lower() or "semana" in user_input.lower():
                    table_header = [html.Thead(html.Tr([html.Th("Dia"), html.Th("Condição"), html.Th("Temp. Máx/Mín"), html.Th("Chuva")]))]
                    table_body_rows = []
                    daily_forecasts = {}
                    for item in forecast_data['list']:
                        date = item['dt_txt'].split(' ')[0]
                        if date not in daily_forecasts: daily_forecasts[date] = {'temps': [], 'pops': [], 'descs': {}}
                        daily_forecasts[date]['temps'].append(item['main']['temp'])
                        daily_forecasts[date]['pops'].append(item['pop'])
                        desc = item['weather'][0]['description']
                        daily_forecasts[date]['descs'][desc] = daily_forecasts[date]['descs'].get(desc, 0) + 1
                    dias_semana_pt = {"Monday": "Segunda", "Tuesday": "Terça", "Wednesday": "Quarta", "Thursday": "Quinta", "Friday": "Sexta", "Saturday": "Sábado", "Sunday": "Domingo"}
                    day_count = 0
                    for date in sorted(daily_forecasts.keys()):
                        if day_count >= 5 or date == datetime.now().strftime('%Y-%m-%d'): continue
                        data = daily_forecasts[date]
                        day_name_en = datetime.strptime(date, '%Y-%m-%d').strftime('%A'); day_name_pt = dias_semana_pt.get(day_name_en, day_name_en)
                        max_temp, min_temp, pop_chance, main_desc = max(data['temps']), min(data['temps']), max(data['pops']) * 100, max(data['descs'], key=data['descs'].get)
                        table_body_rows.append(html.Tr([html.Td(day_name_pt), html.Td(main_desc.capitalize()), html.Td(f"{max_temp:.1f}°C / {min_temp:.1f}°C"), html.Td(f"{pop_chance:.0f}%")]))
                        day_count += 1
                    table_body = [html.Tbody(table_body_rows)]
                    bot_response = dbc.Table(table_header + table_body, bordered=False, hover=True, responsive=True, className="mt-2 table-dark")
                else: bot_response = "Desculpe, só consigo prever o tempo para **'amanhã'** ou **'próximos dias'**. Por favor, especifique um período e, opcionalmente, uma cidade (ex: 'previsão para amanhã em Sorriso')."
            except Exception as e: bot_response = f"Ocorreu um erro ao processar a previsão. Detalhe: {e}"
    chat_history.append({'sender': 'bot', 'message': bot_response})
    chat_display = []
    for entry in chat_history:
        style = {'padding': '10px', 'borderRadius': '10px', 'marginBottom': '10px', 'maxWidth': '80%'}
        content = dcc.Markdown(entry['message'], dangerously_allow_html=True) if isinstance(entry['message'], str) else entry['message']
        if entry['sender'] == 'user': style.update({'backgroundColor': colors['user_message_bg'], 'color': 'white', 'marginLeft': 'auto', 'textAlign': 'right'})
        else: style.update({'textAlign': 'left', 'backgroundColor': colors['bot_message_bg'], 'marginRight': 'auto'})
        chat_display.append(html.Div(content, style=style))
    return chat_history, chat_display, "", None

@app.callback(
    [Output('kpi-prod-media', 'children'), Output('kpi-area-total', 'children'), Output('mini-grafico-prod-cultura', 'figure'),
     Output('kpi-lucro-medio', 'children'), Output('kpi-custo-medio', 'children'), Output('mini-grafico-lucro-evolucao', 'figure'),
     Output('kpi-custo-oportunidade', 'children'), Output('kpi-preco-mercado-soja', 'children'), Output('mini-grafico-mercado', 'figure'),
     Output('kpi-ph-medio', 'children'), Output('kpi-fosforo-medio', 'children'), Output('mini-grafico-solo', 'figure'),
     Output('kpi-maquina-top-custo', 'children'), Output('kpi-operador-top-prod', 'children'), Output('mini-grafico-custo-atividade', 'figure'),
     Output('kpi-chuva-anual', 'children'), Output('kpi-temp-media-anual', 'children'), Output('mini-grafico-clima', 'figure')],
    [Input('filtro-ano-store', 'data'), Input('filtro-season-store', 'data'), Input('filtro-fazenda-store', 'data'), Input('filtro-cultura-store', 'data')]
)
def update_painel_principal(ano, season, fazenda, cultura):
    dff_agricola = df_agricola.copy()
    dff_completo = df_completo.copy()
    if ano is not None and ano != 'todos':
        dff_agricola = dff_agricola[dff_agricola['ano_safra'] == ano]
        dff_completo = dff_completo[dff_completo['ano_safra_num'] == int(ano)]
    if season is not None and season != 'todos':
        dff_agricola = dff_agricola[dff_agricola['season'] == season]
        dff_completo = dff_completo[dff_completo['season'] == season]
    if fazenda is not None and fazenda != 'todos':
        dff_agricola = dff_agricola[dff_agricola['fazenda'] == fazenda]
        dff_completo = dff_completo[dff_completo['fazenda'] == fazenda]
    if cultura is not None and cultura != 'todos':
        dff_agricola = dff_agricola[dff_agricola['cultura'] == cultura]
        dff_completo = dff_completo[dff_completo['cultura'] == cultura]
    prod_media = dff_agricola['produtividade_kg_ha'].mean()
    area_total = dff_agricola.drop_duplicates(subset=['talhao', 'ano_safra', 'season'])['area_ha'].sum()
    kpi_prod_media_str = f"{prod_media:,.0f} kg/ha" if pd.notna(prod_media) else "N/D"
    kpi_area_total_str = f"Área Total: {area_total:,.0f} ha"
    df_prod_cultura = dff_agricola.groupby('cultura')['produtividade_kg_ha'].mean().reset_index()
    mini_fig_prod_cultura = create_mini_figure()
    if not df_prod_cultura.empty: mini_fig_prod_cultura.add_trace(go.Bar(x=df_prod_cultura['cultura'], y=df_prod_cultura['produtividade_kg_ha'], marker_color=colors['primary']))
    lucro_medio = dff_agricola['lucro_ha'].mean()
    custo_medio = dff_agricola['custo_total_safra_ha'].mean()
    kpi_lucro_medio_str = f"R$ {lucro_medio:,.2f} / ha" if pd.notna(lucro_medio) else "N/D"
    kpi_custo_medio_str = f"Custo Médio: R$ {custo_medio:,.2f} / ha" if pd.notna(custo_medio) else "N/D"
    df_lucro_ano = dff_agricola.groupby('ano_safra_num')['lucro_ha'].mean().reset_index()
    mini_fig_lucro_evolucao = create_mini_figure()
    if not df_lucro_ano.empty: mini_fig_lucro_evolucao.add_trace(go.Scatter(x=df_lucro_ano['ano_safra_num'], y=df_lucro_ano['lucro_ha'], fill='tozeroy', line_color=colors['primary']))
    if df_vendas.empty or df_precos_mercado.empty:
        kpi_custo_oportunidade_str = "Dados de mercado indisponíveis"
        kpi_preco_mercado_soja = "Execute o script populate_data.py"
        mini_fig_mercado = create_mini_figure()
    else:
        receita_realizada_total = 0
        if 'preco_venda_kg' in dff_agricola.columns:
            dff_vendas_filtrado = dff_agricola.dropna(subset=['preco_venda_kg'])
            if not dff_vendas_filtrado.empty: receita_realizada_total = (dff_vendas_filtrado['quantidade_kg'] * dff_vendas_filtrado['preco_venda_kg']).sum()
        producao_total = (dff_agricola['produtividade_kg_ha'] * dff_agricola['area_ha']).sum()
        cultura_principal = dff_agricola['cultura'].mode()[0] if not dff_agricola.empty else ''
        preco_max_mercado = df_precos_mercado[df_precos_mercado['cultura_nome'] == cultura_principal]['preco_fecho_kg'].max() if not df_precos_mercado.empty and cultura_principal else 0
        receita_potencial = producao_total * preco_max_mercado
        custo_oportunidade = receita_potencial - receita_realizada_total if receita_realizada_total > 0 else 0
        kpi_custo_oportunidade_str = f"Custo Oport.: R$ {custo_oportunidade/1000:,.0f}k" if custo_oportunidade > 0 else "N/D"
        kpi_preco_mercado_soja = f"Mercado ({cultura_principal}): R$ {preco_max_mercado:.2f}/kg" if preco_max_mercado > 0 else "N/D"
        df_mercado_filtrado = df_precos_mercado[df_precos_mercado['cultura_nome'] == cultura_principal]
        mini_fig_mercado = create_mini_figure()
        if not df_mercado_filtrado.empty: mini_fig_mercado.add_trace(go.Scatter(x=df_mercado_filtrado['data'], y=df_mercado_filtrado['preco_fecho_kg'], line_color='red'))
    if 'ph' not in df_agricola.columns:
        kpi_ph_medio_str = "Dados de solo indisponíveis"
        kpi_fosforo_medio_str = "Execute o script populate_data.py"
        mini_fig_solo = create_mini_figure()
    else:
        ph_medio = dff_agricola['ph'].mean()
        fosforo_medio = dff_agricola['fosforo_ppm'].mean()
        kpi_ph_medio_str = f"pH Médio: {ph_medio:.2f}" if pd.notna(ph_medio) else "N/D"
        kpi_fosforo_medio_str = f"Fósforo Médio: {fosforo_medio:.1f} ppm" if pd.notna(fosforo_medio) else "N/D"
        mini_fig_solo = create_mini_figure()
        if not dff_agricola['ph'].dropna().empty:
            mini_fig_solo.add_trace(go.Box(y=dff_agricola['ph'], name='pH', marker_color=colors['primary']))
            mini_fig_solo.update_layout(xaxis=dict(showticklabels=True))
    df_maquinas = dff_completo.dropna(subset=['maquina']).groupby('maquina')['custo_total_ha'].sum().nlargest(1).reset_index()
    maquina_top_custo = df_maquinas.iloc[0]['maquina'] if not df_maquinas.empty else 'N/D'
    kpi_maquina_top_custo_str = f"Maior Custo: {maquina_top_custo}"
    df_colheita = dff_completo[dff_completo['tipo_atividade'] == 'Colheita'].dropna(subset=['operador'])
    df_prod_operador = df_colheita.groupby('operador')['produtividade_kg_ha'].mean().nlargest(1).reset_index()
    operador_top_prod = df_prod_operador.iloc[0]['operador'] if not df_prod_operador.empty else 'N/D'
    kpi_operador_top_prod_str = f"Top Produtiv.: {operador_top_prod}"
    df_custo_atividade = dff_completo.groupby('tipo_atividade')['custo_total_ha'].sum().reset_index()
    mini_fig_custo_atividade = create_mini_figure()
    if not df_custo_atividade.empty:
        mini_fig_custo_atividade.add_trace(go.Pie(labels=df_custo_atividade['tipo_atividade'], values=df_custo_atividade['custo_total_ha'], hole=.6))
        mini_fig_custo_atividade.update_layout(showlegend=False)
    if df_clima.empty:
        kpi_chuva_anual_str = "Dados de clima indisponíveis"
        kpi_temp_media_anual_str = "Falta o arquivo .csv"
        mini_fig_clima = create_mini_figure()
    else:
        ano_clima = int(ano) if ano is not None and ano != 'todos' else (df_clima['ano'].max() if not df_clima.empty else datetime.now().year)
        dff_clima = df_clima[df_clima['ano'] == ano_clima]
        chuva_anual = dff_clima['precipitacao_mm'].sum()
        temp_media_anual = dff_clima['temperatura_c'].mean()
        kpi_chuva_anual_str = f"Chuva em {ano_clima}: {chuva_anual:,.0f} mm"
        kpi_temp_media_anual_str = f"Temp. Média: {temp_media_anual:.1f}°C"
        df_mensal_clima = dff_clima.groupby('mes')['precipitacao_mm'].sum().reset_index()
        mini_fig_clima = create_mini_figure()
        if not df_mensal_clima.empty: mini_fig_clima.add_trace(go.Bar(x=df_mensal_clima['mes'], y=df_mensal_clima['precipitacao_mm'], marker_color='lightblue'))
    return (kpi_prod_media_str, kpi_area_total_str, mini_fig_prod_cultura, kpi_lucro_medio_str, kpi_custo_medio_str, mini_fig_lucro_evolucao, kpi_custo_oportunidade_str, kpi_preco_mercado_soja, mini_fig_mercado, kpi_ph_medio_str, kpi_fosforo_medio_str, mini_fig_solo, kpi_maquina_top_custo_str, kpi_operador_top_prod_str, mini_fig_custo_atividade, kpi_chuva_anual_str, kpi_temp_media_anual_str, mini_fig_clima)

@app.callback(
    [Output('cards-kpi-agricola', 'children'), Output('grafico-prod-cultura', 'figure'), Output('grafico-prod-fazenda', 'figure'), Output('grafico-evolucao-prod', 'figure')],
    [Input('filtro-ano-store', 'data'), Input('filtro-season-store', 'data'), Input('filtro-fazenda-store', 'data'), Input('filtro-cultura-store', 'data')]
)
def update_dashboard_agricola(ano, season, fazenda, cultura):
    dff = df_agricola.copy()
    if ano is not None and ano != 'todos': dff = dff[dff['ano_safra'] == ano]
    if season is not None and season != 'todos': dff = dff[dff['season'] == season]
    if fazenda is not None and fazenda != 'todos': dff = dff[dff['fazenda'] == fazenda]
    if cultura is not None and cultura != 'todos': dff = dff[dff['cultura'] == cultura]
    def criar_card_agricola(titulo, valor, unidade): return dbc.Card(dbc.CardBody([html.H4(titulo, className="card-title", style={'marginBottom': '10px'}), html.P(f"{valor:,.0f} {unidade}", className="card-text", style={'fontSize': 24, 'color': colors['primary'], 'fontWeight': 'bold'})]))
    prod_media, area_total, total_talhoes, total_registros = (dff['produtividade_kg_ha'].mean(), dff.drop_duplicates(subset=['talhao', 'ano_safra', 'season'])['area_ha'].sum(), dff['talhao'].nunique(), len(dff)) if not dff.empty else (0,0,0,0)
    cards = dbc.CardGroup([criar_card_agricola("Produtividade Média", prod_media, "kg/ha"), criar_card_agricola("Área Plantada", area_total, "ha"), criar_card_agricola("Talhões Cultivados", total_talhoes, ""), criar_card_agricola("Registos de Safra", total_registros, "")])
    def style_figure(fig, title):
        fig.update_layout(title=title, plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], xaxis=dict(gridcolor=colors['grid']), yaxis=dict(gridcolor=colors['grid']), margin=dict(l=40, r=20, t=40, b=30))
        return fig
    df_prod_cultura = dff.groupby('cultura')['produtividade_kg_ha'].mean().sort_values(ascending=False).reset_index()
    fig_prod_cultura = px.bar(df_prod_cultura, x='cultura', y='produtividade_kg_ha', text_auto='.0f')
    fig_prod_cultura = style_figure(fig_prod_cultura, 'Produtividade Média por Cultura')
    df_prod_fazenda = dff.groupby('fazenda')['produtividade_kg_ha'].mean().sort_values(ascending=True).reset_index()
    fig_prod_fazenda = px.bar(df_prod_fazenda, y='fazenda', x='produtividade_kg_ha', text_auto='.0f', orientation='h')
    fig_prod_fazenda = style_figure(fig_prod_fazenda, 'Produtividade Média por Fazenda')
    fig_prod_fazenda.update_yaxes(title_text='')
    df_evolucao = df_agricola.groupby('ano_safra_num')['produtividade_kg_ha'].mean().reset_index()
    fig_evolucao_prod = px.area(df_evolucao, x='ano_safra_num', y='produtividade_kg_ha', markers=True)
    fig_evolucao_prod = style_figure(fig_evolucao_prod, 'Evolução da Produtividade Anual (kg/ha)')
    return cards, fig_prod_cultura, fig_prod_fazenda, fig_evolucao_prod

@app.callback(
    Output('comparativo-talhoes-container', 'children'),
    [Input('filtro-ano-store', 'data'), Input('filtro-season-store', 'data'), Input('filtro-cultura-store', 'data')]
)
def update_analise_talhoes(ano, season, cultura):
    dff = df_agricola.copy()
    if ano is not None and ano != 'todos': dff = dff[dff['ano_safra'] == ano]
    if season is not None and season != 'todos': dff = dff[dff['season'] == season]
    if cultura is not None and cultura != 'todos': dff = dff[dff['cultura'] == cultura]
    if dff.empty or dff['talhao'].nunique() < 2: return dbc.Alert("Dados insuficientes para comparação.", color="warning", style={'textAlign': 'center'})
    lucro_medio_talhoes = dff.groupby('talhao')['lucro_ha'].mean()
    melhor_talhao_id = lucro_medio_talhoes.idxmax()
    pior_talhao_id = lucro_medio_talhoes.idxmin()
    def criar_card_talhao(talhao_id, tipo):
        dados_talhao = dff[dff['talhao'] == talhao_id].iloc[0]
        lucro_medio = lucro_medio_talhoes.loc[talhao_id]
        cor_titulo = 'success' if tipo == 'Melhor' else 'danger'
        return dbc.Card([dbc.CardHeader(html.H4(f'{tipo} Desempenho: {talhao_id}', className=f"text-{cor_titulo}")), dbc.CardBody([html.P(f"Fazenda: {dados_talhao['fazenda']}"), html.P(f"Lucratividade Média: R$ {lucro_medio:,.2f} / ha"), dbc.Button('Ver Detalhes da Última Safra', id={'type': 'btn-detalhes-talhao', 'index': talhao_id}, n_clicks=0, color=cor_titulo)])], style={'width': '48%'})
    return [criar_card_talhao(melhor_talhao_id, 'Melhor'), criar_card_talhao(pior_talhao_id, 'Pior')]
    
@app.callback(
    [Output('modal-detalhes-talhao', 'is_open'), Output('modal-detalhes-talhao', 'children')],
    [Input({'type': 'btn-detalhes-talhao', 'index': ALL}, 'n_clicks')],
    [State('modal-detalhes-talhao', 'is_open')], prevent_initial_call=True
)
def display_talhao_details(n_clicks, is_open):
    ctx = dash.callback_context
    if not any(n_clicks) or not ctx.triggered: return False, []
    if any(n > 0 for n in n_clicks):
        button_id_str = ctx.triggered[0]['prop_id'].split('.')[0]
        talhao_id = eval(button_id_str)['index']
        ultima_safra_df = df_completo[df_completo['talhao'] == talhao_id].sort_values('data_plantio', ascending=False)
        if ultima_safra_df.empty: return True, dbc.ModalBody("Não foi possível encontrar dados.")
        ultima_safra = ultima_safra_df.iloc[0]
        safra_id = ultima_safra['safra_id']
        atividades = df_completo[df_completo['safra_id'] == safra_id].drop_duplicates(subset=['tipo_atividade', 'produto_utilizado'])
        chuva_total = "Dados indisponíveis"
        if not df_clima.empty and pd.notna(ultima_safra['data_plantio']) and pd.notna(ultima_safra['data_colheita_real']):
            chuva_periodo = df_clima[(df_clima.index >= ultima_safra['data_plantio']) & (df_clima.index <= ultima_safra['data_colheita_real'])]
            chuva_total = f"{chuva_periodo['precipitacao_mm'].sum():.1f} mm"
        atividades_formatadas = atividades[['data_execucao', 'tipo_atividade', 'produto_utilizado', 'quantidade_aplicada_ha', 'unidade', 'custo_total_ha']].copy()
        atividades_formatadas['data_execucao'] = atividades_formatadas['data_execucao'].dt.strftime('%d/%m/%Y')
        modal_content = [
            dbc.ModalHeader(dbc.ModalTitle(f"Detalhes do Talhão: {talhao_id}")),
            dbc.ModalBody([
                html.H4("Resumo da Última Safra"),
                dbc.Row([dbc.Col(html.P(f"Cultura: {ultima_safra['cultura']}")), dbc.Col(html.P(f"Produtividade: {ultima_safra['produtividade_kg_ha']:,.0f} kg/ha"))]),
                dbc.Row([dbc.Col(html.P(f"Lucro: R$ {df_agricola[df_agricola['safra_id']==safra_id]['lucro_ha'].iloc[0]:,.2f} / ha")), dbc.Col(html.P(f"Chuva no Ciclo: {chuva_total}"))]),
                html.Hr(),
                html.H4("Atividades de Manejo e Custos"),
                dash_table.DataTable(data=atividades_formatadas.to_dict('records'), columns=[{'name': i, 'id': i} for i in atividades_formatadas.columns], style_cell={'textAlign': 'left'}, style_data={'backgroundColor': 'transparent', 'color': colors['text']}, style_header={'backgroundColor': colors['primary'], 'fontWeight': 'bold'})
            ])
        ]
        return not is_open, modal_content
    return is_open, []

@app.callback(
    Output('grafico-correlacao-solo', 'figure'),
    [Input('dropdown-eixo-x-solo', 'value'), Input('dropdown-eixo-y-solo', 'value'), Input('filtro-ano-store', 'data'), Input('filtro-cultura-store', 'data')]
)
def update_grafico_correlacao(eixo_x, eixo_y, ano, cultura):
    if 'ph' not in df_agricola.columns:
        return go.Figure().update_layout(title='Dados de solo não disponíveis', paper_bgcolor=colors['background'], plot_bgcolor=colors['background'], font_color=colors['text'])
    dff = df_agricola.dropna(subset=[eixo_x, eixo_y]).copy()
    if ano is not None and ano != 'todos': dff = dff[dff['ano_safra'] == ano]
    if cultura is not None and cultura != 'todos': dff = dff[dff['cultura'] == cultura]
    fig = px.scatter(dff, x=eixo_x, y=eixo_y, color='cultura', hover_data=['talhao', 'ano_safra'], trendline='ols', title=f'Correlação entre {eixo_x.replace("_", " ").title()} e {eixo_y.replace("_", " ").title()}')

    if not dff.empty:
        traces = [trace for trace in fig.data if trace.mode == 'markers']
        num_traces = len(traces)
        for i in range(num_traces):
            original_name = fig.data[i].name
            fig.data[i + num_traces].name = f'Tendência - {original_name}'
            fig.data[i + num_traces].showlegend = True
            
    fig.update_layout(plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'])
    return fig

# <<< MUDANÇA: Função auxiliar para detecção de anomalias, agora retorna alertas e uma tabela de dados >>>
def detectar_anomalias_operacionais(df_filtrado, df_historico, z_score_threshold=2):
    alertas = []
    anomalous_rows = []
    atividades_para_analisar = df_filtrado['tipo_atividade'].dropna().unique()

    for atividade in atividades_para_analisar:
        custos_historicos = df_historico[df_historico['tipo_atividade'] == atividade]['custo_total_ha'].dropna()
        if len(custos_historicos) < 5:
            continue
        
        media_historica = custos_historicos.mean()
        std_historico = custos_historicos.std()
        
        limite_superior = media_historica + z_score_threshold * std_historico
        
        atividades_filtradas = df_filtrado[df_filtrado['tipo_atividade'] == atividade]
        
        for index, row in atividades_filtradas.iterrows():
            custo_atual = row['custo_total_ha']
            if pd.isna(custo_atual):
                continue

            if custo_atual > limite_superior:
                percentual_acima = ((custo_atual - media_historica) / media_historica) * 100
                
                mensagem = [
                    html.Strong(f"Custo Alto: {row['tipo_atividade']} no Talhão {row['talhao']}", className="alert-heading"),
                    f" registrou um custo de R$ {custo_atual:,.2f}/ha, ",
                    html.Strong(f"{percentual_acima:.0f}% acima da média histórica"),
                    f" (Média: R$ {media_historica:,.2f}/ha)."
                ]
                
                # <<< MUDANÇA: removido 'duration' para tornar o alerta persistente >>>
                alertas.append(dbc.Alert(mensagem, color="warning"))
                
                # Adiciona a linha anômala à lista para a tabela
                row_data = row.to_dict()
                row_data['custo_medio_historico'] = media_historica
                row_data['desvio_percentual'] = percentual_acima
                anomalous_rows.append(row_data)

    if not alertas:
        return [dbc.Alert("Nenhuma anomalia de custo operacional detectada no período selecionado.", color="success")], pd.DataFrame()

    df_anomalias = pd.DataFrame(anomalous_rows)
    # Seleciona e formata colunas para a tabela
    df_anomalias['Data'] = df_anomalias['data_execucao'].dt.strftime('%d/%m/%Y')
    df_anomalias['Custo Verificado (R$/ha)'] = df_anomalias['custo_total_ha'].round(2)
    df_anomalias['Custo Esperado (R$/ha)'] = df_anomalias['custo_medio_historico'].round(2)
    df_anomalias['Desvio (%)'] = df_anomalias['desvio_percentual'].apply(lambda x: f"+{x:.0f}%")
    
    colunas_tabela = {
        'Data': 'Data', 'operador': 'Operador', 'fazenda': 'Fazenda', 'talhao': 'Talhão',
        'tipo_atividade': 'Atividade', 'Custo Verificado (R$/ha)': 'Custo Verificado (R$/ha)',
        'Custo Esperado (R$/ha)': 'Custo Esperado (R$/ha)', 'Desvio (%)': 'Desvio (%)'
    }
    df_anomalias_final = df_anomalias[list(colunas_tabela.keys())].rename(columns=colunas_tabela)

    return alertas, df_anomalias_final

# <<< MUDANÇA: Callback agora tem 5 saídas, incluindo os dados e colunas da nova tabela >>>
@app.callback(
    [Output('grafico-custo-maquina', 'figure'),
     Output('grafico-prod-operador', 'figure'),
     Output('alert-panel-operacional', 'children'),
     Output('anomaly-table-operacional', 'data'),
     Output('anomaly-table-operacional', 'columns')],
    [Input('filtro-ano-store', 'data'),
     Input('filtro-season-store', 'data'),
     Input('filtro-cultura-store', 'data')]
)
def update_grafico_operacional(ano, season, cultura):
    dff = df_completo.copy()
    if ano is not None and ano != 'todos': dff = dff[dff['ano_safra_num'] == int(ano)]
    if season is not None and season != 'todos': dff = dff[dff['season'] == season]
    if cultura is not None and cultura != 'todos': dff = dff[dff['cultura'] == cultura]
    
    def style_figure(fig, title):
        fig.update_layout(title=title, plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], xaxis=dict(gridcolor=colors['grid']), yaxis=dict(gridcolor=colors['grid']), margin=dict(l=40, r=20, t=40, b=30))
        return fig
    
    df_maquinas = dff.dropna(subset=['maquina']).groupby('maquina')['custo_total_ha'].sum().reset_index()
    fig_maquina = px.bar(df_maquinas, x='maquina', y='custo_total_ha', title='Custo Total Acumulado por Máquina (R$)', text_auto='.2s')
    fig_maquina = style_figure(fig_maquina, 'Custo Total Acumulado por Máquina')
    
    df_colheita = dff[dff['tipo_atividade'] == 'Colheita'].dropna(subset=['operador'])
    df_prod_operador = df_colheita.groupby('operador')['produtividade_kg_ha'].mean().reset_index()
    fig_operador = px.bar(df_prod_operador, x='operador', y='produtividade_kg_ha', title='Produtividade Média na Colheita por Operador (kg/ha)', text_auto='.0f')
    fig_operador = style_figure(fig_operador, 'Produtividade Média na Colheita por Operador')

    # Detecção de Anomalias agora retorna alertas e um dataframe
    alertas, df_anomalias = detectar_anomalias_operacionais(dff, df_completo)
    
    # Prepara os dados para a DataTable
    table_data = df_anomalias.to_dict('records')
    table_columns = [{'name': i, 'id': i} for i in df_anomalias.columns]
    
    return fig_maquina, fig_operador, alertas, table_data, table_columns

@app.callback(
    [Output('cards-kpi-clima', 'children'), Output('grafico-clima-mensal', 'figure')],
    [Input('filtro-ano-clima', 'value')]
)
def update_dashboard_clima(ano):
    if df_clima.empty or ano is None:
        return [], go.Figure().update_layout(title='Dados climáticos não disponíveis', paper_bgcolor=colors['background'], plot_bgcolor=colors['background'], font_color=colors['text'])
    dff = df_clima[df_clima['ano'] == ano]
    def criar_card_clima(titulo, valor, unidade): return dbc.Card(dbc.CardBody([html.H4(titulo, className="card-title", style={'marginBottom': '10px'}), html.P(f"{valor:.1f} {unidade}", className="card-text", style={'fontSize': 24, 'color': colors['primary'], 'fontWeight': 'bold'})]))
    prec_anual, temp_media, temp_max, temp_min = (dff['precipitacao_mm'].sum(), dff['temperatura_c'].mean(), dff['temperatura_c'].max(), dff['temperatura_c'].min()) if not dff.empty else (0,0,0,0)
    cards = dbc.CardGroup([criar_card_clima("Precipitação Total", prec_anual, "mm"), criar_card_clima("Temp. Média", temp_media, "°C"), criar_card_clima("Temp. Máxima", temp_max, "°C"), criar_card_clima("Temp. Mínima", temp_min, "°C")])
    df_mensal = dff.groupby('mes').agg(precipitacao_mm=('precipitacao_mm', 'sum'), temperatura_c_media=('temperatura_c', 'mean'), temperatura_c_max=('temperatura_c', 'max'), temperatura_c_min=('temperatura_c', 'min')).reset_index()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df_mensal['mes'], y=df_mensal['precipitacao_mm'], name='Precipitação', marker_color='blue'), secondary_y=True)
    fig.add_trace(go.Scatter(x=df_mensal['mes'], y=df_mensal['temperatura_c_media'], name='Temp. Média', mode='lines+markers', line=dict(color='orange')), secondary_y=False)
    fig.add_trace(go.Scatter(x=df_mensal['mes'], y=df_mensal['temperatura_c_max'], name='Temp. Máxima', mode='lines', line=dict(color='red', dash='dot')), secondary_y=False)
    fig.add_trace(go.Scatter(x=df_mensal['mes'], y=df_mensal['temperatura_c_min'], name='Temp. Mínima', mode='lines', line=dict(color='lightblue', dash='dot')), secondary_y=False)
    fig.update_layout(title=f'Dados Climáticos Mensais para {ano}', plot_bgcolor=colors['card_background'], paper_bgcolor=colors['background'], font_color=colors['text'], xaxis_title='Mês', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_yaxes(title_text="Temperatura (°C)", secondary_y=False)
    fig.update_yaxes(title_text="Precipitação (mm)", secondary_y=True, showgrid=False)
    return cards, fig

# =============================================================================
# 5. EXECUÇÃO DO SERVIDOR
# =============================================================================
if __name__ == '__main__':
    app.run(debug=True)