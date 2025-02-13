import requests
import json
import pandas as pd
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud.exceptions import NotFound
from datetime import datetime
import os
import tempfile
import logging
from flask import Flask, jsonify

app = Flask(__name__)

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função para acessar segredos no Google Cloud Secret Manager
def acessar_segredo(nome_segredo):
    client = secretmanager.SecretManagerServiceClient()
    resposta = client.access_secret_version(request={"name": nome_segredo})
    return resposta.payload.data.decode("UTF-8")

def acessar_chave_api():
    client = secretmanager.SecretManagerServiceClient()
    nome_segredo = "projects/325835689813/secrets/chave_api_coingecko/versions/latest"
    resposta = client.access_secret_version(request={"name": nome_segredo})
    chave_api = resposta.payload.data.decode("UTF-8")
    # Remove aspas extras, se houver
    chave_api = chave_api.strip('"')
    return chave_api

def carregar_credenciais():
    credenciais = acessar_segredo("projects/325835689813/secrets/credencial_bigquery/versions/latest")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(credenciais)
        return temp_file.name

# Configurações do ambiente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = carregar_credenciais()
os.environ["GOOGLE_CLOUD_PROJECT"] = "projeto-treinamento-450619"  # Defina o projeto do Google Cloud

CHAVE_API = acessar_chave_api()
logger.info(f"Chave da API obtida: {CHAVE_API}")
TIPO_MOEDA = "brl"
DATASET_BIGQUERY = 'cripto_dataset'
TABELA_HISTORICO = 'tabela_criptomoedas'
TABELA_COMPLET = 'tabela_complet_criptomoedas'
URL_API = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={TIPO_MOEDA}&x_cg_demo_api_key={CHAVE_API}"

# Log da URL da API
logger.info(f"URL da API: {URL_API}")

# Função para consumir a API
def buscar_dados_api():
    try:
        resposta = requests.get(URL_API)
        resposta.raise_for_status()  # Levanta uma exceção para códigos de status 4xx/5xx
        logger.info("Dados da API consumidos com sucesso.")
        return resposta.json()
    except requests.exceptions.HTTPError as err:
        logger.error(f"Erro HTTP ao consumir a API: {err}")
        raise
    except Exception as e:
        logger.error(f"Erro ao consumir a API: {e}")
        raise

# Função para adicionar timestamp aos dados
def adicionar_timestamp(dados):
    timestamp = datetime.utcnow().isoformat()  # Timestamp no formato ISO
    for item in dados:
        item["data_hora_coleta"] = timestamp  # Novo campo para o timestamp
    return dados

# Função para tratar os dados
def tratar_dados(dados):
    df = pd.DataFrame(dados)
    
    # Converter campos numéricos
    campos_numericos = [
        "current_price", "market_cap", "fully_diluted_valuation", "total_volume",
        "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h",
        "market_cap_change_24h", "market_cap_change_percentage_24h", "circulating_supply",
        "total_supply", "max_supply", "ath", "ath_change_percentage", "atl", "atl_change_percentage"
    ]
    for campo in campos_numericos:
        df[campo] = pd.to_numeric(df[campo], errors='coerce')

    # Tratar campos de data
    campos_data = ["ath_date", "atl_date", "last_updated"]
    for campo in campos_data:
        df[campo] = pd.to_datetime(df[campo], errors='coerce')

    # Tratar campo ROI (pode ser nulo ou um objeto)
    df["roi"] = df["roi"].apply(lambda x: json.dumps(x) if x is not None else None)

    return df

# Função para criar a tabela no BigQuery se ela não existir
def criar_tabela_se_nao_existir(tabela, schema):
    cliente = bigquery.Client()
    referencia_tabela = cliente.dataset(DATASET_BIGQUERY).table(tabela)
    
    try:
        cliente.get_table(referencia_tabela)  # Tenta obter a tabela
        logger.info(f"Tabela {tabela} já existe.")
    except NotFound:
        logger.info(f"Tabela {tabela} não existe. Criando...")
        tabela = bigquery.Table(referencia_tabela, schema=schema)
        cliente.create_table(tabela)
        logger.info(f"Tabela {tabela} criada com sucesso.")

# Função para salvar dados no BigQuery
def salvar_dados_bigquery(df, tabela):
    cliente = bigquery.Client()
    referencia_tabela = cliente.dataset(DATASET_BIGQUERY).table(tabela)
    tabela = cliente.get_table(referencia_tabela)

    # Inserir os dados no BigQuery
    job = cliente.load_table_from_dataframe(df, tabela)
    job.result()  # Espera a conclusão do job

    if job.errors is None:
        logger.info(f"Dados inseridos com sucesso no BigQuery na tabela {tabela}.")
    else:
        logger.error(f"Erros ao inserir dados no BigQuery: {job.errors}")

# Função para registrar a execução na tabela_complet_criptomoedas
def registrar_execucao(status):
    data_execucao = datetime.utcnow().isoformat()
    registro = {
        "data_execucao": data_execucao,
        "status": status
    }
    df = pd.DataFrame([registro])
    salvar_dados_bigquery(df, TABELA_COMPLET)

# Schema da tabela_criptomoedas
schema_criptomoedas = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("simbolo", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("imagem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preco_atual", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("capitalizacao_mercado", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rank_capitalizacao", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("valor_total_diluido", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("volume_total", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("maior_preco_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("menor_preco_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_preco_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_capitalizacao_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_capitalizacao_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oferta_circulante", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oferta_total", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oferta_maxima", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("preco_maximo_historico", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_preco_maximo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("data_preco_maximo", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("preco_minimo_historico", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_preco_minimo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("data_preco_minimo", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("retorno_investimento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ultima_atualizacao", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("data_hora_coleta", "TIMESTAMP", mode="NULLABLE"),
]

@app.route('/', methods=['GET'])
def main():
    try:
        # Consumir a API
        dados_criptomoedas = buscar_dados_api()

        # Adicionar timestamp aos dados
        dados_com_timestamp = adicionar_timestamp(dados_criptomoedas)

        # Tratar os dados
        df = tratar_dados(dados_com_timestamp)

        # Renomear as colunas para português
        df = df.rename(columns={
            "id": "id",
            "symbol": "simbolo",
            "name": "nome",
            "image": "imagem",
            "current_price": "preco_atual",
            "market_cap": "capitalizacao_mercado",
            "market_cap_rank": "rank_capitalizacao",
            "fully_diluted_valuation": "valor_total_diluido",
            "total_volume": "volume_total",
            "high_24h": "maior_preco_24h",
            "low_24h": "menor_preco_24h",
            "price_change_24h": "variacao_preco_24h",
            "price_change_percentage_24h": "variacao_percentual_24h",
            "market_cap_change_24h": "variacao_capitalizacao_24h",
            "market_cap_change_percentage_24h": "variacao_percentual_capitalizacao_24h",
            "circulating_supply": "oferta_circulante",
            "total_supply": "oferta_total",
            "max_supply": "oferta_maxima",
            "ath": "preco_maximo_historico",
            "ath_change_percentage": "variacao_percentual_preco_maximo",
            "ath_date": "data_preco_maximo",
            "atl": "preco_minimo_historico",
            "atl_change_percentage": "variacao_percentual_preco_minimo",
            "atl_date": "data_preco_minimo",
            "roi": "retorno_investimento",
            "last_updated": "ultima_atualizacao",
            "data_hora_coleta": "data_hora_coleta"
        })

        # Mostrar o schema e os dados
        logger.info(df.info())
        logger.info(df.head())

        # Criar a tabela se não existir
        criar_tabela_se_nao_existir(TABELA_HISTORICO, schema_criptomoedas)

        # Salvar os dados no BigQuery
        salvar_dados_bigquery(df, TABELA_HISTORICO)

        # Registrar a execução como sucesso
        registrar_execucao(True)

        return jsonify({"status": "success", "message": "Dados processados e salvos com sucesso."}), 200

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        # Registrar a execução como falha
        registrar_execucao(False)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)