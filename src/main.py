import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime
import os
import tempfile
import logging
import json
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

# Função para carregar credenciais do BigQuery
def carregar_credenciais():
    credenciais = acessar_segredo("projects/325835689813/secrets/credencial_bigquery/versions/latest")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(credenciais)
        return temp_file.name

# Configurações do ambiente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = carregar_credenciais()
os.environ["GOOGLE_CLOUD_PROJECT"] = "projeto-treinamento-450619"  # Defina o projeto do Google Cloud

# Constantes
CHAVE_API = acessar_segredo("projects/325835689813/secrets/chave_api_coingecko/versions/latest").strip('"')
TIPO_MOEDA = "brl"
DATASET_BIGQUERY = 'cripto_pandas_dataset'
TABELA_HISTORICO = 'tabela_criptomoedas'
TABELA_COMPLET = 'tabela_complet_criptomoedas'
URL_API = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={TIPO_MOEDA}&x_cg_demo_api_key={CHAVE_API}"

# Schema das tabelas no BigQuery
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

schema_tabela_complet = [
    bigquery.SchemaField("data_execucao", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("status", "BOOLEAN", mode="REQUIRED"),
]

# Função para criar dataset e tabelas no BigQuery
def criar_dataset_e_tabelas():
    cliente = bigquery.Client()

    # Criar dataset se não existir
    dataset_ref = cliente.dataset(DATASET_BIGQUERY)
    try:
        cliente.get_dataset(dataset_ref)
        logger.info(f"Dataset {DATASET_BIGQUERY} já existe.")
    except Exception:
        logger.info(f"Dataset {DATASET_BIGQUERY} não existe. Criando...")
        cliente.create_dataset(dataset_ref)
        logger.info(f"Dataset {DATASET_BIGQUERY} criado com sucesso.")

    # Criar tabelas se não existirem
    for tabela, schema in [(TABELA_HISTORICO, schema_criptomoedas), (TABELA_COMPLET, schema_tabela_complet)]:
        tabela_ref = dataset_ref.table(tabela)
        try:
            cliente.get_table(tabela_ref)
            logger.info(f"Tabela {tabela} já existe.")
        except Exception:
            logger.info(f"Tabela {tabela} não existe. Criando...")
            tabela = bigquery.Table(tabela_ref, schema=schema)
            cliente.create_table(tabela)
            logger.info(f"Tabela {tabela} criada com sucesso.")

# Função para buscar dados da API
def buscar_dados_api():
    try:
        resposta = requests.get(URL_API)
        resposta.raise_for_status()
        logger.info("Dados da API consumidos com sucesso.")
        return resposta.json()
    except Exception as e:
        logger.error(f"Erro ao consumir a API: {e}")
        raise

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
        df[campo] = pd.to_numeric(df[campo], errors="coerce")

    # Tratar campos de data
    campos_data = ["ath_date", "atl_date", "last_updated"]
    for campo in campos_data:
        df[campo] = pd.to_datetime(df[campo], errors="coerce")

    # Adicionar timestamp de coleta
    df["data_hora_coleta"] = datetime.utcnow()

    # Tratar campo ROI (pode ser um objeto JSON)
    df["roi"] = df["roi"].apply(lambda x: json.dumps(x) if x is not None else None)

    # Renomear colunas para português
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
    })

    return df

# Função para salvar dados no BigQuery
def salvar_dados_bigquery(df, tabela):
    cliente = bigquery.Client()
    tabela_ref = cliente.dataset(DATASET_BIGQUERY).table(tabela)
    job = cliente.load_table_from_dataframe(df, tabela_ref)
    job.result()  # Espera a conclusão do job
    logger.info(f"Dados salvos na tabela {tabela} com sucesso.")

@app.route('/')
def main():
    try:
        # Criar dataset e tabelas
        criar_dataset_e_tabelas()

        # Buscar dados da API
        dados = buscar_dados_api()

        # Tratar dados
        df = tratar_dados(dados)

        # Salvar dados no BigQuery
        salvar_dados_bigquery(df, TABELA_HISTORICO)

        # Registrar execução bem-sucedida
        registro = pd.DataFrame([{
            "data_execucao": datetime.utcnow(),
            "status": True
        }])
        salvar_dados_bigquery(registro, TABELA_COMPLET)

        logger.info("Processo concluído com sucesso.")

        # Retornar uma resposta JSON de sucesso
        return jsonify({"status": "success", "message": "Dados processados e salvos com sucesso."})

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")

        # Registrar execução falha
        registro = pd.DataFrame([{
            "data_execucao": datetime.utcnow(),
            "status": False
        }])
        salvar_dados_bigquery(registro, TABELA_COMPLET)

        logger.error("Processo concluído com erros.")

        # Retornar uma resposta JSON de erro
        return jsonify({"status": "error", "message": str(e)}), 500

# Executar o script
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
