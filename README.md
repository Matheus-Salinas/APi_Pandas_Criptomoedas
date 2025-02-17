Projeto: Coleta e Armazenamento de Dados de Criptomoedas

Descrição

Este projeto tem como objetivo consumir dados de criptomoedas da API CoinGecko, armazená-los no Google BigQuery e utilizar ferramentas da Google Cloud Platform (GCP), como Cloud Run, Cloud Scheduler e Secret Manager. O código é programado para rodar a cada uma hora, proporcionando uma forma automatizada de coleta e armazenamento de dados.

Tecnologias Utilizadas

Python e Pandas para manipulação e tratamento de dados.

Flask para disponibilização de um endpoint.

Google Cloud BigQuery para armazenamento dos dados coletados.

Google Cloud Secret Manager para gerenciamento seguro de credenciais.

Google Cloud Run para executar o serviço de forma escalável.

Google Cloud Scheduler para programar execuções periódicas.

GitHub Actions para automação do deploy.

Estrutura do Projeto

/
|-- main.py                # Script principal para coleta e armazenamento de dados
|-- requirements.txt       # Dependências do projeto
|-- Dockerfile             # Configuração para conteinerização no Cloud Run
|-- deploy.yml             # Configuração do pipeline CI/CD com GitHub Actions

Instalação e Configuração

1. Clonar o repositório

git clone <URL_DO_REPOSITORIO>
cd <NOME_DO_PROJETO>

2. Criar um ambiente virtual (opcional, mas recomendado)

python -m venv venv
source venv/bin/activate  # No Windows, use: venv\Scripts\activate

3. Instalar dependências

pip install -r requirements.txt

4. Configurar o Google Cloud

Criar um projeto no Google Cloud e ativar as APIs necessárias (BigQuery, Secret Manager, Cloud Run, Cloud Scheduler).

Criar e armazenar as credenciais no Secret Manager:

Chave da API CoinGecko

Credenciais do BigQuery

5. Executar o projeto localmente

python main.py

Deploy no Google Cloud Run

1. Autenticar no Google Cloud

gcloud auth login
gcloud config set project <SEU_PROJETO_ID>

2. Criar imagem Docker e fazer deploy no Cloud Run

gcloud builds submit --tag gcr.io/<SEU_PROJETO_ID>/cripto-app

gcloud run deploy cripto-app --image gcr.io/<SEU_PROJETO_ID>/cripto-app --platform managed --region us-central1 --allow-unauthenticated

CI/CD com GitHub Actions

O projeto inclui um arquivo deploy.yml para automatizar o deploy no Google Cloud Run usando GitHub Actions. O pipeline é acionado automaticamente a cada push na branch main.

Como configurar o GitHub Actions:

Criar os segredos no repositório GitHub:

GCP_CREDENTIALS: Chave JSON do Google Cloud Service Account

O pipeline irá:

Configurar o Google Cloud SDK

Criar o arquivo de credenciais

Construir e enviar a imagem Docker para o Google Container Registry

Implantar o serviço no Cloud Run

Exibir a URL do serviço

Agendamento com Cloud Scheduler

Para rodar o código automaticamente a cada 1 hora, crie um job no Cloud Scheduler que faça uma requisição HTTP para a URL do Cloud Run:

gcloud scheduler jobs create http executar-cripto-app \
    --schedule "0 * * * *" \
    --uri "https://<URL_DO_CLOUD_RUN>" \
    --http-method=GET

Autor

Este projeto foi desenvolvido com o objetivo de aprender e aprimorar conhecimentos sobre APIs, manipulação de dados e ferramentas da Google Cloud Platform.
