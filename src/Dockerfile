# Use uma imagem base oficial do Python
FROM python:3.10

# Define o diretório de trabalho no contêiner
WORKDIR /app

# Copia os arquivos do seu projeto para o contêiner
COPY . .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Define a variável de ambiente do Google Cloud
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/credenciais.json"

# Expor a porta do Flask (se aplicável)
EXPOSE 8080

# Comando para rodar a aplicação
CMD ["python", "main.py"]
