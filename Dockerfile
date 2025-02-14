#Dockerfile Correto
# Escolhe uma imagem base com Python 3.12
FROM python:3.12-slim
 
# Define o diretório de trabalho
WORKDIR /app
 
# Copia o arquivo de requisitos e instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copia o restante do código da aplicação
COPY . .
 
# Define a porta que o Cloud Run espera (a variável de ambiente PORT é automaticamente configurada)
ENV PORT=8080
EXPOSE 8080
 
# Define o comando para iniciar a aplicação (equivalente ao que estava no Procfile)
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:8080", "--timeout", "120"]