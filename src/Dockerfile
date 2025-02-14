# Usar uma imagem base com Python
FROM python:3.9-slim

# Definir o diretório de trabalho
WORKDIR /app

# Copiar os arquivos do projeto para o container
COPY . /app

# Definir a variável de ambiente com o caminho do arquivo de credenciais no container
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/credentials.json"

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Expor a porta que o Flask irá rodar
EXPOSE 8080

# Comando para iniciar a aplicação
CMD ["python", "main.py"]
