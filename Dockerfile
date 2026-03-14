# Define a imagem base (neste caso, um ambiente com uma versão mínima do Python)
FROM python:3.10-slim

# Copia os binários do uv da imagem oficial
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Define uma pasta de trabalho dentro do contêiner
WORKDIR /app

# Adiciona um ambiente virtual ao PATH (isso nos desobriga de executar os comandos com `uv run`)
ENV PATH="/app/.venv/bin:$PATH"

# Copia os arquivos necessários para configurarmos o ambiente no container
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Instala as dependências
RUN uv sync --locked

# Copia o código do host para a imagem
COPY pipeline.py pipeline.py

# Ao criar o container com `docker run`, o comando `python pipeline.py` será executado 
ENTRYPOINT ["python", "pipeline.py"]

