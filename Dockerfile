FROM python:3.11-slim AS builder

WORKDIR /app

# Creiamo un ambiente virtuale che copieremo
RUN python -m venv /opt/venv

## Usa un'immagine ufficiale di Python come base
FROM python:3.10-slim

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Copia il file requirements.txt (che creeremo) nella directory di lavoro
COPY requirements.txt .

# CRITICO PER PYSPARK E PER COMPILARE ALTRE LIBRERIE SCIENTIFICHE
# 1. Aggiorna e installa Java Runtime Environment (JRE)
# 2. Installa gli strumenti di compilazione essenziali
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    build-essential \
    libgfortran5 \
    libopenblas-dev \
    pkg-config && \
    rm -rf /var/lib/apt/lists/*
# Installa tutte le dipendenze Python specificate
RUN pip install --no-cache-dir -r requirements.txt

# Imposta la variabile d'ambiente JAVA_HOME
# Questo è necessario affinché PySpark trovi l'installazione Java
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64" >> /etc/bash.bashrc
ENV SPARK_EXECUTOR_OPTS "-Xss32m"

#  Aumenta ULTERIORMENTE lo Stack Size per il Driver
ENV SPARK_DRIVER_OPTS "-Xss32m"
# Copia il resto del codice sorgente dell'applicazione
COPY . .

ENV PYTHONUNBUFFERED 1
# 5. Comando di esecuzione
# Specifica il comando da eseguire quando il container si avvia.
# Sostituisci "il_tuo_script.py" con il nome del tuo file Python principale.
CMD ["python", "main.py"]