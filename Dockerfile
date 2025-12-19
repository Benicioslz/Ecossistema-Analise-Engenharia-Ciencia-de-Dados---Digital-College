# Usa a imagem oficial do Airflow como base
FROM apache/airflow:2.9.3-python3.10

USER root

# 1. Instalação Geral
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        gnupg \
        apt-transport-https \
        unixodbc \
        unixodbc-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        python3-dev \
        gcc \
        openjdk-17-jre-headless \
        ca-certificates \
        tar && \
    # Configuração do Repositório da Microsoft
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    # Instalação dos Drivers MSSQL
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 && \
    # Limpeza
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Configura Variáveis do MSSQL Tools no PATH
ENV PATH=$PATH:/opt/mssql-tools18/bin

# 3. Instala Cliente Hadoop (3.2.1)
ENV HADOOP_VERSION=3.2.1
ENV HADOOP_HOME=/opt/hadoop
RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
    tar -xz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME

# 4. Configura Variáveis de Ambiente (Java e Hadoop)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$HADOOP_HOME/bin

# 5. Copia o requirements.txt
COPY requirements.txt /requirements.txt

# 6. Volta para o usuário airflow para instalar pacotes Python
USER airflow

# 7. Instala os providers e dependências
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt" \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-hdfs \
    -r /requirements.txt