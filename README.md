# 📊 Projeto Final - Pipeline de Dados e Machine Learning

## 🎯 Visão Geral

Este projeto implementa um pipeline completo de dados e machine learning para análise de recompra de clientes, utilizando Apache Airflow para orquestração, Hadoop HDFS para armazenamento distribuído, e Streamlit para visualização dos resultados.

## 🏗️ Arquitetura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   Apache        │    │   SQL Server    │
│   (Origem)      │───▶│   Airflow       │───▶│   (Destino)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Hadoop HDFS   │
                       │   (Storage)     │
                       └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Streamlit     │
                       │   (Dashboard)   │
                       └─────────────────┘
```

## 🚀 Funcionalidades

### ETL Pipeline
- **Extração**: Dados do PostgreSQL (sistema transacional)
- **Transformação**: Limpeza e modelagem dimensional
- **Carga**: Data Warehouse no SQL Server

### Machine Learning
- **Problema**: Previsão de recompra em 90 dias
- **Modelos**: Logistic Regression, Random Forest, Gradient Boosting
- **Avaliação**: ROC AUC, Accuracy, Precision, Recall, F1-Score

### Visualização
- Dashboard interativo com Streamlit
- Comparação de modelos
- Simulador de previsões

## 📁 Estrutura do Projeto

```
projeto_final/
├── dags/                           # DAGs do Airflow
│   ├── check_db_conexao.py        # Verificação de conexões
│   ├── dag_gerar_slice_analitico.py # Geração do slice analítico
│   ├── dag_treinamento_modelos.py  # Treinamento de ML
│   ├── dim*.py                     # ETL das dimensões
│   └── fato*.py                    # ETL das tabelas fato
├── data/                           # Dados processados
│   ├── analytical_slice/           # Dados para ML (particionado)
│   ├── feature_store/              # Features preparadas
│   └── models/                     # Modelos treinados
├── logs/                           # Logs do Airflow
├── init-db.sh/                     # Scripts de inicialização
├── docker-compose.yaml             # Orquestração dos containers
├── dashboard.py                    # Dashboard Streamlit
└── requirements.txt                # Dependências Python
```

## 🛠️ Tecnologias Utilizadas

- **Orquestração**: Apache Airflow
- **Bancos de Dados**: PostgreSQL, SQL Server
- **Big Data**: Hadoop HDFS
- **Machine Learning**: Scikit-learn
- **Visualização**: Streamlit
- **Containerização**: Docker & Docker Compose
- **Linguagem**: Python

## 📋 Pré-requisitos

- Docker e Docker Compose
- 8GB+ de RAM disponível
- Portas livres: 8080 (Airflow), 8501 (Streamlit), 5432 (PostgreSQL), 1450 (SQL Server)

## 🚀 Como Executar

### 1. Configuração Inicial

```bash
# Clone o repositório
git clone <repository-url>
cd projeto_final

# Configure as variáveis de ambiente
cp .env.example .env
# Edite o arquivo .env com suas configurações
```

### 2. Subir os Serviços

```bash
# Iniciar todos os containers
docker-compose up -d

# Verificar status dos serviços
docker-compose ps
```

### 3. Acessar as Interfaces

- **Airflow**: http://localhost:8080
- **Streamlit Dashboard**: http://localhost:8501
- **Hadoop NameNode**: http://localhost:9870

### 4. Executar o Pipeline

1. Acesse o Airflow (usuário/senha definidos no .env)
2. Execute as DAGs na seguinte ordem:
   - `01_check_db_connections`
   - `etl_dim*` (dimensões)
   - `etl_fato*` (tabelas fato)
   - `dag_gerar_slice_analitico_para_ml`
   - `dag_treinamento_modelo_recompra_90d`

## 📊 DAGs Disponíveis

### Verificação e ETL
- **check_db_conexao**: Testa conectividade com bancos
- **etl_dimcliente**: ETL da dimensão cliente
- **etl_dimproduto**: ETL da dimensão produto
- **etl_dimvendedor**: ETL da dimensão vendedor
- **etl_dimfornecedor**: ETL da dimensão fornecedor
- **etl_dimformapagamento**: ETL da dimensão forma pagamento
- **etl_dimsituacaotitulo**: ETL da dimensão situação título

### Tabelas Fato
- **etl_fatovenda**: ETL das vendas
- **etl_fatocontasreceber**: ETL contas a receber
- **etl_fatocontaspagar**: ETL contas a pagar

### Machine Learning
- **dag_gerar_slice_analitico**: Prepara dados para ML
- **dag_treinamento_modelo_recompra_90d**: Treina modelos preditivos

## 🤖 Modelos de Machine Learning

### Problema de Negócio
Prever se um cliente fará uma nova compra nos próximos 90 dias após uma transação.

### Features Utilizadas
- Valor total da venda
- Quantidade de itens
- Valor da parcela
- Número da parcela
- Tipo do cliente (física/jurídica)
- Situação do título
- Forma de pagamento

### Modelos Testados
1. **Logistic Regression**: Modelo linear interpretável
2. **Random Forest**: Ensemble de árvores de decisão
3. **Gradient Boosting**: Boosting sequencial

### Métricas de Avaliação
- **ROC AUC**: Métrica principal para seleção
- **Accuracy**: Precisão geral
- **Precision**: Evita falsos positivos
- **Recall**: Captura verdadeiros positivos
- **F1-Score**: Balanceamento precision/recall

## 📈 Dashboard

O dashboard Streamlit oferece:

1. **Comparação de Modelos**: Visualização das métricas
2. **Análise do Melhor Modelo**: Detalhes e importância das features
3. **Simulador**: Interface para fazer previsões em tempo real

## 🔧 Configuração Avançada

### Variáveis de Ambiente (.env)
```env
# Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# PostgreSQL (Source)
POSTGRES_HOST=host.docker.internal
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=database

# SQL Server (Target)
MSSQL_PASSWORD=YourStrong@Passw0rd
```

### Volumes Docker
- `postgres-db-volume`: Dados do PostgreSQL
- `sqlserver-data`: Dados do SQL Server
- `hadoop-namenode`: Metadados HDFS
- `hadoop-datanode`: Dados HDFS

## 🐛 Troubleshooting

### Problemas Comuns

1. **Containers não sobem**:
   ```bash
   docker-compose down -v
   docker-compose up -d
   ```

2. **Erro de conexão com banco**:
   - Verifique as variáveis no .env
   - Execute a DAG `check_db_conexao`

3. **Falta de memória**:
   - Aumente recursos do Docker
   - Monitore com `docker stats`

4. **Portas ocupadas**:
   - Altere as portas no docker-compose.yaml
   - Verifique com `netstat -tulpn`

## 📝 Logs e Monitoramento

- **Airflow Logs**: `logs/dag_id/run_id/task_id/`
- **Container Logs**: `docker-compose logs <service>`
- **Hadoop Logs**: Interface web do NameNode

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.

## 👥 Autores

- **Israel** -
- **Christian**-
- **JoaoPedro**-

## 🙏 Agradecimentos

- Digital College - Curso Python para Dados
- Comunidade Apache Airflow
- Documentação Scikit-learn
