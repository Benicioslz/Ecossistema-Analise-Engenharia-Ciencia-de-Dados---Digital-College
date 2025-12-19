from __future__ import annotations

import pandas as pd
import numpy as np
import pendulum
import joblib
import os
import json
from airflow.decorators import dag, task

# Bibliotecas de Machine Learning
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score, precision_score, recall_score

# Modelos a serem testados
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

# --- Constantes ---
DATA_PATH = "/opt/airflow/data/"
ANALYTICAL_SLICE_PATH = f"{DATA_PATH}analytical_slice/"
FEATURE_STORE_PATH = f"{DATA_PATH}feature_store/recompra_90d/"
MODELS_PATH = f"{DATA_PATH}models/recompra_90d/"
PRIMARY_METRIC = "roc_auc" # Métrica principal para escolher o melhor modelo

@dag(
    dag_id="dag_treinamento_modelo_recompra_90d",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,  # Idealmente acionado após a DAG do slice
    catchup=False,
    tags=['ml', 'training', 'classification'],
    doc_md="""
    ### DAG de Treinamento de Modelos para Recompra em 90 Dias

    Esta DAG realiza o ciclo completo de treinamento e avaliação de modelos:
    1. **load_and_prepare_data**: Carrega o slice analítico, cria a variável alvo e prepara os dados.
    2. **train_model_* (paralelo)**: Treina e avalia diferentes algoritmos.
    3. **select_best_model**: Compara os resultados e elege o melhor modelo.
    """,
)
def treinamento_modelo_recompra_dag():

    @task
    def load_and_prepare_data() -> str:
        """
        Carrega os dados do Parquet, realiza a engenharia de features para o problema de 
        recompra e salva os datasets de treino/teste.
        """
        print(f"➡️ Lendo dados do slice analítico de: {ANALYTICAL_SLICE_PATH}")
        # Pandas lê os diretórios particionados automaticamente
        df = pd.read_parquet(ANALYTICAL_SLICE_PATH, engine='pyarrow')

        # --- Engenharia de Features e Criação da Variável Alvo ---
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        df = df.sort_values(by=['id_dim_cliente', 'data_venda'])
        
        # Calcula a data da próxima compra para cada cliente
        df['proxima_compra'] = df.groupby('id_dim_cliente')['data_venda'].shift(-1)
        
        # Calcula a diferença em dias
        df['dias_para_proxima_compra'] = (df['proxima_compra'] - df['data_venda']).dt.days
        
        # Cria a variável alvo (target): recomprou em 90 dias?
        # 1 se sim, 0 se não. Se não houve próxima compra (NaT), consideramos 0.
        df['target_recompra_90d'] = np.where(df['dias_para_proxima_compra'] <= 90, 1, 0)
        
        print("✅ Variável alvo 'target_recompra_90d' criada.")
        print(df['target_recompra_90d'].value_counts(normalize=True))

        # --- Preparação dos Dados para o Modelo ---
        # Seleciona features iniciais (excluindo o que não deve entrar no modelo)
        features_to_use = [
            'valor_total_venda', 'quantidade_total_itens', 'valor_parcela', 
            'numero_parcela', 'tipo_cliente', 'situacao_titulo', 'descricao_forma_pagamento'
        ]
        target = 'target_recompra_90d'

        # Remove linhas onde o target não pode ser calculado (última compra de cada cliente)
        df_model = df.dropna(subset=['dias_para_proxima_compra'])
        
        X = df_model[features_to_use]
        y = df_model[target]

        # Divide em treino e teste
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

        # Garante que os diretórios de saída existam
        os.makedirs(FEATURE_STORE_PATH, exist_ok=True)

        # Salva os datasets para as próximas tasks
        X_train.to_parquet(f"{FEATURE_STORE_PATH}X_train.parquet")
        X_test.to_parquet(f"{FEATURE_STORE_PATH}X_test.parquet")
        y_train.to_frame().to_parquet(f"{FEATURE_STORE_PATH}y_train.parquet")
        y_test.to_frame().to_parquet(f"{FEATURE_STORE_PATH}y_test.parquet")

        print(f"✅ Datasets de treino e teste salvos em: {FEATURE_STORE_PATH}")
        return FEATURE_STORE_PATH

    @task
    def train_model(model_name: str, model_instance, feature_store_path: str) -> dict:
        """
        Função genérica para treinar e avaliar um modelo.
        """
        print(f"--- Treinando Modelo: {model_name} ---")
        
        # Carrega os dados
        X_train = pd.read_parquet(f"{feature_store_path}X_train.parquet")
        X_test = pd.read_parquet(f"{feature_store_path}X_test.parquet")
        y_train = pd.read_parquet(f"{feature_store_path}y_train.parquet").squeeze()
        y_test = pd.read_parquet(f"{feature_store_path}y_test.parquet").squeeze()

        # Define as colunas categóricas e numéricas
        categorical_features = X_train.select_dtypes(include=['object', 'category']).columns
        numerical_features = X_train.select_dtypes(include=np.number).columns

        # Cria um pipeline de pré-processamento
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', 'passthrough', numerical_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ])

        # Cria o pipeline final com o modelo
        pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                   ('classifier', model_instance)])
        
        # Treina o modelo
        pipeline.fit(X_train, y_train)
        
        # Faz predições
        y_pred = pipeline.predict(X_test)
        y_pred_proba = pipeline.predict_proba(X_test)[:, 1]

        # Calcula métricas
        metrics = {
            "model_name": model_name,
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
            "f1_score": f1_score(y_test, y_pred),
            "roc_auc": roc_auc_score(y_test, y_pred_proba)
        }
        
        print(f"✅ Métricas para {model_name}:")
        for key, value in metrics.items():
            if key != "model_name":
                print(f"  - {key}: {value:.4f}")

        # Salva o pipeline (pré-processador + modelo)
        os.makedirs(MODELS_PATH, exist_ok=True)
        model_path = f"{MODELS_PATH}{model_name}.joblib"
        joblib.dump(pipeline, model_path)
        print(f"✅ Modelo salvo em: {model_path}")

        return metrics

    @task
    def select_best_model(model_metrics: list):
        """
        Recebe as métricas de todos os modelos e seleciona o melhor.
        """
        if not model_metrics:
            raise ValueError("Nenhuma métrica de modelo foi recebida.")
        
        print("--- Selecionando o Melhor Modelo ---")
        
        best_model = max(model_metrics, key=lambda x: x[PRIMARY_METRIC])
        
        print(f"🏆 Melhor modelo baseado em '{PRIMARY_METRIC}': {best_model['model_name']}")
        print("Métricas do melhor modelo:")
        for key, value in best_model.items():
            if key != "model_name":
                print(f"  - {key}: {value:.4f}")
        
        # Opcional: Salvar um arquivo indicando qual é o melhor modelo
        with open(f"{MODELS_PATH}best_model.txt", "w") as f:
            f.write(best_model['model_name'])
            
        # ✅ NOVO: Salva as métricas de todos os modelos para o dashboard
        metrics_path = f"{MODELS_PATH}model_comparison_metrics.json"
        with open(metrics_path, "w") as f:
            json.dump(model_metrics, f, indent=4)
        print(f"✅ Métricas de comparação salvas em: {metrics_path}")
            
        return best_model

    # --- Orquestração da DAG ---
    
    # 1. Prepara os dados
    feature_store_path = load_and_prepare_data()
    
    # 2. Define os modelos a serem treinados
    models_to_train = {
        "logistic_regression": LogisticRegression(random_state=42, class_weight='balanced', max_iter=1000),
        "random_forest": RandomForestClassifier(random_state=42, class_weight='balanced', n_estimators=150),
        "gradient_boosting": GradientBoostingClassifier(random_state=42, n_estimators=150)
    }
    
    # 3. Treina todos os modelos em paralelo e coleta as métricas
    all_metrics = []
    for name, model in models_to_train.items():
        metrics = train_model(name, model, feature_store_path)
        all_metrics.append(metrics)
        
    # 4. Seleciona o melhor modelo após todos terminarem
    select_best_model(all_metrics)


treinamento_modelo_recompra_dag()
