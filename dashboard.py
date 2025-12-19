import streamlit as st
import pandas as pd
import joblib
import json
import os

# --- Constantes ---
# Caminhos para os artefatos do modelo (ajuste se necessário)
MODELS_PATH = "data/models/recompra_90d/"
BEST_MODEL_NAME_FILE = os.path.join(MODELS_PATH, "best_model.txt")
METRICS_FILE = os.path.join(MODELS_PATH, "model_comparison_metrics.json")

# --- Funções Auxiliares ---

@st.cache_data
def load_metrics():
    """Carrega o JSON com as métricas de comparação dos modelos."""
    if os.path.exists(METRICS_FILE):
        with open(METRICS_FILE, 'r') as f:
            return json.load(f)
    return None

@st.cache_resource
def load_best_model():
    """Carrega o melhor modelo treinado (pipeline completo)."""
    if os.path.exists(BEST_MODEL_NAME_FILE):
        with open(BEST_MODEL_NAME_FILE, 'r') as f:
            best_model_name = f.read().strip()
        
        model_file = os.path.join(MODELS_PATH, f"{best_model_name}.joblib")
        if os.path.exists(model_file):
            pipeline = joblib.load(model_file)
            return best_model_name, pipeline
    return None, None

def get_feature_importances(pipeline, model_name):
    """Extrai a importância das features se o modelo for baseado em árvore."""
    if 'forest' in model_name or 'boosting' in model_name:
        # Extrai o nome das features após o OneHotEncoding
        try:
            feature_names = pipeline.named_steps['preprocessor'].get_feature_names_out()
            importances = pipeline.named_steps['classifier'].feature_importances_
            
            df_importances = pd.DataFrame({
                'Feature': feature_names,
                'Importance': importances
            }).sort_values(by='Importance', ascending=False)
            
            return df_importances
        except Exception as e:
            st.warning(f"Não foi possível extrair a importância das features: {e}")
    return None

# --- Layout do Painel ---

st.set_page_config(page_title="Painel de Resultados - Modelo Preditivo", layout="wide")

st.title("📈 Painel de Análise do Modelo Preditivo")
st.subheader("Problema de Negócio: Previsão de Recompra em 90 dias")

# --- Carregamento dos Dados ---
metrics_data = load_metrics()
best_model_name, best_model_pipeline = load_best_model()

if not metrics_data or not best_model_pipeline:
    st.error(
        "**Arquivos de modelo não encontrados!** "
        "Por favor, execute a DAG `dag_treinamento_modelo_recompra_90d` no Airflow primeiro."
    )
else:
    st.success(f"Resultados carregados com sucesso. O melhor modelo identificado foi: **{best_model_name.replace('_', ' ').title()}**")

    # --- Seção 1: Comparação de Modelos ---
    st.header("📊 Comparação de Performance dos Modelos")
    
    df_metrics = pd.DataFrame(metrics_data).set_index('model_name')
    st.dataframe(df_metrics.style.highlight_max(axis=0, color='lightgreen').format("{:.4f}"))
    
    st.write("""
    **Métricas:**
    - **ROC AUC:** Habilidade do modelo em distinguir entre as classes (quanto maior, melhor). **Métrica principal para seleção.**
    - **Accuracy:** Percentual de predições corretas.
    - **Precision:** Das predições positivas, quantas estavam corretas. Importante para evitar falsos positivos.
    - **Recall:** Dos positivos reais, quantos foram capturados pelo modelo. Importante para evitar falsos negativos.
    - **F1-Score:** Média harmônica entre Precision e Recall.
    """)

    # --- Seção 2: Análise do Melhor Modelo ---
    st.header(f"深入 Análise do Melhor Modelo: {best_model_name.replace('_', ' ').title()}")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Métricas Detalhadas")
        best_metrics = df_metrics.loc[best_model_name]
        st.json(best_metrics.to_dict())

    with col2:
        st.subheader("Importância das Features")
        df_importances = get_feature_importances(best_model_pipeline, best_model_name)
        if df_importances is not None:
            st.bar_chart(df_importances.set_index('Feature')['Importance'])
            st.write("Features mais importantes para a decisão do modelo.")
        else:
            st.info("A importância das features não é aplicável para este tipo de modelo (ex: Regressão Logística).")

    # --- Seção 3: Simulação de Previsão ---
    st.header("🔮 Simulador de Previsão")
    st.write("Preencha os dados abaixo para simular uma previsão com o melhor modelo.")

    with st.form("prediction_form"):
        # Coleta de inputs do usuário (baseado nas features do modelo)
        valor_total_venda = st.number_input("Valor Total da Venda", min_value=0.0, value=100.0, step=10.0)
        quantidade_total_itens = st.number_input("Quantidade de Itens", min_value=1, value=2, step=1)
        valor_parcela = st.number_input("Valor da Parcela", min_value=0.0, value=50.0, step=10.0)
        numero_parcela = st.number_input("Número da Parcela", min_value=1, value=1, step=1)
        tipo_cliente = st.selectbox("Tipo do Cliente", options=['fisica', 'juridica'])
        situacao_titulo = st.selectbox("Situação do Título", options=['pago', 'pendente', 'atrasado']) # Ajuste as opções
        descricao_forma_pagamento = st.selectbox("Forma de Pagamento", options=['cartao', 'boleto', 'pix']) # Ajuste as opções

        submitted = st.form_submit_button("Realizar Previsão")

        if submitted:
            # Cria um DataFrame com os dados do formulário
            input_data = pd.DataFrame([{
                'valor_total_venda': valor_total_venda,
                'quantidade_total_itens': quantidade_total_itens,
                'valor_parcela': valor_parcela,
                'numero_parcela': numero_parcela,
                'tipo_cliente': tipo_cliente,
                'situacao_titulo': situacao_titulo,
                'descricao_forma_pagamento': descricao_forma_pagamento
            }])

            # Realiza a predição
            prediction = best_model_pipeline.predict(input_data)[0]
            prediction_proba = best_model_pipeline.predict_proba(input_data)[0]

            if prediction == 1:
                st.success(f"**Previsão: Cliente RECOMPRARÁ em 90 dias.** (Probabilidade: {prediction_proba[1]:.2%})")
            else:
                st.error(f"**Previsão: Cliente NÃO recomprará em 90 dias.** (Probabilidade: {prediction_proba[0]:.2%})")

