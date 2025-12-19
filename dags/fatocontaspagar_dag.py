from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd
import numpy as np
import os
import sys

# Configurações de Conexão e Staging
POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

# Definição dos Staging Paths específicos para Contas a Pagar
RAW_STAGING_PATH_CP = "/tmp/contas_pagar_raw.csv" 
TRANSFORMED_STAGING_PATH_CP = "/tmp/contas_pagar_transformed.csv"

@dag(
    dag_id="etl_fatocontaspagar_pg_to_mssql", 
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "contas_pagar", "upsert"],
)
def etl_fato_contas_pagar_upsert():
    
    def get_lookup_dict(mssql_hook, table_name, business_key, surrogate_key):
        """Função auxiliar para obter dicionários de lookup de Dimensões."""
        # Quando business_key == surrogate_key, selecionar apenas uma coluna
        # para evitar DataFrame com colunas duplicadas que causa erro.
        if business_key == surrogate_key:
            df_lookup = mssql_hook.get_pandas_df(f"SELECT {business_key} FROM DataWarehouse.dbo.{table_name};")
            if df_lookup.empty:
                print(f"Aviso: Tabela de Dimensão {table_name} está vazia.")
                return {}
            # Criar dicionário de identidade (chave = valor)
            return {v: v for v in df_lookup[business_key].tolist()}
        else:
            df_lookup = mssql_hook.get_pandas_df(f"SELECT {business_key}, {surrogate_key} FROM DataWarehouse.dbo.{table_name};")
            if df_lookup.empty:
                print(f"Aviso: Tabela de Dimensão {table_name} está vazia.")
                return {}
            
            if business_key == 'data':
                df_lookup[business_key] = df_lookup[business_key].astype(str)
            
            return pd.Series(df_lookup[surrogate_key].values, index=df_lookup[business_key]).to_dict()

# --------------------------------------------------------------------------------------
## 1. Extração: PostgreSQL (Consulta Simples)
    @task
    def extract_postgres_to_file():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT
                cp.id AS id_conta_pagar,
                cp.id_situacao,
                cp.valor_original,
                cp.valor_atual,
                cp.emissao AS data_emissao,        
                cp.vencimento AS data_vencimento, 
                cp.data_pagamento,
                cp.documento,                      
                cp.id_forma_pagamento,
                cp.descricao AS descricao_pagamento 
            FROM financeiro.conta_pagar cp;
        """
        print("✅ Extraindo FatoContasAPagar (Upsert) do PostgreSQL.")
        df = pg.get_pandas_df(sql)
        
        if df.empty:
            raise Exception("A extração do PostgreSQL não retornou dados. Terminando o pipeline.")
        
        df.to_csv(RAW_STAGING_PATH_CP, index=False)
        print(f"✅ Dados RAW salvos em: {RAW_STAGING_PATH_CP}")
        
        return RAW_STAGING_PATH_CP

# --------------------------------------------------------------------------------------
## 2. Transformação & Lookup
    @task
    def transform_and_lookup(raw_staging_path):
        if not os.path.exists(raw_staging_path):
             raise FileNotFoundError(f"Arquivo RAW de Staging não encontrado em {raw_staging_path}")
             
        df = pd.read_csv(raw_staging_path)
        print(f"➡️ Carregados {len(df)} registros do Staging File RAW para transformação.")
        
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        mssql.run("USE DataWarehouse;")
        
        # Lookups necessários
        lookup_situacao = get_lookup_dict(mssql, "DimSituacaoTitulo", "id_dim_situacao", "id_dim_situacao")
        lookup_formapagto = get_lookup_dict(mssql, "DimFormaPagamento", "id_dim_forma_pagamento", "id_dim_forma_pagamento")
        lookup_tempo = get_lookup_dict(mssql, "DimTempo", "data", "id_dim_tempo")

        # Mapeamento de Chaves Substitutas
        df['data_vencimento_str'] = df['data_vencimento'].astype(str)
        df['data_pagamento_str'] = df['data_pagamento'].astype(str).replace({'None': np.nan}) 
        df['data_emissao_str'] = df['data_emissao'].astype(str) 

        df['id_dim_situacao'] = df['id_situacao'].map(lookup_situacao).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagamento'].map(lookup_formapagto).fillna(-1).astype(int)
        
        df['id_dim_tempo_vencimento'] = df['data_vencimento_str'].map(lookup_tempo).fillna(-1).astype(int)
        df['id_dim_tempo_pagamento'] = df['data_pagamento_str'].map(lookup_tempo).fillna(np.nan) 
        df['id_dim_tempo_emissao'] = df['data_emissao_str'].map(lookup_tempo).fillna(-1).astype(int) 
        
        # Seleção de colunas finais
        df_final = df[[
            'id_conta_pagar', 
            'id_dim_situacao', 'id_dim_forma_pagamento',
            'id_dim_tempo_emissao', 'id_dim_tempo_vencimento', 'id_dim_tempo_pagamento',
            'valor_original', 'valor_atual', 'documento', 'descricao_pagamento'
        ]].copy()
        
        # Renomear coluna para corresponder à tabela de destino
        df_final.rename(columns={'id_conta_pagar': 'id_dim_conta_pagar'}, inplace=True)
        
        # Filtro de integridade
        df_final = df_final[
            (df_final['id_dim_situacao'] != -1) & 
            (df_final['id_dim_tempo_vencimento'] != -1) &
            (df_final['id_dim_tempo_emissao'] != -1)
        ]
        
        df_final.to_csv(TRANSFORMED_STAGING_PATH_CP, index=False)
        print(f"✅ Transformação concluída. Salvando em: {TRANSFORMED_STAGING_PATH_CP}")
        
        return TRANSFORMED_STAGING_PATH_CP

# --------------------------------------------------------------------------------------
## 3. Upsert: SQL Server
    @task
    def upsert_mssql(transformed_staging_path):
        
        def clean_staging_files():
            if os.path.exists(RAW_STAGING_PATH_CP): os.remove(RAW_STAGING_PATH_CP)
            if os.path.exists(transformed_staging_path): os.remove(transformed_staging_path)

        if not os.path.exists(transformed_staging_path):
             clean_staging_files()
             raise FileNotFoundError(f"Arquivo TRANSFORMADO de Staging não encontrado em {transformed_staging_path}")
             
        df_final = pd.read_csv(transformed_staging_path)
        rows_to_process = df_final.to_dict(orient="records")
        
        if not rows_to_process: 
            clean_staging_files()
            return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        print(f"✅ Processando {len(rows_to_process)} registros com UPSERT na FatoContasAPagar...")
    
        upsert_sql = """
            MERGE DataWarehouse.dbo.FatoContasAPagar AS target
            USING (VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)) AS source 
                (id_dim_conta_pagar, id_dim_situacao, id_dim_forma_pagamento, 
                 id_dim_tempo_emissao, id_dim_tempo_vencimento, id_dim_tempo_pagamento,
                 valor_original, valor_atual, documento, descricao_pagamento)
            ON target.id_dim_conta_pagar = source.id_dim_conta_pagar
            WHEN MATCHED THEN
                UPDATE SET 
                    id_dim_situacao = source.id_dim_situacao,
                    id_dim_forma_pagamento = source.id_dim_forma_pagamento,
                    id_dim_tempo_emissao = source.id_dim_tempo_emissao,
                    id_dim_tempo_vencimento = source.id_dim_tempo_vencimento,
                    id_dim_tempo_pagamento = source.id_dim_tempo_pagamento,
                    valor_original = source.valor_original,
                    valor_atual = source.valor_atual,
                    documento = source.documento,
                    descricao_pagamento = source.descricao_pagamento
            WHEN NOT MATCHED THEN
                INSERT (id_dim_conta_pagar, id_dim_situacao, id_dim_forma_pagamento, 
                        id_dim_tempo_emissao, id_dim_tempo_vencimento, id_dim_tempo_pagamento,
                        valor_original, valor_atual, documento, descricao_pagamento)
                VALUES (source.id_dim_conta_pagar, source.id_dim_situacao, source.id_dim_forma_pagamento,
                        source.id_dim_tempo_emissao, source.id_dim_tempo_vencimento, source.id_dim_tempo_pagamento,
                        source.valor_original, source.valor_atual, source.documento, source.descricao_pagamento);
        """

        rows_affected = 0
        try:
            for row in rows_to_process:
                id_pagamento = row["id_dim_tempo_pagamento"]
                if pd.isna(id_pagamento): id_pagamento = None
                else: id_pagamento = int(id_pagamento)

                cursor.execute(upsert_sql, (
                    int(row["id_dim_conta_pagar"]),
                    int(row["id_dim_situacao"]),
                    int(row["id_dim_forma_pagamento"]),
                    int(row["id_dim_tempo_emissao"]),
                    int(row["id_dim_tempo_vencimento"]),
                    id_pagamento,
                    row["valor_original"],
                    row["valor_atual"],
                    row["documento"],
                    row["descricao_pagamento"]
                ))
                rows_affected += 1
            
            conn.commit()
            print(f"✅ UPSERT concluído: {rows_affected} registros processados.")

        except Exception as e:
            conn.rollback()
            print(f"ERRO DE UPSERT NO SQL SERVER: {e}", file=sys.stderr)
            raise e 

        finally:
            cursor.close()
            conn.close()
            clean_staging_files()

        return f"UPSERT concluído: {rows_affected} registros processados."

    # Definição do fluxo
    task_extract = extract_postgres_to_file()
    task_transform = transform_and_lookup(task_extract)
    task_load = upsert_mssql(task_transform)

    task_extract >> task_transform >> task_load

etl_fato_contas_pagar_upsert()
