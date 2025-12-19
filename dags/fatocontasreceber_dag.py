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

# Definição dos Staging Paths (Local Filesystem)
RAW_STAGING_PATH = "/tmp/contas_receber_raw.csv" 
TRANSFORMED_STAGING_PATH = "/tmp/contas_receber_transformed.csv"

@dag(
    dag_id="etl_fatocontasreceber_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "contas_receber", "upsert"],
)
def etl_fato_contas_receber_upsert():
    
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
    @task
    def extract_postgres_to_file():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT
                cr.id AS id_conta_receber, cr.id_situacao, cr.id_forma_pagamento, nf.id_cliente, 
                cr.valor_original, cr.valor_atual, p.valor AS valor_parcela,
                CAST(cr.vencimento AS DATE) AS data_vencimento, 
                CAST(cr.data_recebimento AS DATE) AS data_recebimento,
                p.numero AS numero_parcela, nf.numero_nf
            FROM financeiro.conta_receber cr
            INNER JOIN vendas.parcela p ON p.id = cr.id_parcela
            INNER JOIN vendas.nota_fiscal nf ON nf.id = p.id_nota_fiscal;
        """
        print("✅ Extraindo FatoContasAReceber (Upsert) do PostgreSQL.")
        df = pg.get_pandas_df(sql)
        
        if df.empty:
            print("A extração do PostgreSQL não retornou dados. Finalizando o pipeline.")
            return None
        
        df.to_csv(RAW_STAGING_PATH, index=False)
        print(f"✅ Dados RAW salvos em: {RAW_STAGING_PATH}")
        
        return RAW_STAGING_PATH

# --------------------------------------------------------------------------------------
    @task
    def transform_and_lookup(raw_staging_path):
        if raw_staging_path is None: return None

        if not os.path.exists(raw_staging_path):
             raise FileNotFoundError(f"Arquivo RAW de Staging não encontrado em {raw_staging_path}")
             
        df = pd.read_csv(raw_staging_path)
        print(f"➡️ Carregados {len(df)} registros do Staging File RAW para transformação.")
        
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        
        # Lookups
        lookup_situacao = get_lookup_dict(mssql, "DimSituacaoTitulo", "id_dim_situacao", "id_dim_situacao")
        lookup_formapagto = get_lookup_dict(mssql, "DimFormaPagamento", "id_dim_forma_pagamento", "id_dim_forma_pagamento")
        lookup_cliente = get_lookup_dict(mssql, "DimCliente", "id_dim_cliente", "id_dim_cliente")
        lookup_tempo = get_lookup_dict(mssql, "DimTempo", "data", "id_dim_tempo")

        # Conversão e Mapeamento
        df['data_vencimento_str'] = df['data_vencimento'].astype(str)
        df['data_recebimento_str'] = df['data_recebimento'].astype(str).replace({'None': np.nan})
        
        df['id_dim_situacao'] = df['id_situacao'].map(lookup_situacao).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagamento'].map(lookup_formapagto).fillna(-1).astype(int)
        df['id_dim_cliente'] = df['id_cliente'].map(lookup_cliente).fillna(-1).astype(int)
        df['id_dim_tempo_vencimento'] = df['data_vencimento_str'].map(lookup_tempo).fillna(-1).astype(int)
        df['id_dim_tempo_recebimento'] = df['data_recebimento_str'].map(lookup_tempo).fillna(np.nan) 
        
        # Seleção de colunas finais e aplicação de filtros de integridade
        df_final = df[[
            'id_conta_receber', 
            'id_dim_situacao', 'id_dim_forma_pagamento', 'id_dim_cliente', 
            'id_dim_tempo_vencimento', 'id_dim_tempo_recebimento',
            'valor_parcela', 'valor_original', 'valor_atual', 'numero_parcela', 'numero_nf'
        ]].copy()
        
        # Renomear coluna para corresponder à tabela de destino
        df_final.rename(columns={'id_conta_receber': 'id_dim_conta_receber'}, inplace=True)

        df_final = df_final[
            (df_final['id_dim_situacao'] != -1) & 
            (df_final['id_dim_cliente'] != -1) & 
            (df_final['id_dim_tempo_vencimento'] != -1)
        ]
        
        df_final.to_csv(TRANSFORMED_STAGING_PATH, index=False)
        print(f"✅ Transformação concluída. Registros prontos: {len(df_final)}. Salvando em: {TRANSFORMED_STAGING_PATH}")
        
        return TRANSFORMED_STAGING_PATH

# --------------------------------------------------------------------------------------
    @task
    def upsert_mssql(transformed_staging_path):
        
        def clean_staging_files():
            if os.path.exists(RAW_STAGING_PATH): os.remove(RAW_STAGING_PATH)
            if os.path.exists(transformed_staging_path): os.remove(transformed_staging_path)

        if transformed_staging_path is None or not os.path.exists(transformed_staging_path):
             clean_staging_files()
             return "Carga interrompida: Nenhum dado novo encontrado ou arquivo não existe."
             
        df_final = pd.read_csv(transformed_staging_path)
        rows_to_process = df_final.to_dict(orient="records")
        
        if not rows_to_process: 
            clean_staging_files()
            return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute("USE DataWarehouse;")

        print(f"✅ Processando {len(rows_to_process)} registros com UPSERT na FatoContasAReceber...")
        
        # UPSERT usando MERGE
        upsert_sql = """
            MERGE DataWarehouse.dbo.FatoContasAReceber AS target
            USING (VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)) AS source 
                (id_dim_conta_receber, id_dim_situacao, id_dim_forma_pagamento, id_dim_cliente,
                 id_dim_tempo_vencimento, id_dim_tempo_recebimento,
                 valor_parcela, valor_original, valor_atual, numero_parcela, numero_nf)
            ON target.id_dim_conta_receber = source.id_dim_conta_receber
            WHEN MATCHED THEN
                UPDATE SET 
                    id_dim_situacao = source.id_dim_situacao,
                    id_dim_forma_pagamento = source.id_dim_forma_pagamento,
                    id_dim_cliente = source.id_dim_cliente,
                    id_dim_tempo_vencimento = source.id_dim_tempo_vencimento,
                    id_dim_tempo_recebimento = source.id_dim_tempo_recebimento,
                    valor_parcela = source.valor_parcela,
                    valor_original = source.valor_original,
                    valor_atual = source.valor_atual,
                    numero_parcela = source.numero_parcela,
                    numero_nf = source.numero_nf
            WHEN NOT MATCHED THEN
                INSERT (id_dim_conta_receber, id_dim_situacao, id_dim_forma_pagamento, id_dim_cliente,
                        id_dim_tempo_vencimento, id_dim_tempo_recebimento,
                        valor_parcela, valor_original, valor_atual,
                        numero_parcela, numero_nf)
                VALUES (source.id_dim_conta_receber, source.id_dim_situacao, source.id_dim_forma_pagamento, 
                        source.id_dim_cliente, source.id_dim_tempo_vencimento, source.id_dim_tempo_recebimento,
                        source.valor_parcela, source.valor_original, source.valor_atual,
                        source.numero_parcela, source.numero_nf);
        """

        rows_affected = 0
        try:
            for row in rows_to_process:
                id_recebimento = row["id_dim_tempo_recebimento"]
                if pd.isna(id_recebimento): id_recebimento = None
                else: id_recebimento = int(id_recebimento)

                cursor.execute(upsert_sql, (
                    int(row["id_dim_conta_receber"]),
                    int(row["id_dim_situacao"]),
                    int(row["id_dim_forma_pagamento"]),
                    int(row["id_dim_cliente"]),
                    int(row["id_dim_tempo_vencimento"]),
                    id_recebimento,
                    row["valor_parcela"],
                    row["valor_original"],
                    row["valor_atual"],
                    row["numero_parcela"],
                    row["numero_nf"]
                ))
                rows_affected += 1
            
            conn.commit()
            print(f"✅ UPSERT concluído: {rows_affected} registros processados.")

        except Exception as e:
            conn.rollback()
            print(f"❌ ERRO DE UPSERT NO SQL SERVER: {e}", file=sys.stderr)
            raise e 

        finally:
            cursor.close()
            conn.close()
            clean_staging_files()

        return f"UPSERT concluído: {rows_affected} registros processados."

    # ORQUESTRAÇÃO
    task_extract = extract_postgres_to_file()
    task_transform = transform_and_lookup(task_extract)
    task_load = upsert_mssql(task_transform)

    task_extract >> task_transform >> task_load

etl_fato_contas_receber_upsert()
