from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd
import numpy as np 
import os
import sys

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="etl_fatovenda_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "venda", "upsert"],
)
def etl_fato_venda_upsert():
    
    def get_lookup_dict(mssql_hook, table_name, business_key, surrogate_key):
        """Função auxiliar para obter dicionários de lookup de Dimensões."""
        # Se as chaves forem iguais, buscamos apenas uma coluna para evitar erro de duplicata no pandas
        if business_key == surrogate_key:
            sql = f"SELECT {business_key} FROM DataWarehouse.dbo.{table_name};"
            df_lookup = mssql_hook.get_pandas_df(sql)
            if df_lookup.empty: return {}
            return {v: v for v in df_lookup[business_key].tolist()}
        else:
            sql = f"SELECT {business_key}, {surrogate_key} FROM DataWarehouse.dbo.{table_name};"
            df_lookup = mssql_hook.get_pandas_df(sql)
            if df_lookup.empty: return {}
            
            if business_key == 'data':
                df_lookup[business_key] = df_lookup[business_key].astype(str)
            
            return pd.Series(df_lookup[surrogate_key].values, index=df_lookup[business_key]).to_dict()

    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT 
                nf.id_vendedor, 
                nf.id_cliente, 
                nf.id_forma_pagto, 
                CAST(nf.data_venda AS DATE) AS data_venda, 
                nf.numero_nf, 
                nf.valor AS valor_total_nf, 
                inf.id_produto, 
                inf.quantidade, 
                inf.valor_unitario, 
                inf.valor_venda_real,
                nf.id AS id_nota_fiscal,
                inf.id AS id_item_nf
            FROM 
                vendas.nota_fiscal nf
            LEFT JOIN 
                vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id;
        """
        df = pg.get_pandas_df(sql)
        return df.to_dict(orient="records")

    @task
    def transform_and_lookup(rows):
        if not rows: return []

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        df = pd.DataFrame(rows)
        
        # CORREÇÃO: Usando 'id_dim_vendedor' como business_key e surrogate_key 
        # já que o init.sql não define uma coluna separada para o ID de origem do vendedor.
        lookup_vendedor = get_lookup_dict(mssql, "DimVendedor", "id_dim_vendedor", "id_dim_vendedor")
        lookup_cliente = get_lookup_dict(mssql, "DimCliente", "id_dim_cliente", "id_dim_cliente")
        lookup_formapagto = get_lookup_dict(mssql, "DimFormaPagamento", "id_dim_forma_pagamento", "id_dim_forma_pagamento")
        lookup_produto = get_lookup_dict(mssql, "DimProduto", "id_dim_produto", "id_dim_produto")
        lookup_tempo = get_lookup_dict(mssql, "DimTempo", "data", "id_dim_tempo")

        df['data_venda_str'] = df['data_venda'].astype(str)
        
        # Mapeamento
        df['id_dim_vendedor'] = df['id_vendedor'].map(lookup_vendedor).fillna(-1).astype(int)
        df['id_dim_cliente'] = df['id_cliente'].map(lookup_cliente).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagto'].map(lookup_formapagto).fillna(-1).astype(int)
        df['id_dim_produto'] = df['id_produto'].map(lookup_produto).fillna(-1).astype(int)
        df['id_dim_tempo_venda'] = df['data_venda_str'].map(lookup_tempo).fillna(-1).astype(int)
        
        # Chave composta para a Fato
        df['chave_negocio'] = df['id_nota_fiscal'].astype(str) + '_' + df['id_item_nf'].astype(str)
        
        df_final = df[[
            'chave_negocio', 'id_dim_produto', 'id_dim_cliente', 'id_dim_vendedor', 
            'id_dim_forma_pagamento', 'id_dim_tempo_venda', 'numero_nf', 
            'valor_total_nf', 'quantidade', 'valor_unitario', 'valor_venda_real'
        ]].copy()
        
        # Filtro de integridade
        df_final = df_final[
            (df_final['id_dim_produto'] != -1) & 
            (df_final['id_dim_cliente'] != -1) & 
            (df_final['id_dim_tempo_venda'] != -1)
        ]
        
        return df_final.to_dict(orient="records")

    @task
    def upsert_mssql_optimized(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        
        try:
            # cursor.execute("USE DataWarehouse;")
            
            # 1. Criação da Tabela Temporária para Performance
            cursor.execute("""
                IF OBJECT_ID('tempdb..#VendaStaging') IS NOT NULL DROP TABLE #VendaStaging;
                CREATE TABLE #VendaStaging (
                    chave_negocio VARCHAR(100),
                    id_dim_produto INT,
                    id_dim_cliente INT,
                    id_dim_vendedor INT,
                    id_dim_forma_pagamento INT,
                    id_dim_tempo_venda INT,
                    numero_nf VARCHAR(50),
                    valor_total_nf DECIMAL(18,2),
                    quantidade INT,
                    valor_unitario DECIMAL(18,2),
                    valor_venda_real DECIMAL(18,2)
                );
            """)

            # 2. Carga em lote (Bulk Insert)
            insert_sql = """
                INSERT INTO #VendaStaging VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_to_insert = [
                (r['chave_negocio'], r['id_dim_produto'], r['id_dim_cliente'], r['id_dim_vendedor'],
                 r['id_dim_forma_pagamento'], r['id_dim_tempo_venda'], r['numero_nf'],
                 r['valor_total_nf'], r['quantidade'], r['valor_unitario'], r['valor_venda_real'])
                for r in rows
            ]
            cursor.executemany(insert_sql, data_to_insert)

            # 3. Execução do MERGE em bloco (Set-based operation)
            merge_sql = """
                MERGE DataWarehouse.dbo.FatoVenda AS target
                USING #VendaStaging AS source
                ON target.chave_negocio = source.chave_negocio
                WHEN MATCHED THEN
                    UPDATE SET 
                        id_dim_produto = source.id_dim_produto,
                        id_dim_cliente = source.id_dim_cliente,
                        id_dim_vendedor = source.id_dim_vendedor,
                        id_dim_forma_pagamento = source.id_dim_forma_pagamento,
                        id_dim_tempo_venda = source.id_dim_tempo_venda,
                        numero_nf = source.numero_nf,
                        valor_total_nf = source.valor_total_nf,
                        quantidade = source.quantidade,
                        valor_unitario = source.valor_unitario,
                        valor_venda_real = source.valor_venda_real
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (chave_negocio, id_dim_produto, id_dim_cliente, id_dim_vendedor, 
                            id_dim_forma_pagamento, id_dim_tempo_venda, numero_nf, 
                            valor_total_nf, quantidade, valor_unitario, valor_venda_real)
                    VALUES (source.chave_negocio, source.id_dim_produto, source.id_dim_cliente, 
                            source.id_dim_vendedor, source.id_dim_forma_pagamento, source.id_dim_tempo_venda,
                            source.numero_nf, source.valor_total_nf, source.quantidade, 
                            source.valor_unitario, source.valor_venda_real);
            """
            cursor.execute(merge_sql)
            conn.commit()
            return f"Sucesso: {len(rows)} registros processados via Staging."
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    # Fluxo
    task_extract = extract_postgres()
    task_transform = transform_and_lookup(task_extract)
    task_load = upsert_mssql_optimized(task_transform)

    task_extract >> task_transform >> task_load

etl_fato_venda_upsert()