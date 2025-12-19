from __future__ import annotations
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
from hdfs import InsecureClient
import os

# Constantes
MSSQL_CONN_ID = "mssql_target"
HDFS_URL = "http://namenode:9870"
HDFS_PATH = "/data/analytical_slice/"

@dag(
    dag_id="dag_gerar_slice_analitico_para_ml",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,  
    catchup=False,
    tags=['ml', 'parquet', 'analytics'],
)
def gerar_slice_analitico_dag():

    @task
    def extrair_dados_do_dw_e_salvar_em_parquet():
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        
        # SQL CORRIGIDO: Removido 'DataWarehouse.dbo' de antes da CTE 'Vendas'
        sql_query = """
            WITH Vendas AS (
                SELECT 
                    numero_nf,
                    id_dim_cliente,
                    id_dim_vendedor,
                    id_dim_tempo_venda,
                    SUM(valor_venda_real) as valor_total_venda,
                    SUM(quantidade) as quantidade_total_itens
                FROM DataWarehouse.dbo.FatoVenda
                GROUP BY numero_nf, id_dim_cliente, id_dim_vendedor, id_dim_tempo_venda  
            )
            SELECT
                v.numero_nf,
                v.id_dim_cliente,
                v.id_dim_vendedor,
                cr.id_dim_situacao,
                cr.id_dim_forma_pagamento,
                t_venda.data as data_venda,
                t_venc.data as data_vencimento,
                t_receb.data as data_recebimento,
                v.valor_total_venda,
                v.quantidade_total_itens,
                cr.valor_parcela,
                cr.numero_parcela,
                cli.cliente,
                cli.tipo_cliente,
                sit.situacao_titulo,
                fp.descricao_forma_pagamento
            FROM Vendas v  -- CORREÇÃO AQUI: Referência direta à CTE
            LEFT JOIN DataWarehouse.dbo.FatoContasAReceber cr ON v.numero_nf = cr.numero_nf
            LEFT JOIN DataWarehouse.dbo.DimTempo t_venda ON v.id_dim_tempo_venda = t_venda.id_dim_tempo
            LEFT JOIN DataWarehouse.dbo.DimTempo t_venc ON cr.id_dim_tempo_vencimento = t_venc.id_dim_tempo
            LEFT JOIN DataWarehouse.dbo.DimTempo t_receb ON cr.id_dim_tempo_recebimento = t_receb.id_dim_tempo
            LEFT JOIN DataWarehouse.dbo.DimCliente cli ON v.id_dim_cliente = cli.id_dim_cliente
            LEFT JOIN DataWarehouse.dbo.DimSituacaoTitulo sit ON cr.id_dim_situacao = sit.id_dim_situacao
            LEFT JOIN DataWarehouse.dbo.DimFormaPagamento fp ON cr.id_dim_forma_pagamento = fp.id_dim_forma_pagamento
            WHERE v.id_dim_cliente IS NOT NULL;
        """
        
        print("➡️ Extraindo dados do Data Warehouse...")
        df = hook.get_pandas_df(sql_query)
        
        if df.empty:
            raise ValueError("A extração retornou vazio.")

        # Tratamento de Datas para o ML
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        df['ano'] = df['data_venda'].dt.year
        df['mes'] = df['data_venda'].dt.month
        
        temp_path = "/tmp/analytical_slice.parquet"
        df.to_parquet(temp_path, engine='pyarrow', compression='snappy', index=False)
        
        print(f"➡️ Enviando para HDFS: {HDFS_URL}")
        try:
            # timeout=60 ajuda em arquivos maiores
            hdfs_client = InsecureClient(HDFS_URL, user='root', timeout=60)
            hdfs_client.makedirs(HDFS_PATH)
            
            hdfs_file_path = f"{HDFS_PATH}analytical_slice.parquet"
            # n_threads=1 resolve o problema de redirecionamento 307 no Docker
            hdfs_client.upload(hdfs_file_path, temp_path, overwrite=True, n_threads=1)
            
            print(f"✅ Sucesso! Arquivo no HDFS: {hdfs_file_path}")
            
            # Limpeza
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
        except Exception as e:
            print(f"❌ Erro no upload para o Hadoop: {e}")
            raise e

    extrair_dados_do_dw_e_salvar_em_parquet()

gerar_slice_analitico_dag()