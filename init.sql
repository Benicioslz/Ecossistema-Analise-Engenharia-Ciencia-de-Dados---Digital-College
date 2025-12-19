/*
Cria o banco de dados e as tabelas necessárias na inicialização
*/
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END
GO

USE DataWarehouse;
GO

-- Criar a tabela DimCliente se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimCliente')
BEGIN
    CREATE TABLE DimCliente (
        id_dim_cliente INT NOT NULL PRIMARY KEY,
        cliente VARCHAR(255),
        tipo_cliente VARCHAR(10)
    );
END
GO

-- Criar a tabela DimVendedor se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimVendedor')
BEGIN
    CREATE TABLE DimVendedor (
        id_dim_vendedor INT NOT NULL PRIMARY KEY,
        nome_vendedor VARCHAR(255),
        cpf VARCHAR(14)
    );
END
GO

-- Criar a tabela DimFornecedor se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimFornecedor')
BEGIN
    CREATE TABLE DimFornecedor (
        id_dim_fornecedor INT NOT NULL PRIMARY KEY,
        fornecedor VARCHAR(255),
        cnpj_fornecedor VARCHAR(20)
    );
END
GO

-- Criar a tabela DimFormaPagamento se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimFormaPagamento')
BEGIN
    CREATE TABLE DimFormaPagamento (
        id_dim_forma_pagamento INT NOT NULL PRIMARY KEY,
        descricao_forma_pagamento VARCHAR(100)
    );
END
GO

-- Criar a tabela DimSituacaoTitulo se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimSituacaoTitulo')
BEGIN
    CREATE TABLE DimSituacaoTitulo (
        id_dim_situacao INT NOT NULL PRIMARY KEY,
        situacao_titulo VARCHAR(100)
    );
END
GO

-- Criar a tabela DimProduto se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimProduto')
BEGIN
    CREATE TABLE DimProduto (
        id_dim_produto INT NOT NULL PRIMARY KEY,
        id_fornecedor INT,
        categoria VARCHAR(100),
        descricao_produto VARCHAR(255)

        CONSTRAINT FK_FornecedorProduto FOREIGN KEY (id_fornecedor) REFERENCES DimFornecedor(id_dim_fornecedor)
    );
END
GO

-- Criar a tabela DimTempo se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimTempo')
BEGIN
    CREATE TABLE DimTempo (
        id_dim_tempo INT NOT NULL PRIMARY KEY,
        data DATE NOT NULL,
        ano INT,
        mes INT,
        dia INT,
        trimestre INT
    );
END
GO

-- Inserção de valores na DimTempo
-- Limpa a tabela se necessário (opcional)
-- TRUNCATE TABLE DimTempo;

WITH DatasCTE AS (
    -- Data de Início
    SELECT CAST('2010-01-01' AS DATE) AS Data
    UNION ALL
    -- Incremento de 1 dia até a data final
    SELECT DATEADD(DAY, 1, Data)
    FROM DatasCTE
    WHERE Data < '2030-12-31'
)
INSERT INTO DimTempo (
    id_dim_tempo, 
    data, 
    ano, 
    mes, 
    dia, 
    trimestre
)
SELECT 
    -- Gera o ID no formato 20251218
    CAST(FORMAT(Data, 'yyyyMMdd') AS INT) AS id_dim_tempo,
    Data,
    YEAR(Data) AS ano,
    MONTH(Data) AS mes,
    DAY(Data) AS dia,
    DATEPART(QUARTER, Data) AS trimestre
FROM DatasCTE
OPTION (MAXRECURSION 0); -- Necessário para permitir mais de 100 iterações
GO

-- Criar a tabela FatoContasAPagar se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FatoContasAPagar')
BEGIN
    CREATE TABLE FatoContasAPagar (
        id_dim_conta_pagar INT NOT NULL PRIMARY KEY,
        id_dim_situacao INT NOT NULL,
        id_dim_forma_pagamento INT NOT NULL,
        id_dim_tempo_emissao INT NOT NULL,
        id_dim_tempo_vencimento INT NOT NULL,
        id_dim_tempo_pagamento INT NULL,
        valor_original DECIMAL(18,2),
        valor_atual DECIMAL(18,2),
        documento VARCHAR(100),
        descricao_pagamento VARCHAR(MAX),

        -- Relacionamentos (Role-Playing)
        CONSTRAINT FK_SituacaoCP FOREIGN KEY (id_dim_situacao) REFERENCES DimSituacaoTitulo(id_dim_situacao),
        CONSTRAINT FK_FormaPagtoCP FOREIGN KEY (id_dim_forma_pagamento) REFERENCES DimFormaPagamento(id_dim_forma_pagamento),
        CONSTRAINT FK_Emissao FOREIGN KEY (id_dim_tempo_emissao) REFERENCES DimTempo(id_dim_tempo),
        CONSTRAINT FK_Vencimento FOREIGN KEY (id_dim_tempo_vencimento) REFERENCES DimTempo(id_dim_tempo),
        CONSTRAINT FK_Pagamento FOREIGN KEY (id_dim_tempo_pagamento) REFERENCES DimTempo(id_dim_tempo)
    );
END
GO

-- Criar a tabela FatoContasAReceber se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FatoContasAReceber')
BEGIN
    CREATE TABLE FatoContasAReceber (
        id_dim_conta_receber INT NOT NULL PRIMARY KEY, -- Chave do Postgres
        id_dim_situacao INT NOT NULL,
        id_dim_forma_pagamento INT NOT NULL,
        id_dim_cliente INT NOT NULL,
        id_dim_tempo_vencimento INT NOT NULL,
        id_dim_tempo_recebimento INT NULL,         -- NULL se não recebido
        valor_parcela DECIMAL(18,2),
        valor_original DECIMAL(18,2),
        valor_atual DECIMAL(18,2),
        numero_parcela INT,
        numero_nf VARCHAR(50),

        -- Relacionamentos (Constraints)
        CONSTRAINT FK_SituacaoCR FOREIGN KEY (id_dim_situacao) REFERENCES DimSituacaoTitulo(id_dim_situacao),
        CONSTRAINT FK_FormaPagtoCR FOREIGN KEY (id_dim_forma_pagamento) REFERENCES DimFormaPagamento(id_dim_forma_pagamento),
        CONSTRAINT FK_Receber_Cliente FOREIGN KEY (id_dim_cliente) REFERENCES DimCliente(id_dim_cliente),
        CONSTRAINT FK_Receber_Tempo_Venc FOREIGN KEY (id_dim_tempo_vencimento) REFERENCES DimTempo(id_dim_tempo),
        CONSTRAINT FK_Receber_Tempo_Rec FOREIGN KEY (id_dim_tempo_recebimento) REFERENCES DimTempo(id_dim_tempo)
    );
END
GO

-- Criar a tabela FatoVenda se não existir
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FatoVenda')
BEGIN
    CREATE TABLE FatoVenda (
        -- Chave composta (id_nota_fiscal + id_item_nf) vinda da DAG
        chave_negocio VARCHAR(100) NOT NULL PRIMARY KEY, 
        
        -- Chaves Estrangeiras para Dimensões
        id_dim_produto INT NOT NULL,
        id_dim_cliente INT NOT NULL,
        id_dim_vendedor INT NOT NULL,
        id_dim_forma_pagamento INT NOT NULL,
        id_dim_tempo_venda INT NOT NULL,
        
        -- Métricas e Atributos da Venda
        numero_nf VARCHAR(50),
        valor_total_nf DECIMAL(18,2),
        quantidade INT,
        valor_unitario DECIMAL(18,2),
        valor_venda_real DECIMAL(18,2),

        -- Relacionamentos
        CONSTRAINT FK_Venda_Produto FOREIGN KEY (id_dim_produto) REFERENCES DimProduto(id_dim_produto),
        CONSTRAINT FK_Venda_Cliente FOREIGN KEY (id_dim_cliente) REFERENCES DimCliente(id_dim_cliente),
        CONSTRAINT FK_Venda_Vendedor FOREIGN KEY (id_dim_vendedor) REFERENCES DimVendedor(id_dim_vendedor),
        CONSTRAINT FK_Venda_FormaPagto FOREIGN KEY (id_dim_forma_pagamento) REFERENCES DimFormaPagamento(id_dim_forma_pagamento),
        CONSTRAINT FK_Venda_Tempo FOREIGN KEY (id_dim_tempo_venda) REFERENCES DimTempo(id_dim_tempo)
    );

    -- Índice para otimizar os comandos de MERGE baseados na chave de negócio
    CREATE INDEX idx_fatovenda_chave ON FatoVenda(chave_negocio);
END
GO