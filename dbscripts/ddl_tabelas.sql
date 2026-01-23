-- ============================================================
-- Script DDL para criação da tabela AD_INTEGLOG
-- Tabela de Log de Integração FASTCHANNEL
-- ============================================================
-- Este script é usado apenas como referência/backup.
-- Com autoDDL=true no build.gradle, a tabela é criada 
-- automaticamente a partir do datadictionary/AD_INTEGLOG.xml
-- ============================================================

-- Criação da Tabela
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'AD_INTEGLOG' AND xtype = 'U')
BEGIN
    CREATE TABLE AD_INTEGLOG (
        NUSEQ           INT             NOT NULL IDENTITY(1,1),
        DTHEVENTO       DATETIME        NOT NULL DEFAULT GETDATE(),
        STATUS          VARCHAR(20)     NOT NULL,
        ENDPOINT        VARCHAR(200)    NULL,
        MESSAGE         VARCHAR(MAX)    NULL,
        PAYLOADHASH     VARCHAR(100)    NULL,
        DATAHASH        VARCHAR(100)    NULL,
        
        CONSTRAINT PK_AD_INTEGLOG PRIMARY KEY (NUSEQ)
    );
    
    -- Índices para performance
    CREATE INDEX IDX_INTEGLOG_DTHEVENTO ON AD_INTEGLOG (DTHEVENTO DESC);
    CREATE INDEX IDX_INTEGLOG_STATUS ON AD_INTEGLOG (STATUS);
    
    PRINT 'Tabela AD_INTEGLOG criada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Tabela AD_INTEGLOG já existe.';
END
GO

-- ============================================================
-- Parâmetros do Sistema para configuração da integração
-- Execute no banco Sankhya para criar os parâmetros necessários
-- ============================================================

-- Parâmetros de Autenticação Azure AD
-- FASTCHANNEL.TENANT.ID = 72b5f416-8f41-4c88-a6a0-bb4b91383888
-- FASTCHANNEL.CLIENT.ID = 1f02761e-0c64-4538-8fb0-4a1b952f5216
-- FASTCHANNEL.CLIENT.SECRET = [Deve ser configurado manualmente pelo administrador]
-- FASTCHANNEL.SCOPE = api://a06e7974-0392-4f40-b725-9abf444d6d74/.default
-- FASTCHANNEL.API.URL = http://48.214.31.160
-- FASTCHANNEL.DISTRIBUIDOR.ID = BELLUBE

-- Exemplo de inserção de parâmetros (ajustar conforme estrutura real da TSIPAR):
/*
INSERT INTO TSIPAR (CHAVE, TEXTO, DESCRICAO, CODUSUALT, DHALTER)
SELECT 'FASTCHANNEL.TENANT.ID', '72b5f416-8f41-4c88-a6a0-bb4b91383888', 'Azure AD Tenant ID para FASTCHANNEL', 0, GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM TSIPAR WHERE CHAVE = 'FASTCHANNEL.TENANT.ID');

INSERT INTO TSIPAR (CHAVE, TEXTO, DESCRICAO, CODUSUALT, DHALTER)
SELECT 'FASTCHANNEL.CLIENT.ID', '1f02761e-0c64-4538-8fb0-4a1b952f5216', 'Azure AD Client ID para FASTCHANNEL', 0, GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM TSIPAR WHERE CHAVE = 'FASTCHANNEL.CLIENT.ID');

INSERT INTO TSIPAR (CHAVE, TEXTO, DESCRICAO, CODUSUALT, DHALTER)
SELECT 'FASTCHANNEL.SCOPE', 'api://a06e7974-0392-4f40-b725-9abf444d6d74/.default', 'Azure AD Scope para FASTCHANNEL', 0, GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM TSIPAR WHERE CHAVE = 'FASTCHANNEL.SCOPE');

INSERT INTO TSIPAR (CHAVE, TEXTO, DESCRICAO, CODUSUALT, DHALTER)
SELECT 'FASTCHANNEL.API.URL', 'http://48.214.31.160', 'URL da API FASTCHANNEL (DEV)', 0, GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM TSIPAR WHERE CHAVE = 'FASTCHANNEL.API.URL');

INSERT INTO TSIPAR (CHAVE, TEXTO, DESCRICAO, CODUSUALT, DHALTER)
SELECT 'FASTCHANNEL.DISTRIBUIDOR.ID', 'BELLUBE', 'ID do Distribuidor para API FASTCHANNEL', 0, GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM TSIPAR WHERE CHAVE = 'FASTCHANNEL.DISTRIBUIDOR.ID');
*/

-- IMPORTANTE: O CLIENT_SECRET deve ser inserido manualmente por questões de segurança!
-- INSERT INTO TSIPAR (CHAVE, TEXTO, DESCRICAO, CODUSUALT, DHALTER)
-- VALUES ('FASTCHANNEL.CLIENT.SECRET', 'SEU_SECRET_AQUI', 'Azure AD Client Secret para FASTCHANNEL', 0, GETDATE());

