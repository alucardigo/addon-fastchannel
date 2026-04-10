-- ============================================================================
-- [REPLICAR TRIGGERS TGFITE] Prod -> Homolog
--
-- Objetivo: extrair as definicoes das 33 triggers de TGFITE em PRODUCAO e
-- aplica-las em HOMOLOG, resolvendo o "erro fatal 217" (max nesting level)
-- que ocorre APENAS em homolog durante o import de pedidos via Fastchannel
-- addon.
--
-- Hipotese: as triggers em homolog foram desabilitadas em 2026-04-06 e, ao
-- reativar, ficaram em estado inconsistente ou com versoes desatualizadas
-- em relacao a prod. Replicar literalmente prod garante comportamento igual.
--
-- ============================================================================
-- PASSO 1 - RODAR EM PRODUCAO (SANKHYA_PROD)
-- ============================================================================
-- Gera um script DROP + CREATE de todas as 33 triggers TGFITE.
-- Salvar output em arquivo .sql para aplicar em homolog.
--
-- Banco: SANKHYA_PROD
-- Usuario: qualquer com db_owner ou VIEW DEFINITION
-- Output: texto com todos os CREATE TRIGGER, um por linha, separados por GO
-- ============================================================================

USE SANKHYA_PROD;
SET NOCOUNT ON;

DECLARE @trigger_name SYSNAME;
DECLARE @definition NVARCHAR(MAX);
DECLARE @schema SYSNAME;
DECLARE @uses_ansi_nulls BIT;
DECLARE @uses_quoted_id BIT;

PRINT '-- ============================================================================';
PRINT '-- DUMP TRIGGERS TGFITE - SANKHYA_PROD';
PRINT '-- Gerado em: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '-- Total de triggers: (contar output abaixo)';
PRINT '-- ============================================================================';
PRINT '';
PRINT 'USE SANKHYA_TESTE;';
PRINT 'GO';
PRINT '';

DECLARE trg_cursor CURSOR FOR
    SELECT
        OBJECT_SCHEMA_NAME(t.object_id) AS schema_name,
        t.name,
        m.definition,
        m.uses_ansi_nulls,
        m.uses_quoted_identifier
    FROM sys.triggers t
    JOIN sys.sql_modules m ON m.object_id = t.object_id
    WHERE t.parent_id = OBJECT_ID('TGFITE')
    ORDER BY t.name;

OPEN trg_cursor;
FETCH NEXT FROM trg_cursor INTO @schema, @trigger_name, @definition, @uses_ansi_nulls, @uses_quoted_id;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '-- ---------------------------------------------------------------';
    PRINT '-- TRIGGER: ' + @schema + '.' + @trigger_name;
    PRINT '-- ANSI_NULLS=' + CAST(@uses_ansi_nulls AS VARCHAR(1))
        + ' QUOTED_IDENTIFIER=' + CAST(@uses_quoted_id AS VARCHAR(1));
    PRINT '-- ---------------------------------------------------------------';
    PRINT 'SET ANSI_NULLS ' + CASE WHEN @uses_ansi_nulls = 1 THEN 'ON' ELSE 'OFF' END + ';';
    PRINT 'SET QUOTED_IDENTIFIER ' + CASE WHEN @uses_quoted_id = 1 THEN 'ON' ELSE 'OFF' END + ';';
    PRINT 'GO';
    PRINT 'IF OBJECT_ID(''' + @schema + '.' + @trigger_name + ''', ''TR'') IS NOT NULL';
    PRINT '    DROP TRIGGER ' + @schema + '.' + @trigger_name + ';';
    PRINT 'GO';
    -- Print definition em chunks de 4000 chars (limite do PRINT)
    DECLARE @chunk_start INT = 1;
    DECLARE @chunk_size INT = 4000;
    DECLARE @total_len INT = LEN(@definition);
    WHILE @chunk_start <= @total_len
    BEGIN
        PRINT SUBSTRING(@definition, @chunk_start, @chunk_size);
        SET @chunk_start = @chunk_start + @chunk_size;
    END
    PRINT 'GO';
    PRINT '';

    FETCH NEXT FROM trg_cursor INTO @schema, @trigger_name, @definition, @uses_ansi_nulls, @uses_quoted_id;
END

CLOSE trg_cursor;
DEALLOCATE trg_cursor;

PRINT '-- ============================================================================';
PRINT '-- FIM DUMP - Total de triggers processadas:';
SELECT COUNT(*) AS TotalTriggersDump FROM sys.triggers WHERE parent_id = OBJECT_ID('TGFITE');
PRINT '-- ============================================================================';

-- ============================================================================
-- PASSO 2 - RODAR EM HOMOLOG (SANKHYA_TESTE)
-- ============================================================================
-- Salvar o output do PASSO 1 como arquivo (ex.: triggers_prod_dump.sql) e
-- executa-lo em SANKHYA_TESTE. Isso vai:
--   1) Setar SET ANSI_NULLS/QUOTED_IDENTIFIER iguais a prod para cada trigger
--   2) DROP da trigger existente em homolog
--   3) CREATE TRIGGER com a definicao EXATA de prod
--
-- CUIDADO: se houver triggers CUSTOMIZADAS em homolog que nao existem em
-- prod, elas ficam intactas (o script so dropa as que estao em prod).
--
-- VALIDACAO POS-EXECUCAO:
-- ----------------------------------------------------------------------------
USE SANKHYA_TESTE;
SET NOCOUNT ON;

SELECT
    COUNT(*) AS total_triggers_tgfite,
    SUM(CAST(is_disabled AS INT)) AS disabled
FROM sys.triggers
WHERE parent_id = OBJECT_ID('TGFITE');

-- Comparar versao via checksum das definicoes
SELECT
    name,
    CHECKSUM(definition) AS def_checksum,
    create_date,
    modify_date
FROM sys.triggers t
JOIN sys.sql_modules m ON m.object_id = t.object_id
WHERE parent_id = OBJECT_ID('TGFITE')
ORDER BY name;
-- ============================================================================
