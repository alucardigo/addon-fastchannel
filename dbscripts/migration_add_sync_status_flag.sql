-- ============================================================
-- Migration: Adicionar flag SYNC_STATUS_ENABLED
-- Data: 2026-01-30
-- Proposito: Permitir desabilitar sincronizacao de status com Fastchannel
-- ============================================================

-- Para SQL Server
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'AD_FCCONFIG'
    AND COLUMN_NAME = 'SYNC_STATUS_ENABLED'
)
BEGIN
    ALTER TABLE AD_FCCONFIG
    ADD SYNC_STATUS_ENABLED VARCHAR(1) NULL;

    -- Valor default 'S' (habilitado) para registros existentes
    UPDATE AD_FCCONFIG
    SET SYNC_STATUS_ENABLED = 'S'
    WHERE SYNC_STATUS_ENABLED IS NULL;

    PRINT 'Coluna SYNC_STATUS_ENABLED adicionada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Coluna SYNC_STATUS_ENABLED ja existe.';
END
GO

-- Para Oracle
-- Descomente se necessario:
/*
DECLARE
    v_column_exists NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO v_column_exists
    FROM user_tab_columns
    WHERE table_name = 'AD_FCCONFIG'
    AND column_name = 'SYNC_STATUS_ENABLED';

    IF v_column_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE AD_FCCONFIG ADD SYNC_STATUS_ENABLED VARCHAR2(1)';
        EXECUTE IMMEDIATE 'UPDATE AD_FCCONFIG SET SYNC_STATUS_ENABLED = ''S'' WHERE SYNC_STATUS_ENABLED IS NULL';
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Coluna SYNC_STATUS_ENABLED adicionada com sucesso.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Coluna SYNC_STATUS_ENABLED ja existe.');
    END IF;
END;
/
*/
