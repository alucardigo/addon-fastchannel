-- ============================================================
-- Migration: Adicionar defaults de cabecalho na AD_FCCONFIG
-- Data: 2026-02-02
-- Proposito:
-- 1. CODNAT (Natureza)
-- 2. CODCENCUS (Centro de Custo)
-- 3. CODVEND_PADRAO (Vendedor fallback)
-- ============================================================

-- Para SQL Server

-- 1. CODNAT
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'AD_FCCONFIG'
    AND COLUMN_NAME = 'CODNAT'
)
BEGIN
    ALTER TABLE AD_FCCONFIG
    ADD CODNAT INT NULL;

    PRINT 'Coluna CODNAT adicionada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Coluna CODNAT ja existe.';
END
GO

-- 2. CODCENCUS
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'AD_FCCONFIG'
    AND COLUMN_NAME = 'CODCENCUS'
)
BEGIN
    ALTER TABLE AD_FCCONFIG
    ADD CODCENCUS INT NULL;

    PRINT 'Coluna CODCENCUS adicionada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Coluna CODCENCUS ja existe.';
END
GO

-- 3. CODVEND_PADRAO
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'AD_FCCONFIG'
    AND COLUMN_NAME = 'CODVEND_PADRAO'
)
BEGIN
    ALTER TABLE AD_FCCONFIG
    ADD CODVEND_PADRAO INT NULL;

    PRINT 'Coluna CODVEND_PADRAO adicionada com sucesso.';
END
ELSE
BEGIN
    PRINT 'Coluna CODVEND_PADRAO ja existe.';
END
GO

PRINT '=== Migracao concluida com sucesso ==='
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
    AND column_name = 'CODNAT';

    IF v_column_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE AD_FCCONFIG ADD CODNAT NUMBER(10)';
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Coluna CODNAT adicionada com sucesso.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Coluna CODNAT ja existe.');
    END IF;
END;
/

DECLARE
    v_column_exists NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO v_column_exists
    FROM user_tab_columns
    WHERE table_name = 'AD_FCCONFIG'
    AND column_name = 'CODCENCUS';

    IF v_column_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE AD_FCCONFIG ADD CODCENCUS NUMBER(10)';
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Coluna CODCENCUS adicionada com sucesso.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Coluna CODCENCUS ja existe.');
    END IF;
END;
/

DECLARE
    v_column_exists NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO v_column_exists
    FROM user_tab_columns
    WHERE table_name = 'AD_FCCONFIG'
    AND column_name = 'CODVEND_PADRAO';

    IF v_column_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE AD_FCCONFIG ADD CODVEND_PADRAO NUMBER(10)';
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Coluna CODVEND_PADRAO adicionada com sucesso.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Coluna CODVEND_PADRAO ja existe.');
    END IF;
END;
/
*/
