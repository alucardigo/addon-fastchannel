-- ============================================================================
-- MIGRATION SCRIPT: Iconic -> Fastchannel
-- PURPOSE: Updates dictionary metadata ownership from 'iconicintegration' to 'fastchannelintegration'
-- TARGET: SQL Server (SankhyaW Database)
-- AUTHOR: Codex (Agent) / Gemini (Orchestrator)
-- ============================================================================

BEGIN TRANSACTION;

BEGIN TRY
    -- 1. PRE-CHECK
    DECLARE @OldDomain VARCHAR(50) = 'iconicintegration';
    DECLARE @NewDomain VARCHAR(50) = 'fastchannelintegration';
    DECLARE @Count INT = 0;

    SELECT @Count = COUNT(*) FROM TDDTAB WHERE DOMAIN = @OldDomain;
    PRINT 'Found ' + CAST(@Count AS VARCHAR) + ' tables in TDDTAB with old domain.';

    -- 2. MIGRATION
    -- Update Tables
    UPDATE TDDTAB SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update Fields
    UPDATE TDDCAM SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update Instances/Objects
    UPDATE TDDINS SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update Logic/Rules
    UPDATE TDDLGC SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update Links
    UPDATE TDDLIG SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update Options
    UPDATE TDDOPC SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update Parameters/Constants (Contexts)
    UPDATE TDDPCO SET DOMAIN = @NewDomain WHERE DOMAIN = @OldDomain;
    
    -- Update values inside TDDPCO (e.g. mge.iconicintegration -> mge.fastchannelintegration)
    UPDATE TDDPCO 
    SET VALOR = REPLACE(CAST(VALOR AS VARCHAR(MAX)), 'mge.iconicintegration', 'mge.fastchannelintegration')
    WHERE CAST(VALOR AS VARCHAR(MAX)) LIKE '%mge.iconicintegration%';

    -- 3. POST-CHECK
    DECLARE @Remaining INT;
    SELECT @Remaining = COUNT(*) FROM TDDTAB WHERE DOMAIN = @OldDomain;
    
    IF @Remaining > 0
    BEGIN
        RAISERROR('Migration failed: Old domain records still exist.', 16, 1);
    END

    -- 4. COMMIT
    PRINT 'Migration completed successfully. Committing transaction.';
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    PRINT 'Error occurred: ' + ERROR_MESSAGE();
    PRINT 'Rolling back transaction.';
    ROLLBACK TRANSACTION;
END CATCH;
