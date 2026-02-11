WITH
TIPO_FAST AS (
	SELECT
		O.VALOR
		, O.OPCAO
	FROM
		[sankhya].TDDCAM C (NOLOCK)
	INNER JOIN
		[sankhya].TDDOPC O (NOLOCK)
		ON C.NUCAMPO = O.NUCAMPO
	WHERE
		C.NOMETAB = 'TGFTAB'
		AND C.NOMECAMPO= 'AD_TIPO_FAST'
		AND O.VALOR  = 'D'
)
, PRICETABS AS (
	SELECT
		T.CODTAB
		, T.DTVIGOR
		, T.CODREG
		, T.DTALTER
		, T.PERCENTUAL
		, T.FORMULA
		, T.CODTABORIG
		, T.NUTAB
		, T.JAPE_ID
		, T.AD_MOBILIDADE
		, T.UTILIZADECCUSTO
		, T.AD_TIPO_FAST AS COD_TIPO_FAST
		, F.OPCAO AS DESCR_TIPO_FAST
	FROM
		[sankhya].TGFTAB T (NOLOCK)
	INNER JOIN
		TIPO_FAST F
		ON T.AD_TIPO_FAST = F.VALOR
)
, PRICEREF AS (
    SELECT
        TOB.NUTAB
        , TOB.CODTAB
		, TOB.DESCR_TIPO_FAST
		, N'TABELA PADRÃO SITE (NUTAB ' + CAST(TOB.NUTAB AS NVARCHAR) + ' - ' + 'TIPO PREÇO ' + TOB.DESCR_TIPO_FAST + ')' AS DESCRTAB
    FROM
        PRICETABS TOB
    WHERE
        TOB.DTVIGOR = (
            SELECT
                MAX(T.DTVIGOR)
            FROM
                [sankhya].TGFTAB T (NOLOCK)
            WHERE
                T.CODTAB = TOB.CODTAB
    )
)
, TABCHECK AS (
       SELECT
           E.CODPROD
           , E.NUTAB
           , P.CODTAB
		   , P.DESCRTAB
		   , P.DESCR_TIPO_FAST
       FROM
           [sankhya].TGFEXC E (NOLOCK)
       INNER JOIN
           PRICEREF P
           ON E.NUTAB = P.NUTAB
)
, MARKA AS (
    SELECT
        M.CODIGO
        , M.DESCRICAO
        , M.AD_RELATORIODIARIO
        , M.AD_FAST
        , M.AD_FASTREF
    FROM
        [sankhya].TGFMAR M (NOLOCK)
    WHERE
        M.AD_FAST = 'S'
        AND M.AD_FASTREF IN('C', 'R')
)
, PRODUTOS AS (
    SELECT
        P.*
        , M.AD_FASTREF
    FROM
        [sankhya].TGFPRO P (NOLOCK)
    INNER JOIN
        MARKA M
        ON P.CODMARCA = M.CODIGO
    WHERE
        P.ATIVO = 'S'
)
, PARCIAL_A AS (
    SELECT
        LTRIM(RTRIM(CAST(P.CODPROD AS VARCHAR))) AS ProductId
        , P.DESCRPROD as ProductName
        , N.DESCRTAB AS PriceTableName
        , [sankhya].SNK_GET_PRECO(N.NUTAB, P.CODPROD, CURRENT_TIMESTAMP) AS VLRUNIT
		, N.DESCR_TIPO_FAST
        , P.AD_FASTREF
        , P.CODPROD
        , N.CODTAB
        , N.NUTAB
    FROM
        PRODUTOS P
    INNER JOIN
        TABCHECK N
        ON P.CODPROD = N.CODPROD
    WHERE
        P.ATIVO = 'S'
        AND P.AD_FASTREF = 'C'
)
, PARCIAL_T AS (
    SELECT
        LTRIM(RTRIM(P.REFFORN)) AS ProductId
        , P.DESCRPROD as ProductName
        , N.DESCRTAB AS PriceTableName
        , [sankhya].SNK_GET_PRECO(N.NUTAB, P.CODPROD, CURRENT_TIMESTAMP) AS VLRUNIT
		, N.DESCR_TIPO_FAST
        , P.AD_FASTREF
        , P.CODPROD
        , N.CODTAB
        , N.NUTAB
    FROM
        PRODUTOS P
    INNER JOIN
        TABCHECK N
        ON P.CODPROD = N.CODPROD
    WHERE
        P.ATIVO = 'S'
        AND P.AD_FASTREF = 'R'
)
, PREBATCH AS (
    SELECT
        PA.ProductId
         , PA.ProductName
         , PA.PriceTableName
         , REPLACE(REPLACE(CONVERT(VARCHAR, FORMAT(ROUND(PA.VLRUNIT, 2), 'N2'), 2), '.', ''), ',', '') AS ListPrice
         , REPLACE(REPLACE(CONVERT(VARCHAR, FORMAT(ROUND(PA.VLRUNIT, 2), 'N2'), 2), '.', ''), ',', '') AS SalePrice
		 , PA.DESCR_TIPO_FAST
         , PA.AD_FASTREF
         , PA.VLRUNIT
         , PA.CODPROD
         , PA.CODTAB
         , PA.NUTAB
    FROM
        PARCIAL_A PA
    UNION ALL
    SELECT
        PT.ProductId
         , PT.ProductName
         , PT.PriceTableName
         , REPLACE(REPLACE(CONVERT(VARCHAR, FORMAT(ROUND(PT.VLRUNIT, 2), 'N2'), 2), '.', ''), ',', '') AS ListPrice
         , REPLACE(REPLACE(CONVERT(VARCHAR, FORMAT(ROUND(PT.VLRUNIT, 2), 'N2'), 2), '.', ''), ',', '') AS SalePrice
		 , PT.DESCR_TIPO_FAST
         , PT.AD_FASTREF
         , PT.VLRUNIT
         , PT.CODPROD
         , PT.CODTAB
         , PT.NUTAB
    FROM
        PARCIAL_T PT
)
, PRERES AS (
    SELECT
        PRE.ProductId
        , PRE.ProductName
        , PRE.PriceTableName
        , PRE.ListPrice
        , PRE.SalePrice
        , PRE.DESCR_TIPO_FAST
        , PRE.VLRUNIT
        , PRE.CODTAB AS PRECODTAB
        , PRE.NUTAB
        , PRO.*
    FROM
        PRODUTOS PRO
    INNER JOIN
        PREBATCH PRE
        ON PRO.CODPROD = PRE.CODPROD
)
, PREDESC AS (
    SELECT
        D1.NUPROMOCAO
        , D1.CODPROD
        , D1.GRUPODESCPROD
        , D1.DTFINAL
        , X1.QTDE
        , X1.PERCDESC
        , X1.TIPDESC
        , (
            CASE WHEN (
                ISNULL(P2.GRUPODESCPROD, '#') = ISNULL(D1.GRUPODESCPROD, '@')
                AND ISNULL(P2.CODPROD, -1) = ISNULL(D1.CODPROD, -2)
                AND ISNULL(P2.PRECODTAB, -1) = ISNULL(D1.CODTAB, -2)
            ) THEN
                'QUATRO CAMPOS'
            WHEN (
                ISNULL(P2.CODPROD, -1) = ISNULL(D1.CODPROD, -2)
                AND ISNULL(P2.PRECODTAB, -1) = ISNULL(D1.CODTAB, -2)
                AND (D1.GRUPODESCPROD IS NULL OR D1.GRUPODESCPROD = '***************')
            ) THEN
                'CODPRO E CODTAB'
            WHEN (
                ISNULL(P2.GRUPODESCPROD, '#') = ISNULL(D1.GRUPODESCPROD, '@')
                AND ISNULL(P2.PRECODTAB, -1) = ISNULL(D1.CODTAB, -2)
                AND (D1.CODPROD IS NULL)
            ) THEN
                'GRUPO E CODTAB'
            WHEN (
                ISNULL(P2.GRUPODESCPROD, '#') = ISNULL(D1.GRUPODESCPROD, '@')
                AND ISNULL(P2.CODPROD, -1) = ISNULL(D1.CODPROD, -2)
                AND (D1.CODTAB IS NULL)
            ) THEN
                'GRUPO E CODPROD'
            WHEN (
                ISNULL(P2.PRECODTAB, -1) = ISNULL(D1.CODTAB, -2)
                AND (D1.CODPROD IS NULL
                AND (D1.GRUPODESCPROD IS NULL OR D1.GRUPODESCPROD = '***************'))
            ) THEN
                'CODTAB'
            WHEN (
                ISNULL(P2.CODPROD, -1) = ISNULL(D1.CODPROD, -2)
                AND (D1.CODTAB IS NULL
                AND (D1.GRUPODESCPROD IS NULL OR D1.GRUPODESCPROD = '***************'))
            ) THEN
                'CODPROD'
            WHEN (
                ISNULL(P2.GRUPODESCPROD, '#') = ISNULL(D1.GRUPODESCPROD, '@')
                AND (D1.CODTAB IS NULL
                AND D1.CODPROD IS NULL)
            ) THEN
                'GRUPO'
            ELSE
                'FALSE'
            END
        ) AS FLAG_BATCH
    FROM
        [sankhya].TGFDES D1 (NOLOCK)
    INNER JOIN
        [sankhya].TGFDPQ X1 (NOLOCK)
        ON D1.NUPROMOCAO = X1.NUPROMOCAO
    INNER JOIN
        PRERES P2
        ON P2.CODPROD = D1.CODPROD
)
, BATCH AS (
    SELECT
        P.ProductId
        , D.NUPROMOCAO
        , Q.QTDE AS MinimumBatchSize
        , (
            ISNULL(
                (
                    SELECT
                        MIN(D1.QTDE)
                    FROM
                        PREDESC D1
                    INNER JOIN
                        [sankhya].TGFPRO P1 (NOLOCK)
                        ON (
                            D1.CODPROD = P1.CODPROD
                            OR D1.GRUPODESCPROD = P1.GRUPODESCPROD
                        )
                    WHERE
                        P1.CODPROD = P.CODPROD
                        AND D1.QTDE > Q.QTDE
						AND D1.NUPROMOCAO = D.NUPROMOCAO
                ), 1001
            ) - 1
        ) AS MaximumBatchSize
        , (
            CASE WHEN Q.TIPDESC = 'P' THEN
                 (P.VLRUNIT - (P.VLRUNIT * (Q.PERCDESC / 100)))
             ELSE
                 Q.PERCDESC
            END
        ) AS UnitaryPriceForBatch
        , 'false' AS BatchDisabled
        , Q.NUVERSAO
        , F.BATCHID
        , F.STATUS
        , F.MSG
        , F.DTALTER
        , (
            CASE WHEN D.DTFINAL >= DATEADD(dd, DATEDIFF(dd, 0, getdate()), 0) THEN
                 'VALID'
            ELSE
                CASE WHEN F.STATUS = 'A' THEN
                     'REMOVE'
                 ELSE
                     'INVALID'
                END
            END
        ) AS FLAG
        , P.CODPROD
        , Q.TIPDESC
        , Q.PERCDESC
        , P.PRECODTAB AS CODTAB
        , P.NUTAB
        , D.CODEMP
        , D.DTINICIAL
        , D.DTFINAL
        , P.DESCR_TIPO_FAST
        , DP.FLAG_BATCH
        , D.CODTAB AS DESCODTAB
        , D.GRUPODESCPROD
    FROM
        [sankhya].TGFDES D (NOLOCK)
    INNER JOIN
        [sankhya].TGFDPQ Q (NOLOCK)
        ON D.NUPROMOCAO = Q.NUPROMOCAO
    INNER JOIN
        PREDESC DP
        ON D.NUPROMOCAO = DP.NUPROMOCAO
        AND D.CODPROD = DP.CODPROD
        AND Q.QTDE = DP.QTDE
    INNER JOIN
        PRERES P
        ON D.CODPROD = P.CODPROD
    LEFT JOIN
        [sankhya].AD_ESCFASTSYNC F (NOLOCK)
        ON D.NUPROMOCAO = F.NUPROMOCAO
        AND Q.QTDE = F.QTDE
        AND F.STATUS != 'I'
    WHERE
        ISNULL(D.NUVERSAO, 0) = (
            SELECT
                MAX(ISNULL(S.NUVERSAO, 0))
            FROM
                [sankhya].TGFDES S (NOLOCK)
            WHERE
                S.NUPROMOCAO = D.NUPROMOCAO
        )
        --AND DP.FLAG_BATCH != 'FALSE'
)
, BATCH_R AS (
    SELECT
        B.ProductId
         , B.MinimumBatchSize
         , B.MaximumBatchSize
         , REPLACE(REPLACE(CONVERT(VARCHAR, FORMAT(ROUND(B.UnitaryPriceForBatch, 2), 'N2'), 2), '.', ''), ',', '') AS ListPrice
         , B.BatchDisabled
         , B.BATCHID
         , B.STATUS
         , B.MSG
         , B.DTALTER
         , B.FLAG
         , B.CODPROD
         , B.TIPDESC
         , B.PERCDESC
         , B.NUPROMOCAO
         , B.CODTAB
         , B.NUTAB
         , B.CODEMP
         , B.DTINICIAL
         , B.DTFINAL
        , B.DESCR_TIPO_FAST
        , B.FLAG_BATCH
        , B.DESCODTAB
        , B.GRUPODESCPROD
    FROM
        BATCH B
    WHERE
        B.FLAG_BATCH != 'FALSE'
)

SELECT
    R.ProductId
    , R.MinimumBatchSize
    , R.MaximumBatchSize
    , R.ListPrice
    , R.BatchDisabled
    , R.BATCHID
    , R.STATUS
    , R.MSG
    , R.DTALTER
    , R.FLAG
    , R.CODPROD
    , R.TIPDESC
    , R.PERCDESC
    , R.NUPROMOCAO
    , R.CODTAB
    , R.NUTAB
    , R.CODEMP
    , R.DTINICIAL
    , R.DTFINAL
    , R.DESCR_TIPO_FAST
    , R.FLAG_BATCH
    , R.DESCODTAB
    , R.GRUPODESCPROD
FROM
    BATCH_R R

