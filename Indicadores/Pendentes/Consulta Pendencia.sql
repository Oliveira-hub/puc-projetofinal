SELECT  
	NUM_PROCESSO,
	COD_CLASSE,
	ORGAO_JULGADOR,
	RELATOR,
	DATA_ULT_MOV,
	COD_ULT_MOV,
	DESCRICAO_ULT_MOV,
	LOCAL_FISICO_ATUAL,
	LOCAL_VIRTUAL_ATUAL,
	ANO,
	MES,
	COD_MAG,
	COD_ORG_JULG
FROM (
	WITH mov_distribuicao AS (
	SELECT 
	    CODDOC AS COD_DOC,
	    MIN(DTHRMOV) AS DTHR_MOV
	FROM EJUD_MPM_BASEDEDADOS
	WHERE TIPOMOV = 'DISTRIBUICAO'
	AND DTHRMOV < ADD_MONTHS(TO_DATE(2024 || '-' || LPAD(9, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	AND CODDOC = 26813674
	GROUP BY coddoc
	), mov_baixa AS (
	SELECT 
	    CODDOC AS COD_DOC,
	    DTHRMOV AS DTHR_MOV
	FROM EJUD_MPM_BASEDEDADOS
	WHERE TIPOMOV = 'BAIXA' 
	AND CODDOC = 26813674
	AND DTHRMOV < ADD_MONTHS(TO_DATE(2024 || '-' || LPAD(9, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	), processo AS (
	    SELECT 
	        P.NUM_PROCESSO,
	        P.CODDOC AS COD_DOC,
	        P.ID_PROCESSO_CORP,
	        P.COD_COMPETENCIA,
	        P.COD_TIPO_PROCESSO AS COD_CLASSE,
	        TP.DESCR AS CLASSE
	    FROM ejud_fac_processo P
	    JOIN EJUD_DIM_TIPO TP ON TP.CODTIPPROC = P.COD_TIPO_PROCESSO
	    WHERE TP.CODEXT NOT IN (5, 30, 40, 41, 42, 54, 73, 93, 94, 95, 297)
	    AND P.COD_COMPETENCIA NOT IN (9,10, 11, 14, 15, 20, 19, 22) 
	    AND CODDOC = 26813674
	), relatores AS (
	SELECT 
	    COD_DOC,
	    DTHR_MOV,
	    COD_MAG_REL,
	    COD_ORG_JULG
	FROM (
	    SELECT 
	        CODDOC AS COD_DOC,
	        DTHRMOV AS DTHR_MOV,
	        CODMAGREL AS COD_MAG_REL,
	        CODORGJULG AS COD_ORG_JULG,
	        ROW_NUMBER() OVER (PARTITION BY CODDOC ORDER BY DTHRMOV DESC) AS RN
	    FROM ejud.movimentorelator@tj01
	    WHERE DTHRMOV < ADD_MONTHS(TO_DATE(2024 || '-' || LPAD(9, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	    AND CODMAGREL IS NOT NULL
	    AND CODDOC = 26813674
		) WHERE RN = 1
    )
	SELECT 
		processo.NUM_PROCESSO,
		processo.COD_CLASSE,
		NULL AS ORGAO_JULGADOR,
		NULL AS RELATOR,
		NULL AS DATA_ULT_MOV,
		NULL AS COD_ULT_MOV,
		NULL AS DESCRICAO_ULT_MOV,
		NULL AS LOCAL_FISICO_ATUAL,
		NULL AS LOCAL_VIRTUAL_ATUAL,
		2024 AS ANO,
		9 AS MES,
		relatores.COD_MAG_REL AS COD_MAG,
		relatores.COD_ORG_JULG AS COD_ORG_JULG,
		ROW_NUMBER() OVER (PARTITION BY mov_distribuicao.cod_doc ORDER BY mov_distribuicao.DTHR_MOV - mov_baixa.DTHR_MOV DESC) AS rn
	FROM mov_distribuicao 
	LEFT JOIN mov_baixa ON (mov_distribuicao.cod_doc = mov_baixa.cod_doc AND mov_baixa.DTHR_MOV >= mov_distribuicao.DTHR_MOV)
	LEFT JOIN relatores ON (mov_distribuicao.cod_doc = relatores.cod_doc)
	JOIN processo ON (processo.cod_doc = mov_distribuicao.cod_doc)
	WHERE mov_baixa.cod_doc IS null
	) WHERE RN = 1;