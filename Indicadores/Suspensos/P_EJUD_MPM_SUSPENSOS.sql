CREATE OR REPLACE procedure P_EJUD_MPM_SUSPENSOS (
    p_ano IN NUMBER,
    p_mes IN NUMBER
)
IS
BEGIN

DELETE FROM DW_DEIGE.MPM_SUSPENSOS_HML WHERE ANO = p_ano AND mes = p_mes;

INSERT INTO DW_DEIGE.MPM_SUSPENSOS_HML
(
	CNJ,
	COD_DOC,
	codfase,
	CLASSE,
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
	COD_ORG_JULG,
	IND_COMPOSICAO
	)
SELECT 
	CNJ,
	COD_DOC,
	codfase,
	CLASSE,
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
	COD_ORG_JULG,
	IND_COMPOSICAO
FROM (
	WITH mov_suspensao AS (
	SELECT 
	    CODDOC AS COD_DOC,
	    MAX(DTHRMOV) AS DTHR_MOV,
	    codfase
	FROM EJUD_MPM_BASEDEDADOS
	WHERE TIPOMOV = 'SUSPENSAO'
	AND DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	GROUP BY coddoc,codfase
	), mov_saida_suspensao AS (
	SELECT 
	    CODDOC AS COD_DOC,
	    DTHRMOV AS DTHR_MOV
	FROM EJUD_MPM_BASEDEDADOS
	WHERE TIPOMOV = 'SAIDA_SUSPENSAO' 
	AND DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	), processo AS (
	    SELECT 
	        P.NUM_PROCESSO,
	        P.CODDOC AS COD_DOC,
	        P.ID_PROCESSO_CORP,
	        P.COD_COMPETENCIA,
	        P.COD_TIPO_PROCESSO AS COD_CLASSE,
	        TP.DESCR AS CLASSE,
	        p_ano AS ANO,
	        p_mes AS MES
	    FROM ejud_fac_processo P
	    JOIN EJUD_DIM_TIPO TP ON TP.CODTIPPROC = P.COD_TIPO_PROCESSO
	    WHERE TP.CODEXT NOT IN (5, 30, 40, 41, 42, 54, 73, 93, 94, 95, 297)
	    AND P.COD_COMPETENCIA NOT IN (9,10, 11, 14, 15, 20, 19, 22) 
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
	    WHERE DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	    AND CODMAGREL IS NOT null
		) WHERE RN = 1
	) 
	SELECT 
		processo.NUM_PROCESSO AS CNJ,
		processo.cod_doc AS cod_doc,
		processo.COD_CLASSE AS CLASSE,
		mov_suspensao.codfase,
		NULL AS ORGAO_JULGADOR,
		NULL AS RELATOR,
		NULL AS DATA_ULT_MOV,
		NULL AS COD_ULT_MOV,
		NULL AS DESCRICAO_ULT_MOV,
		NULL AS LOCAL_FISICO_ATUAL,
		NULL AS LOCAL_VIRTUAL_ATUAL,
		p_ano AS ANO,
		p_mes AS MES,
		relatores.COD_MAG_REL AS COD_MAG,
		relatores.COD_ORG_JULG AS COD_ORG_JULG,
		FN_RELATOR_VALIDO(relatores.COD_MAG_REL,relatores.COD_ORG_JULG,relatores.dthr_mov) AS IND_COMPOSICAO,
		ROW_NUMBER() OVER (PARTITION BY mov_suspensao.cod_doc ORDER BY mov_suspensao.DTHR_MOV - mov_saida_suspensao.DTHR_MOV DESC) AS rn
	FROM mov_suspensao 
	LEFT JOIN mov_saida_suspensao ON (mov_suspensao.cod_doc = mov_saida_suspensao.cod_doc AND mov_saida_suspensao.DTHR_MOV >= mov_suspensao.DTHR_MOV)
	LEFT JOIN relatores ON (mov_suspensao.cod_doc = relatores.cod_doc)
	JOIN processo ON (processo.cod_doc = mov_suspensao.cod_doc)
	WHERE mov_saida_suspensao.cod_doc IS null
	) WHERE RN = 1;

	DELETE FROM DW_DEIGE.MPM_SUSPENSOS_HML WHERE ANO = p_ano AND MES = p_mes AND COD_DOC NOT IN (SELECT COD_DOC FROM DW_DEIGE.MPM_PENDENTES_HML WHERE ano = p_ano AND mes = p_mes);

	INSERT INTO MPM_LOG_EXECUCAO (nome, data_execucao) VALUES ('P_EJUD_MPM_SUSPENSOS', SYSTIMESTAMP);

	EXCEPTION WHEN OTHERS THEN
	    DBMS_OUTPUT.PUT_LINE('Error in main loop: ' || SQLERRM);
	    DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
	    DBMS_OUTPUT.PUT_LINE('Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
	    DBMS_OUTPUT.PUT_LINE('Call Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK);
	    RAISE;	
    
END P_EJUD_MPM_SUSPENSOS;