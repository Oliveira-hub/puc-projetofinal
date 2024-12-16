CREATE OR REPLACE procedure DW_DEIGE.P_EJUD_MPM_CASOSNOVOS (
    p_ano IN NUMBER,
    p_mes IN NUMBER
)
IS
BEGIN

DELETE FROM DW_DEIGE.MPM_CASOSNOVOS_HML where ano = p_ano AND mes = p_mes;

INSERT INTO DW_DEIGE.MPM_CASOSNOVOS_HML
(
	CNJ,
	NUMERO_ANTIGO,
	PRIMEIRA_FASE,
	RELATOR,
	EX_RELATOR,
	ORGAO_JULGADOR,
	DATA_fASE,
	FASE,
	CLASSE_CNJ,
	NATUREZA,
	TIPO,
	ELETRONICO,
	ANO, 
	MES
)
SELECT * FROM (
WITH mov_casosnovos AS (
    SELECT 
        CODDOC AS COD_DOC,
        DTHRMOV AS DTHR_MOV,
        CODFASE AS COD_FASE,
        CODCOMPL1 AS COD_COMPL_1,
        CODCOMPL2 AS COD_COMPL_2,
        CODCOMPL3 AS COD_COMPL_3,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY DTHRMOV) = 1 AND codfase IN (26, 36) THEN 'S'
            ELSE 'N'
        END AS primeira_fase
    FROM EJUD_MPM_BASEDEDADOS 
    WHERE TIPOMOV = 'DISTRIBUICAO'
), processos AS (
    SELECT 
		P.NUM_PROCESSO,
	    P.CODDOC AS COD_DOC,
	    P.ID_PROCESSO_CORP,
	    P.COD_COMPETENCIA,
	    P.COD_TIPO_PROCESSO AS COD_CLASSE,
	    N.DESCR AS NATUREZA,
	    P.IND_ELETRONICO AS ELETRONICO,
	    CASE 
	        WHEN SUBSTR(P.NUM_PROCESSO, -4) = '0000' THEN 'Originário'
	        ELSE 'Recursal'
	    END AS TIPO
    FROM ejud_fac_processo P
    JOIN EJUD.TIPOPROCESSO@TJ01 TP ON TP.CODTIPPROC = P.COD_TIPO_PROCESSO
    LEFT JOIN EJUD.NATUREZA@TJ01 N ON TP.CODNAT = N.CODNAT
    WHERE TP.CODEXT NOT IN (5, 30, 40, 41, 42, 54, 73, 93, 94, 95, 297)
    AND P.COD_COMPETENCIA NOT IN (9,10, 11, 14, 15, 20, 19, 22) 
), local_atual AS (
    SELECT 
        cod_doc, 
        dthr_mov, 
        cod_local
    FROM (
        SELECT 
            mf.coddoc AS cod_doc, 
            mf.dthrmov AS dthr_mov,
            mf.codlocal AS cod_local, 
            lf.descr AS ult_local,
            ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) AS rn
        FROM ejud.movimentofisico@tj01 mf
        LEFT JOIN ejud.localfisico@tj01 lf ON mf.codlocal = lf.codlocal
        WHERE mf.dthrmov < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
    )
    WHERE rn = 1 AND cod_local NOT IN (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)
), mov_relator AS (
    SELECT 
        mr.coddoc AS cod_doc,
        mr.dthrmov AS dthr_mov,
        mr.codorgjulg AS cod_org_julg,
        mr.codmagrel AS cod_mag_rel
    FROM ejud.movimentorelator@tj01 mr
    WHERE mr.dthrmov < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
)
SELECT 
    processos.NUM_PROCESSO AS CNJ,
    NULL AS NUMERO_ANTIGO,
    mov_casosnovos.PRIMEIRA_FASE,
    mov_relator.cod_mag_rel AS RELATOR,
    NULL AS EX_RELATOR,
    mov_relator.cod_org_julg AS ORGAO_JULGADOR,
    mov_casosnovos.DTHR_MOV AS DATA_FASE,
    mov_casosnovos.COD_FASE AS FASE,
    processos.COD_CLASSE AS CLASSE_CNJ,
    processos.NATUREZA,
    processos.TIPO,
    processos.ELETRONICO,
    p_ano AS ANO,
    p_mes AS MES
FROM mov_casosnovos
JOIN processos ON mov_casosnovos.COD_DOC = processos.COD_DOC
JOIN local_atual ON mov_casosnovos.cod_doc = local_atual.cod_doc
LEFT JOIN mov_relator ON (mov_casosnovos.cod_doc = mov_relator.cod_doc AND mov_casosnovos.dthr_mov = mov_relator.dthr_mov)
WHERE EXTRACT(YEAR FROM mov_casosnovos.DTHR_MOV) = p_ano
AND EXTRACT(MONTH FROM mov_casosnovos.DTHR_MOV) = p_mes);

INSERT INTO MPM_LOG_EXECUCAO (nome, data_execucao) VALUES ('P_EJUD_MPM_CASOSNOVOS', SYSTIMESTAMP);
   
	EXCEPTION WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE('Error in main loop: ' || SQLERRM);
		DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
		DBMS_OUTPUT.PUT_LINE('Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
		DBMS_OUTPUT.PUT_LINE('Call Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK);
		RAISE;

END P_EJUD_MPM_CASOSNOVOS;