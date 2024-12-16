CREATE OR REPLACE procedure DW_DEIGE.P_EJUD_MPM_DECISOES (
    p_ano IN NUMBER,
    p_mes IN NUMBER
)
IS
BEGIN

DELETE FROM DW_DEIGE.MPM_DECISOES_HML where ano = p_ano AND mes = p_mes;

INSERT INTO DW_DEIGE.MPM_DECISOES_HML
(
	NATUREZA,
	CNJ,
	NUM_ANT,
	OJ,
	MAGISTRADO,
	DATA_DECISAO,
	CLASSE,
	FASE,
	COMPL1,
	COMPL2,
	COMPL3,
	ANO,
	MES
)
SELECT 
	NATUREZA,
	CNJ,
	NUM_ANT,
	OJ,
	MAGISTRADO,
	DATA_DECISAO,
	CLASSE,
	FASE,
	COMPL1,
	COMPL2,
	COMPL3,
	ANO,
	MES
FROM (
WITH mov_decisoes AS (
	SELECT 
	    BD.CODDOC AS COD_DOC,
	    BD.DTHRMOV AS DTHR_MOV,
	    BD.CODFASE AS COD_FASE,
	    BD.CODCOMPL1 AS COD_COMPL_1,
	    BD.CODCOMPL2 AS COD_COMPL_2,
	    BD.CODCOMPL3 AS COD_COMPL_3,
	    extract(YEAR FROM BD.dthrmov) AS ANO,
	    extract(MONTH FROM BD.dthrmov) AS MES
	FROM EJUD_MPM_BASEDEDADOS BD
	WHERE BD.TIPOMOV = 'DECISAO'
	AND BD.CODFASE IN (193,50193)
	AND extract(YEAR FROM BD.dthrmov) = p_ano 
	AND extract(MONTH FROM BD.dthrmov) = p_mes
), processos AS (
	SELECT 
	    P.NUM_PROCESSO,
	    P.CODDOC AS COD_DOC,
	    P.ID_PROCESSO_CORP,
	    P.COD_COMPETENCIA,
	    P.COD_TIPO_PROCESSO AS COD_CLASSE,
	    N.DESCR AS NATUREZA,
	    P.IND_ELETRONICO AS ELETRONICO
	FROM ejud_fac_processo P
	LEFT JOIN EJUD.TIPOPROCESSO@TJ01 TP ON TP.CODTIPPROC = P.COD_TIPO_PROCESSO
	LEFT JOIN EJUD.NATUREZA@TJ01 N ON TP.CODNAT = N.CODNAT
	WHERE TP.CODEXT NOT IN (5, 30, 40, 41, 42, 54, 73, 93, 94, 95, 297)
), julgamentos AS (
	SELECT 
	CODDOC AS COD_DOC,
	DTHRMOV as DTHR_MOV,
	CODMAGDESIG AS cod_mag_desig,
	CODMAGREL AS cod_mag_rel,
	CODORGJULG AS cod_org_julg
	FROM ejud.movjulg@tj01 
	WHERE EXTRACT(YEAR FROM DTHRMOV) = p_ano 
	AND EXTRACT(MONTH FROM DTHRMOV) = p_mes
), relatores AS (
	SELECT 
	CODDOC AS COD_DOC,
	DTHRMOV as DTHR_MOV,
	CODMAGREL AS COD_MAG_REL,
	CODORGJULG AS cod_org_julg
	FROM ejud.movimentorelator@tj01
	WHERE DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
)
SELECT 
	processos.NATUREZA,
	processos.NUM_PROCESSO AS CNJ,
	NULL AS NUM_ANT,
	COALESCE(julgamentos.cod_org_julg,relatores.cod_org_julg) AS OJ, 
	COALESCE(julgamentos.COD_MAG_DESIG,relatores.cod_mag_REL) AS MAGISTRADO,
	mov_decisoes.DTHR_MOV AS DATA_DECISAO,
	processos.COD_CLASSE AS CLASSE,
	mov_decisoes.COD_FASE AS FASE,
	mov_decisoes.COD_COMPL_1 AS COMPL1,
	mov_decisoes.COD_COMPL_2 AS COMPL2,
	mov_decisoes.COD_COMPL_3 AS COMPL3,
	mov_decisoes.ANO,
	mov_decisoes.MES
FROM mov_decisoes 
JOIN processos on mov_decisoes.cod_doc = processos.cod_doc
LEFT JOIN julgamentos ON (mov_decisoes.cod_doc = julgamentos.cod_doc AND mov_decisoes.dthr_mov = julgamentos.dthr_mov)
LEFT JOIN relatores ON (mov_decisoes.cod_doc = relatores.cod_doc AND fn_get_ult_mov_relator(mov_decisoes.cod_doc,mov_decisoes.dthr_mov) = relatores.dthr_mov));
   
INSERT INTO MPM_LOG_EXECUCAO (nome, data_execucao) VALUES ('P_EJUD_MPM_DECISOES', SYSTIMESTAMP);
   
	EXCEPTION WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE('Error in main loop: ' || SQLERRM);
		DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
		DBMS_OUTPUT.PUT_LINE('Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
		DBMS_OUTPUT.PUT_LINE('Call Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK);
		RAISE;
    
END P_EJUD_MPM_DECISOES;