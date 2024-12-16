CREATE OR REPLACE procedure DW_DEIGE.P_EJUD_MPM_BAIXA (
    p_ano IN NUMBER,
    p_mes IN NUMBER
)
IS
BEGIN

	DELETE FROM DW_DEIGE.MPM_BAIXADOS_HML where ano = p_ano AND mes = p_mes;

	INSERT INTO DW_DEIGE.MPM_BAIXADOS_HML
	(
		NUMERO_ANTIGO,
		CNJ,
		COD_DOC,
		CLASSE_CNJ,
		NATUREZA,
		DATA_DA_BAIXA,
		mag_ult_conslusao,
		ORGAO_JULGADOR,
		relator,
		OJ,
		data_de_autuacao,
		ano,
		mes
	)
	SELECT 
		NUMERO_ANTIGO,
		CNJ,
		COD_DOC,
		CLASSE_CNJ,
		NATUREZA,
		DATA_DA_BAIXA,
		mag_ult_conslusao,
		ORGAO_JULGADOR,
		relator,
		OJ,
		data_de_autuacao,
		ano,
		mes 
	FROM (
	WITH mov_distribuicao AS (
	SELECT 
	    CODDOC AS COD_DOC,
	    min(DTHRMOV) AS DTHR_MOV_DISTRIBUICAO
	FROM EJUD_MPM_BASEDEDADOS
	WHERE TIPOMOV = 'DISTRIBUICAO'
	AND DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	GROUP BY coddoc
	), mov_baixados AS (
	SELECT 
	    CODDOC AS COD_DOC,
	    DTHRMOV AS DATA_DA_BAIXA
	FROM EJUD_MPM_BASEDEDADOS
	WHERE TIPOMOV = 'BAIXA' 
	AND DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
	), processos AS (
	    SELECT 
			P.NUM_PROCESSO,
		    P.CODDOC AS COD_DOC,
		    P.ID_PROCESSO_CORP,
		    P.COD_COMPETENCIA,
		    P.COD_TIPO_PROCESSO AS COD_CLASSE,
		    N.DESCR AS NATUREZA,
		    P.IND_ELETRONICO AS ELETRONICO,
		    P.DATA_AUTUACAO AS data_de_autuacao,
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
		)
	SELECT 
		NULL AS NUMERO_ANTIGO,
		processos.NUM_PROCESSO AS CNJ,
		mov_baixados.COD_DOC,
		processos.COD_CLASSE AS CLASSE_CNJ,
		processos.NATUREZA,
		mov_baixados.DATA_DA_BAIXA,
		NULL AS mag_ult_conslusao,
		NULL AS ORGAO_JULGADOR,
		NULL AS relator,
		NULL AS OJ,
		processos.data_de_autuacao,
		p_ano AS ano,
		p_mes AS mes,
		ROW_NUMBER() OVER (PARTITION BY mov_distribuicao.cod_doc ORDER BY mov_distribuicao.DTHR_MOV_DISTRIBUICAO - mov_baixados.DATA_DA_BAIXA DESC) AS RN
	    FROM mov_baixados
	    JOIN processos ON mov_baixados.COD_DOC = processos.COD_DOC
	    JOIN mov_distribuicao ON (mov_baixados.COD_DOC = mov_distribuicao.COD_DOC AND mov_baixados.DATA_DA_BAIXA > mov_distribuicao.DTHR_MOV_DISTRIBUICAO))
	    WHERE ANO = p_ano AND MES = p_mes AND rn = 1 AND extract(YEAR FROM DATA_DA_BAIXA) = p_ano AND extract(MONTH FROM DATA_DA_BAIXA) = p_mes;
	   
	MERGE INTO MPM_BAIXADOS_HML hml_merge
	USING (
	    SELECT mr.coddoc, 
	           mr.codmagrel,
	           mr.codorgjulg,
	           mr.dthrmov,
	           hml.data_da_baixa,
	           hml.ANO,
	           hml.MES
	    FROM MPM_BAIXADOS_HML hml 
	    JOIN  ejud.movimentorelator@tj01 mr ON hml.cod_doc = mr.coddoc and fn_get_ult_mov_relator(hml.cod_doc, hml.DATA_DA_BAIXA) = mr.dthrmov
	    where hml.ano = p_ano AND hml.mes = p_mes
	) src
	ON (hml_merge.cod_doc = src.coddoc AND hml_merge.ANO = src.ANO AND hml_merge.MES = src.MES)
	WHEN MATCHED THEN
	    UPDATE SET 
	        hml_merge.RELATOR = src.codmagrel,
	        hml_merge.OJ = src.codorgjulg;
	       
INSERT INTO MPM_LOG_EXECUCAO (nome, data_execucao) VALUES ('P_EJUD_MPM_BAIXA', SYSTIMESTAMP);
   
	EXCEPTION WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE('Error in main loop: ' || SQLERRM);
		DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
		DBMS_OUTPUT.PUT_LINE('Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
		DBMS_OUTPUT.PUT_LINE('Call Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK);
		RAISE;
    
END P_EJUD_MPM_BAIXA;