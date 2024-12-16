--Supostamento a tabela para consulta das estatisticas de Casos Novos, Baixados e Pendentes
--Ela esta trazendo dados de todos os meses e anos anteriroes

SELECT distinct(dataini) FROM ejud.EstatCNBaixaPend@tj01 ORDER BY dataini desc;

CREATE TABLE MPM_PENDENTES_SINTETICO AS(
SELECT 
dataini, 
codmag AS relator,
codorgjulg AS oj,
qtdeprocesso AS crim, 
qtdeprocessoncrim as ncrim  
FROM ejud.EstatCNBaixaPend@tj01 
AND TIPO IN ('PEND','PENDMAG')
ORDER BY codmag ASC);

SELECT 
    TRUNC(LAST_DAY(ADD_MONTHS(dataini, -1))) + INTERVAL '23:59:59' HOUR TO SECOND AS Data, 
    codmag AS relator,
    codorgjulg AS oj,
    qtdeprocesso AS crim, 
    qtdeprocessoncrim AS ncrim  
FROM ejud.EstatCNBaixaPend@tj01 
WHERE EXTRACT(YEAR FROM dataini) = 2024
  AND EXTRACT(MONTH FROM dataini) = 10
  AND codorgjulg = 12293
  AND TIPO IN ('PEND', 'PENDMAG')
ORDER BY codmag ASC;


--Supostamento a tabela com estatisticas ANUAIS de Casos Novos, Baixados e Pendentes

SELECT * FROM ejud.EstatCNBaixaPend_Ana@tj01;

SELECT 
dataini, 
codmag AS relator,
codorgjulg AS oj,
procscriminais AS crim, 
procsnaocriminais as ncrim  
FROM ejud.EstatCNBaixaPend_Ana@tj01 
where EXTRACT(YEAR FROM dataini) = 2024
AND extract(MONTH FROM dataini) = 1
AND codorgjulg = 12293
ORDER BY codmag asc;

SELECT 
dataini, 
codmag AS relator,
codorgjulg AS oj,
procscriminais AS crim, 
procsnaocriminais as ncrim  
FROM ejud.EstatCNBaixaPend_Ana@tj01 
where EXTRACT(YEAR FROM dataini) = 2024
AND extract(MONTH FROM dataini) = 4
AND codorgjulg = 12293
ORDER BY codmag asc;

--A tabela ejud.EstatCNBaixaPend@tj01 na verdade tras dados SINTÉTICOS
--A tabela ejud.EstatCNBaixaPend_Ana@tj01 na verdade tras dados ANALITICOS

SELECT * FROM ejud.GRP_BAIXAPEND@tj01;
SELECT * FROM ejud.DGW_DISTRIBUI_BAIXA@tj01;

SELECT 
    TRUNC(LAST_DAY(ADD_MONTHS(dataini, -1))) + INTERVAL '23:59:59' HOUR TO SECOND AS Data, 
    codmag AS relator,
    codorgjulg AS oj,
    qtdeprocesso AS crim, 
    qtdeprocessoncrim AS ncrim  
FROM ejud.EstatCNBaixaPend@tj01 
WHERE TIPO IN ('PEND', 'PENDMAG')
AND codmag IS NOT NULL 
AND CODORGJULG IS NOT NULL
AND EXTRACT(YEAR FROM TRUNC(LAST_DAY(ADD_MONTHS(dataini, -1)))) = 2024;

SELECT * FROM MPM_PENDENTES_SINTETICO;
SELECT count(1) FROM MPM_PENDENTES_ANALITICO WHERE ano = 2024 AND mes = 9;

------------------------
-- 	 PENDENTES MPS    --
------------------------

BEGIN
	P_POPULADGW_DISTRIBUI_BAIXA_5;
END;

SELECT *
FROM USER_ERRORS 
WHERE name = 'P_POPULADGW_DISTRIBUI_BAIXA_5' AND type = 'PROCEDURE';

SELECT * FROM dw_deige.MPM_PENDENTES_ANALITICO;
SELECT count(1) FROM DW_DEIGE.MPM_PENDENTES WHERE ano = 2024 AND mes = 9;

------------------------------
-- 	 PROCURANDO DIFERENÇAS  --
------------------------------

SELECT * FROM DW_DEIGE.MPM_PENDENTES_ANALITICO;
SELECT * FROM EJUD_DIM_MAGISTRADO;
SELECT * FROM EJUD_DIM_ORGAO;
SELECT CODTIPPROC,descr,codext,COD_EXT_CNJ FROM EJUD_DIM_TIPO WHERE CODTIPPROC = 198 OR CODEXT = 198 OR COD_EXT_CNJ = 198;
SELECT CODTIPPROC,descr,codext,COD_EXT_CNJ FROM EJUD_DIM_TIPO WHERE CODTIPPROC = 426 OR CODEXT = 426 OR COD_EXT_CNJ = 426;
SELECT CODTIPPROC,descr,codext,COD_EXT_CNJ FROM EJUD_DIM_TIPO WHERE COD_EXT_CNJ = 426;

SELECT * FROM MPM_PENDENTES_ANALITICO WHERE ANO = 2024 AND MES = 9;
SELECT count(1) FROM MPM_PENDENTES_ANALITICO WHERE ANO = 2024 AND MES = 9;

SELECT * FROM (
WITH ejud_extracao AS 
(
	SELECT 
		CPA.NUM_PROCESSO,
		CPA.ANO,
		CPA.MES,
		M.CODMAG AS COD_MAG, 
		O.CODORGJULG AS cod_org_julg,
		CPA.COD_CLASSE
	FROM MPM_PENDENTES_ANALITICO CPA
	LEFT JOIN EJUD_DIM_MAGISTRADO M ON CPA.RELATOR = M.NOME
	LEFT JOIN EJUD_DIM_ORGAO O ON CPA.ORGAO_JULGADOR = O.NOME
	WHERE CPA.ANO = 2024 
	AND CPA.MES = 9
), 
banco_extracao as 
(
	SELECT 
		P.NUM_PROCESSO,
		P.ANO,
		P.mes,
		P.cod_ult_mag AS cod_mag, 
		P.cod_ult_org_julg AS cod_org_julg, 
		TP.CODTIPPROC,
		TP.DESCR AS CLASSE
	from DW_DEIGE.MPM_PENDENTES P
	JOIN EJUD_DIM_TIPO TP ON TP.CODTIPPROC = P.CODTIPPROC
	WHERE ANO = 2024 AND MES = 10
)
SELECT 
    COALESCE(ejud_extracao.NUM_PROCESSO, banco_extracao.NUM_PROCESSO) AS NUM_PROCESSO,
    ejud_extracao.ano AS ANO_EJUD,
    banco_extracao.ano AS ANO_EXTRACAO,
    ejud_extracao.mes AS MES_EJUD,
    banco_extracao.mes AS MES_ANO,
    ejud_extracao.COD_MAG AS COD_MAG_EJUD,
    banco_extracao.COD_MAG AS COD_MAG_EXTRACAO,
    ejud_extracao.COD_ORG_JULG AS COD_ORG_JULG_EJUD,
    banco_extracao.COD_ORG_JULG AS COD_ORG_JULG_EXTRACAO,
    banco_extracao.CODTIPPROC AS COD_CLASSE_EXTRACAO,
    banco_extracao.CLASSE AS CLASSE_EXTRACAO,
    CASE 
        WHEN ejud_extracao.NUM_PROCESSO IS NOT NULL AND banco_extracao.NUM_PROCESSO IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.NUM_PROCESSO IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN banco_extracao.NUM_PROCESSO IS NOT NULL THEN 'EXTRACAO_MPS'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN banco_extracao
ON ejud_extracao.NUM_PROCESSO = banco_extracao.NUM_PROCESSO
) WHERE FLAG IN ('AMBOS','SISTEMA_EJUD','EXTRACAO_MPS');

SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 9;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 8;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 7;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 6;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 5;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 4;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 3;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 2;
SELECT * FROM MPM_PENDENTES_ANALITICO WHERE mes = 1;
