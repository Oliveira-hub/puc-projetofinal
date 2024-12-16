BEGIN
	P_EJUD_MPM_CASOS_NOVOS(2018,1);
END;

DECLARE
    v_year NUMBER := 2024; -- Start year
    v_month NUMBER := 1;   -- Start month
BEGIN
    WHILE v_month <= 6 LOOP
        -- Execute the procedure for the current year and month
        P_EJUD_MPM_CASOS_NOVOS(v_year, v_month);

        -- Increment the month
        v_month := v_month + 1;
    END LOOP;
END;

SELECT ano,mes,count(1) FROM MPM_CASOSNOVOS_HML WHERE PRIMEIRA_FASE = 'S' GROUP BY ano,mes ORDER BY ano DESC, mes DESC;

SELECT *
FROM USER_ERRORS 
WHERE name = 'P_EJUD_MPM_CASOS_NOVOS' AND type = 'PROCEDURE';

/*Consulta para extração de PRIMEIRA FASE com regra do EJUD e PRIMEIRA FASE com regra EJUD*/
SELECT * FROM (
WITH ejud_extracao AS (
    SELECT 
        CNJ,
        PRIMEIRA_FASE,
    	RELATOR,
    	ORGAO_JULGADOR,
    	DATA_FASE
    FROM MPM_CASOSNOVOS_ANALITICO 
    WHERE PRIMEIRA_FASE = 'S'
    AND EXTRACT(YEAR FROM DATA_FASE) = :ano AND EXTRACT(MONTH FROM DATA_FASE) = :mes
), 
jgd_extracao AS (
    SELECT 
    	CNJ,
    	PRIMEIRA_FASE,
    	RELATOR,
    	ORGAO_JULGADOR,
    	DATA_FASE
    FROM DW_DEIGE.MPM_CASOSNOVOS_HML 
    WHERE primeira_fase = 'S'
    AND EXTRACT(YEAR FROM DATA_FASE) = :ano AND EXTRACT(MONTH FROM DATA_FASE) = :mes  
)
SELECT 
    COALESCE(ejud_extracao.CNJ, jgd_extracao.CNJ) AS CNJ,
    ejud_extracao.PRIMEIRA_FASE AS EJUD_PRIMEIRA_FASE,
    jgd_extracao.primeira_fase AS JGD_PRIMEIRA_FASE,
    ejud_extracao.RELATOR AS EJUD_RELATOR,
    jgd_extracao.RELATOR AS JGD_RELATOR,
    ejud_extracao.ORGAO_JULGADOR AS EJUD_ORGAO_JULGADOR,
    jgd_extracao.ORGAO_JULGADOR AS JGD_ORGAO_JULGADOR,
    ejud_extracao.DATA_FASE AS EJUD_DATA_FASE,
    jgd_extracao.DATA_FASE AS JGD_DATA_FASE,
    CASE 
        WHEN ejud_extracao.CNJ IS NOT NULL AND jgd_extracao.CNJ IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.CNJ IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN jgd_extracao.CNJ IS NOT NULL THEN 'EXTRACAO_JGD'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN jgd_extracao ON ejud_extracao.CNJ = jgd_extracao.CNJ)
WHERE FLAG IN ('AMBOS');

