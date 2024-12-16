BEGIN
	P_EJUD_MPM_DECISOES(2024,6);
END;

DECLARE
    v_year NUMBER := 2018; -- Start year
    v_month NUMBER;        -- Declare month variable
BEGIN
    WHILE v_year <= 2024 LOOP
        v_month := 1; -- Reset month to January for each year
        WHILE v_month <= 12 LOOP
            -- Execute the procedure for the current year and month
            P_EJUD_MPM_DECISOES(v_year, v_month);

            -- Increment the month
            v_month := v_month + 1;
        END LOOP;

        -- Increment the year
        v_year := v_year + 1;
    END LOOP;
END;


SELECT * FROM USER_ERRORS WHERE name = 'P_EJUD_MPM_DECISOES' AND type = 'PROCEDURE';

SELECT * FROM EJUD_MPM_BASEDEDADOS;

SELECT * FROM MPM_DECISOES_ANALITICO;

SELECT ano,mes,count(1) FROM MPM_DECISOES_HML GROUP BY ano,mes ORDER BY ano DESC, mes desc;

SELECT * FROM (
WITH ejud_extracao AS (
    SELECT 
        CNJ,
        FASE,
        DATA_DECISAO,
        OJ,
        MAGISTRADO
    FROM MPM_DECISOES_ANALITICO
    WHERE EXTRACT(YEAR FROM DATA_DECISAO) = 2024 AND EXTRACT(MONTH FROM DATA_DECISAO) = 9
    AND FASE IN (193,50193)
), 
jgd_extracao AS (
    SELECT 
    	CNJ,
        FASE,
        DATA_DECISAO,
        OJ,
        MAGISTRADO
	FROM DW_DEIGE.MPM_DECISOES_HML
	WHERE EXTRACT(YEAR FROM DATA_DECISAO) = 2024 AND EXTRACT(MONTH FROM DATA_DECISAO) = 9
	AND FASE IN (193,50193)
)
SELECT 
    COALESCE(ejud_extracao.CNJ, jgd_extracao.CNJ) AS CNJ,
    ejud_extracao.FASE AS EJUD_FASE,
    jgd_extracao.FASE AS JGD_FASE,
    ejud_extracao.DATA_DECISAO AS EJUD_DATA_DECISAO,
    jgd_extracao.DATA_DECISAO AS JGD_DATA_DECISAO,
    ejud_extracao.OJ AS EJUD_OJ,
    jgd_extracao.OJ AS JGD_OJ,
    ejud_extracao.MAGISTRADO AS EJUD_MAGISTRADO,
    jgd_extracao.MAGISTRADO AS JGD_MAGISTRADO,
    CASE 
        WHEN ejud_extracao.CNJ IS NOT NULL AND jgd_extracao.CNJ IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.CNJ IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN jgd_extracao.CNJ IS NOT NULL THEN 'EXTRACAO_JGD'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN jgd_extracao ON ejud_extracao.CNJ = jgd_extracao.CNJ)
WHERE FLAG IN ('AMBOS') AND EJUD_OJ <> JGD_OJ ORDER BY CNJ DESC;

SELECT * FROM (
WITH ejud_extracao AS (
    SELECT 
        CNJ,
        FASE,
        DATA_DECISAO,
        OJ,
        MAGISTRADO
    FROM MPM_DECISOES_ANALITICO
    WHERE EXTRACT(YEAR FROM DATA_DECISAO) = 2024 AND EXTRACT(MONTH FROM DATA_DECISAO) = 9
    AND FASE IN (193,50193)
), 
jgd_extracao AS (
    SELECT 
    	CNJ,
        FASE,
        DATA_DECISAO,
        OJ,
        MAGISTRADO
	FROM DW_DEIGE.MPM_DECISOES_HML
	WHERE EXTRACT(YEAR FROM DATA_DECISAO) = 2024 AND EXTRACT(MONTH FROM DATA_DECISAO) = 9
)
SELECT 
    COALESCE(ejud_extracao.CNJ, jgd_extracao.CNJ) AS CNJ,
    ejud_extracao.FASE AS EJUD_FASE,
    jgd_extracao.FASE AS JGD_FASE,
    ejud_extracao.DATA_DECISAO AS EJUD_DATA_DECISAO,
    jgd_extracao.DATA_DECISAO AS JGD_DATA_DECISAO,
    ejud_extracao.OJ AS EJUD_OJ,
    jgd_extracao.OJ AS JGD_OJ,
    ejud_extracao.MAGISTRADO AS EJUD_MAGISTRADO,
    jgd_extracao.MAGISTRADO AS JGD_MAGISTRADO,
    CASE 
        WHEN ejud_extracao.CNJ IS NOT NULL AND jgd_extracao.CNJ IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.CNJ IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN jgd_extracao.CNJ IS NOT NULL THEN 'EXTRACAO_JGD'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN jgd_extracao ON ejud_extracao.CNJ = jgd_extracao.CNJ)
WHERE FLAG IN ('AMBOS') AND EJUD_MAGISTRADO <> JGD_MAGISTRADO ORDER BY CNJ DESC;

--DIVERGENCIAS DE MAGISTRADO

--1. 0055976-50.2024.8.19.0000 - 2024-09-05 15:53:00.000 - EJUD MAGISTRADO 445 - JGD MAGISTRADO 690
--jgd errado
SELECT * FROM ejud_fac_processo WHERE num_processo = '0055976-50.2024.8.19.0000'; --27378450
SELECT coddoc,dthrmov,codfase,codmagrel,codmagdesig FROM ejud.movjulg@tj01 WHERE CODDOC = 27378450;
SELECT * FROM ejud.movimentorelator@tj01 WHERE coddoc = 27378450;

--2. 0039173-89.2024.8.19.0000 - 2024-09-12 13:00:00.000 - EJUD MAGISTRADO 422 - JGD MAGISTRADO 413
--jgd errado
SELECT * FROM ejud_fac_processo WHERE num_processo = '0039173-89.2024.8.19.0000'; --27102026
SELECT coddoc,dthrmov,codfase,codmagrel,codmagdesig FROM ejud.movjulg@tj01 WHERE CODDOC = 27102026;
SELECT * FROM ejud.movimentorelator@tj01 WHERE coddoc = 27102026;

--3. 0026991-08.2023.8.19.0000 - 2024-09-09 16:17:00.000 - EJUD MAGISTRADO 505 - JGD MAGISTRADO 644
--jgd errado
SELECT * FROM ejud_fac_processo WHERE num_processo = '0026991-08.2023.8.19.0000'; --25183420
SELECT coddoc,dthrmov,codfase,codmagrel,codmagdesig FROM ejud.movjulg@tj01 WHERE CODDOC = 25183420;
SELECT * FROM ejud.movimentorelator@tj01 WHERE coddoc = 25183420;

--4. 0005698-79.2023.8.19.0000 - 2024-09-04 16:08:00.000 - EJUD MAGISTRADO 584 - JGD MAGISTRADO 492
--jgd errado
SELECT * FROM ejud_fac_processo WHERE num_processo = '0005698-79.2023.8.19.0000'; --24873265
SELECT coddoc,dthrmov,codfase,codmagrel,codmagdesig FROM ejud.movjulg@tj01 WHERE CODDOC = 24873265;
SELECT * FROM ejud.movimentorelator@tj01 WHERE coddoc = 24873265;

