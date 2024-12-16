SELECT * FROM MPM_BAIXADOS_ANALITICO;
SELECT * FROM MPM_BAIXADOS_HMl;
SELECT ANO,MES,COUNT(1) FROM MPM_CASOSNOVOS_HML GROUP BY ANO,MES

BEGIN
	P_EJUD_MPM_BAIXA(2024,9);
END;


DECLARE
    v_year NUMBER := 2019; -- Start year
    v_month NUMBER;        -- Declare month variable
BEGIN
    WHILE v_year <= 2024 LOOP
        v_month := 1; -- Reset month to January for each year
        WHILE v_month <= 12 LOOP
            -- Execute the procedure for the current year and month
            P_EJUD_MPM_BAIXA(v_year, v_month);

            -- Increment the month
            v_month := v_month + 1;
        END LOOP;

        -- Increment the year
        v_year := v_year + 1;
    END LOOP;
END;



SELECT * FROM USER_ERRORS WHERE name = 'P_EJUD_MPM_BAIXA' AND type = 'PROCEDURE';

SELECT * FROM (
WITH ejud_extracao AS (
    SELECT 
        CNJ,
        CLASSE_CNJ,
        DATA_DA_BAIXA,
        RELATOR,
        OJ
    FROM MPM_BAIXADOS_ANALITICO
    WHERE EXTRACT(YEAR FROM DATA_DA_BAIXA) = 2024 AND EXTRACT(MONTH FROM DATA_DA_BAIXA) = 9
), 
jgd_extracao AS (
    SELECT 
        CNJ,
        CLASSE_CNJ,
        DATA_DA_BAIXA,
        RELATOR,
        OJ
	FROM DW_DEIGE.MPM_BAIXADOS_HML
	WHERE EXTRACT(YEAR FROM DATA_DA_BAIXA) = 2024 AND EXTRACT(MONTH FROM DATA_DA_BAIXA) = 9
)
SELECT 
    COALESCE(ejud_extracao.CNJ, jgd_extracao.CNJ) AS CNJ,
    ejud_extracao.CLASSE_CNJ AS EJUD_CLASSE_CNJ,
    jgd_extracao.CLASSE_CNJ AS JGD_CLASSE_CNJ,
    ejud_extracao.DATA_DA_BAIXA EJUD_DATA_DA_BAIXA,
    jgd_extracao.DATA_DA_BAIXA JGD_DATA_DA_BAIXA,
    ejud_extracao.RELATOR AS EJUD_RELATOR,
    jgd_extracao.RELATOR AS JGD_RELATOR,
    ejud_extracao.OJ AS EJUD_OJ,
    jgd_extracao.OJ AS JGD_OJ,
    CASE 
        WHEN ejud_extracao.CNJ IS NOT NULL AND jgd_extracao.CNJ IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.CNJ IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN jgd_extracao.CNJ IS NOT NULL THEN 'EXTRACAO'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN jgd_extracao ON ejud_extracao.CNJ = jgd_extracao.CNJ)
--LEFT JOIN EJUD.TIPOPROCESSO@TJ01 TP ON CLASSE_CNJ = TP.CODEXT
WHERE FLAG IN ('AMBOS')AND JGD_RELATOR <> EJUD_RELATOR ORDER BY CNJ DESC

----VALIDANDO RELATOR E OJ-----
--PROCESSO: 0017151-97.2021.8.19.0014
SELECT * FROM MPM_BAIXADOS_HML WHERE CNJ = '0017151-97.2021.8.19.0014';
SELECT * FROM MPM_BAIXADOS_ANALITICO WHERE CNJ = '0017151-97.2021.8.19.0014';

SELECT CNJ, RELATOR, OJ FROM MPM_BAIXADOS_HML WHERE CNJ = '0014378-53.2023.8.19.0000'; --608	12281
SELECT CNJ, RELATOR, OJ FROM MPM_BAIXADOS_ANALITICO WHERE CNJ = '0014378-53.2023.8.19.0000'; --465	12267

SELECT * FROM EJUD.MOVIMENTORELATOR@TJ01 WHERE CODDOC = 24991318 ORDER BY DTHRMOV DESC;

