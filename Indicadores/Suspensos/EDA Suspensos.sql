DELETE FROM EJUD_MPM_BASEDEDADOS WHERE TIPOMOV = 'SUSPENSAO';
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE TIPOMOV = 'SUSPENSAO';

BEGIN
	p_ejud_mov_suspensao;
END;


SELECT * FROM MPM_SUSPENSOS_HML; 

DECLARE
    v_year NUMBER := 2018; -- Start year
    v_month NUMBER := 1;   -- Start month
BEGIN
    WHILE v_month <= 12 LOOP
        -- Execute the procedure for the current year and month
        P_EJUD_MPM_SUSPENSOS(v_year, v_month);

        -- Increment the month
        v_month := v_month + 1;
    END LOOP;
END;


SELECT 
    COALESCE(ph.ano, pa.ano) AS ano,
    COALESCE(ph.mes, pa.mes) AS mes,
    COALESCE(ph.count_suspensos_hml, 0) AS count_suspensos_hml,
    COALESCE(pa.count_suspensos_analitico, 0) AS count_suspensos_analitico,
    COALESCE(ph.count_suspensos_hml, 0) - COALESCE(pa.count_suspensos_analitico, 0) AS dif
FROM 
    (SELECT ano, mes, COUNT(1) AS count_suspensos_hml
     FROM MPM_SUSPENSOS_HML 
     GROUP BY ano, mes) ph
FULL OUTER JOIN 
    (SELECT ano, mes, COUNT(1) AS count_suspensos_analitico
     FROM MPM_SUSPENSOS_ANALITICO 
     GROUP BY ano, mes) pa
ON ph.ano = pa.ano AND ph.mes = pa.mes
ORDER BY ano DESC, mes DESC;

BEGIN
	P_EJUD_MPM_SUSPENSOS(2024,11);
END;

SELECT *
FROM USER_ERRORS 
WHERE name = 'P_EJUD_MPM_SUSPENSOS' AND type = 'PROCEDURE';

SELECT ind_composicao,count(1) FROM MPM_SUSPENSOS_HML WHERE ano = 2024 AND mes = 9 GROUP BY ind_composicao;

SELECT * FROM (
WITH ejud_extracao AS (
    SELECT 
        CNJ,
        CLASSE,
        COD_MAG,
        COD_ORG_JULG,
        ANO,
        MES
    FROM MPM_SUSPENSOS_ANALITICO
    WHERE ANO = 2024 AND MES = 11
), 
jgd_extracao AS (
    SELECT 
        CNJ,
        CLASSE,
        COD_MAG,
        COD_ORG_JULG,
        ANO,
        MES
    FROM MPM_SUSPENSOS_HML
    WHERE ANO = 2024 AND MES = 11 AND IND_GTDL = 'NAO' AND IND_VALIDO = 'SIM' AND IND_EVENTO = 'NAO'
)
SELECT 
    COALESCE(ejud_extracao.CNJ, jgd_extracao.CNJ) AS CNJ,
    ejud_extracao.CLASSE AS ejud_cod_classe,
    jgd_extracao.classe AS jgd_cod_classe,
    ejud_extracao.COD_ORG_JULG AS EJUD_OJ,
    jgd_extracao.COD_ORG_JULG AS JGD_OJ,
    ejud_extracao.COD_MAG AS EJUD_MAGISTRADO,
    jgd_extracao.COD_MAG AS JGD_MAGISTRADO,
    CASE 
        WHEN ejud_extracao.CNJ IS NOT NULL AND jgd_extracao.CNJ IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.CNJ IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN jgd_extracao.CNJ IS NOT NULL THEN 'EXTRACAO_JGD'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN jgd_extracao ON ejud_extracao.CNJ = jgd_extracao.CNJ)
WHERE FLAG IN ('EXTRACAO_JGD') ORDER BY CNJ DESC ;

--PROCESSOS NA EXTRACAO_JGD 2024/09

--1. 0836463-07.2022.8.19.0203
SELECT * FROM MPM_SUSPENSOS_HML WHERE cnj = '0836463-07.2022.8.19.0203';
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 27025533 ORDER BY DTHRMOV DESC;
SELECT * FROM ejud.historicomagistradocomposicao@tj01 WHERE codmag = 557 AND CODORGJULG = 12288 ORDER BY dtinic desc;
--codorgjulg 12288	codmag 557

SELECT * FROM ejud.historicomagistradocomposicao@tj01 WHERE codmag = 557 ORDER BY dtinic desc;
--DT INIC 2024-09-13 00:00:00.000	DTFINAL 2024-09-13 23:59:00.000

SELECT hist.codmag,p.nome,hist.dtinic,hist.dtfinal,hist.codtipevntmag,ev.descr FROM ejud.historicomagistrado@tj01 hist
LEFT JOIN ejud.tipoeventomagistrado@tj01 ev ON hist.codtipevntmag = ev.codtipevntmag
LEFT JOIN ejud.magistrado@tj01 mag ON hist.codmag = mag.codmag
LEFT JOIN ejud.pessoa@tj01 p ON mag.codpess = p.codpess
WHERE hist.codmag = 557 AND ev.codtipevntmag IN (2,3,4,10) ORDER by hist.dtinic desc;

--façam parte da composição de órgão julgador

--2. 0807182-65.2022.8.19.0054
SELECT * FROM EJUD_FAC_PROCESSO WHERE NUM_PROCESSO = '0807182-65.2022.8.19.0054'; --26888540
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 26888540 ORDER BY DTHRMOV DESC;

--3. 0806282-78.2022.8.19.0023
SELECT * FROM MPM_SUSPENSOS_HML WHERE cnj = '0806282-78.2022.8.19.0023'; --25241028
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 25241028 ORDER BY DTHRMOV DESC;

SELECT * FROM MPM_SUSPENSOS_ANALITICO WHERE CNJ = '0807182-65.2022.8.19.0054';

--4. 0802857-41.2024.8.19.0001
SELECT * FROM EJUD_FAC_PROCESSO WHERE NUM_PROCESSO = '0802857-41.2024.8.19.0001'; --27191432
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 27191432 ORDER BY DTHRMOV DESC;

SELECT * FROM MPM_SUSPENSOS_ANALITICO WHERE CNJ = '0802857-41.2024.8.19.0001';

--5. 0503828-51.2014.8.19.0001
SELECT * FROM EJUD_FAC_PROCESSO WHERE NUM_PROCESSO = '0503828-51.2014.8.19.0001'; --11596958
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 11596958 ORDER BY DTHRMOV DESC;

--codorgjulg 442	codmag 520
SELECT * FROM ejud.historicomagistradocomposicao@tj01 WHERE codmag = 442 ORDER BY dtinic desc;

--6. 0498640-43.2015.8.19.0001
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0498640-43.2015.8.19.0001'; --16047597
SELECT * FROM MPM_SUSPENSOS_ANALITICO WHERE CNJ = '0498640-43.2015.8.19.0001'; --2020-02-18 13:00:00.000
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 16047597 ORDER BY DTHRMOV DESC;
--codorgjulg 3904	codmag 592
SELECT codmag,codorgjulg,dtinic,dtfinal FROM ejud.historicomagistradocomposicao@tj01 WHERE codmag = 592 ORDER BY dtinic desc;

--7. 0494502-33.2015.8.19.0001
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0494502-33.2015.8.19.0001'; --14923279
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 14923279 ORDER BY DTHRMOV DESC;
SELECT * FROM ejud.historicomagistrado@tj01 WHERE codmag = 374 ORDER BY dtinic desc;

--8. 0494424-39.2015.8.19.0001
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0494424-39.2015.8.19.0001'; --13623802
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 13623802 ORDER BY DTHRMOV DESC;

--9. 0490576-44.2015.8.19.0001
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0490576-44.2015.8.19.0001'; --14430225
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 14430225 ORDER BY DTHRMOV DESC;

--10. 0489420-21.2015.8.19.0001
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0489420-21.2015.8.19.0001'; --coddoc 12197962 codmag 583 orgao julgador 442
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 12197962 ORDER BY DTHRMOV DESC;

--Processos da Luciana 


SELECT * FROM ejud.historicomagistradocomposicao@tj01 WHERE codmag = 465 AND codorgjulg = 431 ORDER BY dtinic desc;

--0097152-31.2009.8.19.0001	237	9240
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0097152-31.2009.8.19.0001' ORDER BY ano DESC, mes desc; --coddoc 15058173 codmag 465 orgao julgador 431
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 15058173 ORDER BY DTHRMOV DESC; 
SELECT * FROM ejud.movimentorelator@tj01 WHERE coddoc = 15058173;

--0006834-33.2016.8.19.0073	237	449
SELECT * FROM MPM_SUSPENSOS_HML WHERE CNJ = '0006834-33.2016.8.19.0073' ORDER BY ano DESC, mes desc; --13269784
SELECT * FROM EJUD_MPM_BASEDEDADOS WHERE CODDOC = 13269784 ORDER BY DTHRMOV DESC; 
SELECT * FROM ejud.movimentorelator@tj01 WHERE coddoc = 13269784;

SELECT mag.codmag,p.nome FROM  ejud.magistrado@tj01 mag
LEFT JOIN ejud.pessoa@tj01 p ON mag.codpess = p.codpess
WHERE mag.codmag in (237,465);

SELECT mag.codmag,p.nome FROM  ejud.magistrado@tj01 mag
LEFT JOIN ejud.pessoa@tj01 p ON mag.codpess = p.codpess
WHERE p.nome LIKE '%CUSTODIO%';

SELECT * FROM MPM_SUSPENSOS_HML WHERE cod_mag = 465 AND ano = 2024 AND mes = 9;
