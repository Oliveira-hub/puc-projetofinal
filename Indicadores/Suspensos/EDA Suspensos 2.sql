--Verificando COMPOSICAO dos MAGISTRADOS
SELECT cnj,codfase,cod_doc,classe,cod_mag,cod_org_julg,ind_composicao,ano,mes FROM MPM_SUSPENSOS_HML;
SELECT cnj,codfase,cod_doc,classe,cod_mag,cod_org_julg,ind_composicao,ano,mes FROM MPM_SUSPENSOS_HML WHERE COD_MAG = 492 AND COD_ORG_JULG = 436 AND ANO = 2024 AND MES = 9;
SELECT cnj,classe,cod_mag,cod_org_julg,ano,mes FROM MPM_SUSPENSOS_ANALITICO WHERE COD_MAG = 492 AND COD_ORG_JULG = 436 AND ANO = 2024 AND MES = 9;

SELECT CODMAG,CODORGJULG,DTINIC,DTFINAL FROM ejud.historicomagistradocomposicao@tj01 WHERE CODMAG = 492 AND CODORGJULG = 436;


SELECT * FROM (
WITH ejud_extracao AS (
    SELECT 
        CNJ,
        CLASSE,
        COD_MAG,
        COD_ORG_JULG
    FROM MPM_SUSPENSOS_ANALITICO
    WHERE ANO = 2024 AND MES = 9 
), 
jgd_extracao AS (
    SELECT 
        CNJ,
        CLASSE,
        COD_MAG,
        COD_ORG_JULG,
        IND_COMPOSICAO
    FROM MPM_SUSPENSOS_HML
    WHERE ANO = 2024 AND MES = 9
)
SELECT 
    COALESCE(ejud_extracao.CNJ, jgd_extracao.CNJ) AS CNJ,
    ejud_extracao.CLASSE AS ejud_cod_classe,
    jgd_extracao.classe AS jgd_cod_classe,
    ejud_extracao.COD_ORG_JULG AS EJUD_OJ,
    jgd_extracao.COD_ORG_JULG AS JGD_OJ,
    ejud_extracao.COD_MAG AS EJUD_MAGISTRADO,
    jgd_extracao.COD_MAG AS JGD_MAGISTRADO,
    jgd_extracao.IND_COMPOSICAO AS IND_COMPOSICAO,
    CASE 
        WHEN ejud_extracao.CNJ IS NOT NULL AND jgd_extracao.CNJ IS NOT NULL THEN 'AMBOS'
        WHEN ejud_extracao.CNJ IS NOT NULL THEN 'SISTEMA_EJUD'
        WHEN jgd_extracao.CNJ IS NOT NULL THEN 'EXTRACAO_JGD'
    END AS FLAG
FROM ejud_extracao
FULL OUTER JOIN jgd_extracao ON ejud_extracao.CNJ = jgd_extracao.CNJ)
WHERE FLAG IN ('EXTRACAO_JGD') ORDER BY CNJ DESC ;


