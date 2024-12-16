SELECT * FROM EJUD.MOVIMENTO@TJ01 WHERE EXTRACT(YEAR FROM DTHRMOV) = 2024 A AND ROWNUM <= 5
26146648	2024-05-03 18:35:00.000
23598567	2024-05-07 17:26:00.000
25222567	2024-05-07 17:26:00.000
24680296	2024-05-07 17:26:00.000
15893826	2024-05-07 17:35:00.000

SELECT * FROM EJUD.MOVIMENTO@TJ01 WHERE EXTRACT(YEAR FROM DTHRMOV) = 2024 AND CODDOC IN (26146648,23598567,25222567,24680296,15893826) AND CODFASE = 22;

SELECT *
FROM user_errors
WHERE name = 'FN_GET_DTHR_ULT_BAIXA'
  AND type = 'FUNCTION';

SELECT coddoc,FN_GET_DTHR_ULT_DISTRIBUICAO(coddoc,sysdate,2024,9),FN_GET_DTHR_ULT_BAIXA(coddoc,FN_GET_DTHR_ULT_DISTRIBUICAO(coddoc,sysdate,2024,9),2024,9)  AS DTHR_ULT_DISTRIBUICAO from(
SELECT column_value AS coddoc
FROM TABLE(SYS.ODCINUMBERLIST(26992568, 26992681, 27013890, 27013891, 27013896)));

 SELECT 
            mov.coddoc AS COD_DOC,
            mov.dthrmov AS DTHR_MOV_DISTRIBUICAO,
            ROW_NUMBER() OVER (PARTITION BY mov.coddoc ORDER BY mov.dthrmov DESC) AS rn
        FROM EJUD.MOVIMENTO@TJ01 mov
        LEFT JOIN EJUD.FASE@TJ01 f ON mov.codfase = f.codfase 
        LEFT JOIN ejud.movimentofisico@tj01 mf ON (mov.coddoc = mf.coddoc AND mov.dthrmov = mf.dthrmov)
		WHERE f.indfaseext = 'S' 
		AND 
			(
				(f.CODFASE = 22)
				--870: Autos Eliminados
				OR (f.CODFASE = 870)
				--50011: Registro do Acordao;
				OR (f.CODFASE = 50011)
				--50014: Remessa a Microfilmagem;
				OR (f.CODFASE = 50014)
				--861: Arquivamento com complemento 246: Definitivo;
				OR (f.CODFASE = 861 AND mov.codcompl1 = 246)
				--50002: Certidao com complemento 1 50118: Processo Findo OU 66103: Descarte
				OR (f.CODFASE = 50002 AND mov.codcompl1 = 50118)
				--60: Expedição de documento com complemento 1 50007: Oficio E tendo o complemento 2
				OR (f.CODFASE = 60 AND mov.codcompl1 = 50007 AND mov.codcompl2 IN (50119,66356))
				--60: Expedição de documento com complemento 1 66103: Descarte E complemento 2 296: Trânsito em Julgado / Custas / Desentranhamento de peças;
				OR (f.CODFASE = 60 AND mov.codcompl1 = 66103 AND mov.codcompl2 = 296)
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 2: • 50052: Baixa definitiva;• 50078: Interposicao de RE/RESP; • 50079: Interposicao de RO;• 50080: Para autuar Embargos Infringentes;
				OR (f.CODFASE IN (123,50123) AND mov.codcompl2 IN (50052,50078,50079,50080))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 um dos seguintes: 50104, 50105, 50648, 50649
				OR (f.CODFASE IN (123,50123) AND mov.codcompl2 IN (50104, 50105, 50648, 50649))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com destino:
				OR (f.CODFASE = 123 AND mf.codlocal IN (503,519,520,521,549,2085,2593,2594,3146,3147,3148,3149,3150,3151,3205,3206,3582,3583,3584,3585,3875,5765,6919))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 
				OR (f.CODFASE in (123,50123) AND mov.codcompl1 = 50723 AND mf.codlocal = 515)
				-- 123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1:
				OR (f.CODFASE in (123,50123) AND mov.codcompl1 in (50653,50655,50656) AND mf.codlocal IN (88,2017,3763))
				----Inclusão de novos locais através da SS2022.0106136:
				OR (f.CODFASE = 123 AND mf.codlocal IN (3144,10682,10683,10685,10686,4580,8972))
			)
        AND mov.coddoc = 26992568 
		AND mov.dthrmov BETWEEN TRUNC(SYSDATE, 'YYYY') AND ADD_MONTHS(TO_DATE(2024 || '-' || LPAD(9, 2, '0') || '-01', 'YYYY-MM-DD'), 1)

