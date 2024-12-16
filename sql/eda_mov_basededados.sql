DELETE FROM EJUD_MPM_BASEDEDADOS emb WHERE TIPOMOV = 'DECISAO';

BEGIN 
	p_ejud_mpm;
END;

SELECT MOV.CODDOC, MOV.DTHRMOV,'SAIDA_SUSPENSAO' AS TIPOMOV, F.CODFASEEXT, MOV.CODCOMPL1, MOV.CODCOMPL2, MOV.CODCOMPL3, NULL AS CODLOCAL, SYSDATE AS DTETL
	    FROM ejud.movimento@tj01 mov
	    JOIN EJUD_DIM_FASE f ON mov.codfase = f.codfase
	    --WHERE mov.DTHRMOV > max_dthrmov
		AND mov.DTHRMOV <= SYSDATE
		AND		
			(
				(f.codfaseext = 50002 AND mov.codcompl1 = 12066)
				OR (f.codfaseext = 893) 
				OR (f.codfaseext = 193) 
				OR (f.codfaseext = 50193)
			)
		AND mov.coddoc = 27096743;
		
SELECT * FROM ejud_dim_fase WHERE codfaseext IN (50002,893,193,50193);

SELECT * FROM ejud.movimento@tj01 WHERE coddoc = 27096743 ORDER BY dthrmov desc;