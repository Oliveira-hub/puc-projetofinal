CREATE OR REPLACE FUNCTION DW_DEIGE.FN_GET_DTHR_ULT_BAIXA(
    p_coddoc NUMBER, p_datadistr DATE, p_anoref NUMBER, p_mesref number
)
RETURN DATE 
IS 
    v_data DATE;
BEGIN
    SELECT DTHR_MOV_BAIXA INTO v_data FROM (
        SELECT 
            mov.coddoc AS COD_DOC,
            mov.dthrmov AS DTHR_MOV_BAIXA,
            ROW_NUMBER() OVER (PARTITION BY mov.coddoc ORDER BY mov.dthrmov DESC) AS rn
        FROM EJUD.MOVIMENTO@TJ01 mov
        LEFT JOIN EJUD.FASE@TJ01 f ON mov.codfase = f.codfase 
        LEFT JOIN ejud.movimentofisico@tj01 mf ON (mov.coddoc = mf.coddoc AND mov.dthrmov = mf.dthrmov)
		WHERE f.indfaseext = 'S' 
		AND 
			(
				(f.codfase = 22)
				--870: Autos Eliminados
				OR (f.codfase = 870)
				--50011: Registro do Acordao;
				OR (f.codfase = 50011)
				--50014: Remessa a Microfilmagem;
				OR (f.codfase = 50014)
				--861: Arquivamento com complemento 246: Definitivo;
				OR (f.codfase = 861 AND mov.codcompl1 = 246)
				--50002: Certidao com complemento 1 50118: Processo Findo OU 66103: Descarte
				OR (f.codfase = 50002 AND mov.codcompl1 = 50118)
				--60: Expedição de documento com complemento 1 50007: Oficio E tendo o complemento 2
				OR (f.codfase = 60 AND mov.codcompl1 = 50007 AND mov.codcompl2 IN (50119,66356))
				--60: Expedição de documento com complemento 1 66103: Descarte E complemento 2 296: Trânsito em Julgado / Custas / Desentranhamento de peças;
				OR (f.codfase = 60 AND mov.codcompl1 = 66103 AND mov.codcompl2 = 296)
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 2: • 50052: Baixa definitiva;• 50078: Interposicao de RE/RESP; • 50079: Interposicao de RO;• 50080: Para autuar Embargos Infringentes;
				OR (f.codfase IN (123,50123) AND mov.codcompl2 IN (50052,50078,50079,50080))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 um dos seguintes: 50104, 50105, 50648, 50649
				OR (f.codfase IN (123,50123) AND mov.codcompl2 IN (50104, 50105, 50648, 50649))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com destino:
				OR (f.codfase = 123 AND mf.codlocal IN (503,519,520,521,549,2085,2593,2594,3146,3147,3148,3149,3150,3151,3205,3206,3582,3583,3584,3585,3875,5765,6919))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 
				OR (f.codfase in (123,50123) AND mov.codcompl1 = 50723 AND mf.codlocal = 515)
				-- 123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1:
				OR (f.codfase in (123,50123) AND mov.codcompl1 in (50653,50655,50656) AND mf.codlocal IN (88,2017,3763))
				----Inclusão de novos locais através da SS2022.0106136:
				OR (f.codfase = 123 AND mf.codlocal IN (3144,10682,10683,10685,10686,4580,8972))
			)
        AND mov.coddoc = p_coddoc 
		AND mov.dthrmov BETWEEN p_datadistr AND ADD_MONTHS(TO_DATE(p_anoref || '-' || LPAD(p_mesref, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
    ) WHERE rn = 1;

    RETURN v_data;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL; -- Return NULL if no data is found
    WHEN OTHERS THEN
        RAISE; -- Propagate other exceptions
END FN_GET_DTHR_ULT_BAIXA;
