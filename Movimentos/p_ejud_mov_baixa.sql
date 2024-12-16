CREATE OR REPLACE PROCEDURE p_ejud_mov_baixa AS

	max_dthrmov DATE;
    -- Define the record type to hold a row structure
    TYPE t_mpm_record IS RECORD (
        CODDOC           NUMBER,
        DTHRMOV          DATE,
        TIPOMOV          VARCHAR2(20),
        CODFASE          NUMBER,
        CODCOMPL1        NUMBER,
        CODCOMPL2        NUMBER,
        CODCOMPL3        NUMBER,
        CODLOCAL         NUMBER,
        DTETL            DATE
    );

    -- Define the PL/SQL table (collection) to hold multiple rows of the record type
    TYPE t_mpm_table IS TABLE OF t_mpm_record;

    l_mpm_data t_mpm_table;  -- This is the collection variable

    -- Batch size constant
    l_batch_size CONSTANT NUMBER := 1000;
    v_rows NUMBER := 0;  -- Variable to count inserted rows

    -- Define the cursor to fetch records from the source table
    CURSOR c_movimentos IS
        SELECT MOV.CODDOC, MOV.DTHRMOV,'BAIXA' AS TIPOMOV, F.CODFASE, MOV.CODCOMPL1, MOV.CODCOMPL2, MOV.CODCOMPL3, mf.codlocal, SYSDATE AS DTETL
	    FROM ejud.movimento@tj01 mov
	    JOIN ejud.fase@tj01 f ON mov.codfase = f.codfase
	    LEFT JOIN ejud.movimentofisico@tj01 mf ON (mov.coddoc = mf.coddoc AND mov.dthrmov = mf.dthrmov)
	    WHERE mov.DTHRMOV > max_dthrmov
	    --WHERE mov.DTHRMOV > TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')
		AND mov.DTHRMOV <= SYSDATE
		AND (
				--Movimentos de BAIXA:
				--22: Baixa Definitiva
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
				OR (f.codfase = 50002 AND mov.codcompl1 in (50118,66103))
				--60: Expedição de documento com complemento 1 50007: Oficio E tendo o complemento 2
				OR (f.codfase = 60 AND mov.codcompl1 = 50007 AND mov.codcompl2 IN (50119,66356))
				--60: Expedição de documento com complemento 1 66103: Descarte E complemento 2 296: Trânsito em Julgado / Custas / Desentranhamento de peças;
				OR (f.codfase = 60 AND mov.codcompl1 = 66103 AND mov.codcompl2 = 296)
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 2: • 50052: Baixa definitiva;• 50078: Interposicao de RE/RESP; • 50079: Interposicao de RO;• 50080: Para autuar Embargos Infringentes;
				OR (f.codfase IN (123,50123) AND mov.codcompl2 IN (50052,50078,50079,50080))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 um dos seguintes: 50104, 50105, 50648, 50649
				OR (f.codfase IN (123,50123) AND mov.codcompl2 IN (50104, 50105, 50648, 50649))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1 
				OR (f.codfase in (123,50123) AND mov.codcompl1 = 50723 AND mf.codlocal = 515)
				-- 123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com complemento 1:
				OR (f.codfase in (123,50123) AND mov.codcompl1 in (50653,50655,50656) AND mf.codlocal IN (88,2017,3763))
				--123: Remessa do Escrivão/Diretor/Secretário OU 50123: Remessa Externa, com destino:
				OR (f.codfase in (123,50123) AND mf.codlocal IN (503,519,520,521,549,2085,2593,2594,3146,3147,3148,3149,3150,3151,3205,3206,3582,3583,3584,3585,3875,5765,6919))
				--Inclusão de novos locais através da SS2022.0106136:
				OR (f.codfase in (123,50123) AND mf.codlocal IN (3144,10682,10683,10684,10685,10686,4580,8972))
				)
		AND f.indfaseext = 'S';

BEGIN
    DBMS_OUTPUT.PUT_LINE('Iniciando a Procedure p_ejud_mpm_baixa: ' || TO_CHAR(SYSDATE, 'YY-MM-DD HH24:MI:SS'));
   
   	SELECT NVL(MAX(DTHRMOV), TO_DATE('1900-01-01', 'YYYY-MM-DD'))
    INTO max_dthrmov
    FROM EJUD_MPM_BASEDEDADOS 
    WHERE tipomov = 'BAIXA';

    OPEN c_movimentos;

    LOOP
        -- Fetch a batch of records from the cursor into the collection
        FETCH c_movimentos BULK COLLECT INTO l_mpm_data LIMIT l_batch_size;

        -- Exit the loop when no more records are fetched
        EXIT WHEN l_mpm_data.COUNT = 0;

        -- Insert the fetched batch into the target table using FORALL
        FORALL i IN 1..l_mpm_data.COUNT
            INSERT INTO EJUD_MPM_BASEDEDADOS (CODDOC, DTHRMOV, TIPOMOV, CODFASE, CODCOMPL1, CODCOMPL2, CODCOMPL3, CODLOCAL, DTETL)
            VALUES (
                l_mpm_data(i).CODDOC,
                l_mpm_data(i).DTHRMOV,
                l_mpm_data(i).TIPOMOV,
                l_mpm_data(i).CODFASE,
                l_mpm_data(i).CODCOMPL1,
                l_mpm_data(i).CODCOMPL2,
                l_mpm_data(i).CODCOMPL3,
                l_mpm_data(i).CODLOCAL,
                l_mpm_data(i).DTETL
            );

        -- Count the number of inserted rows
        v_rows := v_rows + l_mpm_data.COUNT;

    END LOOP;

    CLOSE c_movimentos;

    COMMIT;  -- Commit after processing all the batches
    
    DBMS_OUTPUT.PUT_LINE('Linhas inseridas: ' || v_rows);

    DBMS_OUTPUT.PUT_LINE('Finalizando a Procedure p_ejud_mpm_baixa: ' || TO_CHAR(SYSDATE, 'YY-MM-DD HH24:MI:SS'));
   
   INSERT INTO mpm_log_execucao (nome, data_execucao) VALUES ('P_EJUD_MOV_BAIXA', SYSTIMESTAMP);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;  -- Rollback in case of any error
        DBMS_OUTPUT.PUT_LINE('Error during batch insert: ' || SQLERRM);
        RAISE;     -- Reraise the error for further handling
END p_ejud_mov_baixa;