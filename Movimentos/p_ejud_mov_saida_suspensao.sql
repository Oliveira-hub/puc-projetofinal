CREATE OR REPLACE PROCEDURE p_ejud_mov_saida_suspensao AS

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
        SELECT MOV.CODDOC, MOV.DTHRMOV,'SAIDA_SUSPENSAO' AS TIPOMOV, F.CODFASEEXT, MOV.CODCOMPL1, MOV.CODCOMPL2, MOV.CODCOMPL3, NULL AS CODLOCAL, SYSDATE AS DTETL
	    FROM ejud.movimento@tj01 mov
	    JOIN EJUD_DIM_FASE f ON mov.codfase = f.codfase
	    WHERE mov.DTHRMOV > max_dthrmov
		AND mov.DTHRMOV <= SYSDATE
		AND		
			(
				(f.codfaseext = 50002 AND mov.codcompl1 = 12066)
				OR (f.codfaseext = 893) 
				OR (f.codfaseext = 193) 
				OR (f.codfaseext = 50193)
			);

BEGIN
    DBMS_OUTPUT.PUT_LINE('Iniciando a Procedure p_ejud_mov_saida_suspensao: ' || TO_CHAR(SYSDATE, 'YY-MM-DD HH24:MI:SS'));
   
   	SELECT NVL(MAX(DTHRMOV), TO_DATE('1900-01-01', 'YYYY-MM-DD'))
    INTO max_dthrmov
    FROM EJUD_MPM_BASEDEDADOS 
    WHERE tipomov = 'SAIDA_SUSPENSAO';

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

    DBMS_OUTPUT.PUT_LINE('Finalizando a Procedure p_ejud_mov_saida_suspensao: ' || TO_CHAR(SYSDATE, 'YY-MM-DD HH24:MI:SS'));
   
   INSERT INTO mpm_log_execucao (nome, data_execucao) VALUES ('P_EJUD_MOV_SAIDA_SUSPENSAO', SYSTIMESTAMP);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;  -- Rollback in case of any error
        DBMS_OUTPUT.PUT_LINE('Error during batch insert: ' || SQLERRM);
        RAISE;     -- Reraise the error for further handling
END p_ejud_mov_saida_suspensao;
