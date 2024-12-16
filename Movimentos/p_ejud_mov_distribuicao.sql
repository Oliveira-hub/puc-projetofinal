CREATE OR REPLACE PROCEDURE p_ejud_mov_distribuicao AS

	max_dthrmov DATE;
    -- Define the record type to hold a row structure
    TYPE t_mov_record IS RECORD (
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
    TYPE t_mov_table IS TABLE OF t_mov_record;

    l_mov_data t_mov_table;  -- This is the collection variable

    -- Batch size constant
    l_batch_size CONSTANT NUMBER := 1000;
    v_rows NUMBER := 0;  -- Variable to count inserted rows

    -- Define the cursor to fetch records from the source table
    CURSOR c_movimentos IS
        SELECT MOV.CODDOC, MOV.DTHRMOV,'DISTRIBUICAO' AS TIPOMOV, F.CODFASE, MOV.CODCOMPL1, MOV.CODCOMPL2, MOV.CODCOMPL3, NULL AS CODLOCAL, SYSDATE AS DTETL
        FROM ejud.movimento@tj01 mov
        JOIN ejud.fase@tj01 f ON mov.codfase = f.codfase
        WHERE mov.DTHRMOV > max_dthrmov
        AND mov.DTHRMOV <= SYSDATE
        AND f.codfase IN (26,36,88,488,60012,50009)
        AND f.indfaseext = 'S';

BEGIN
    DBMS_OUTPUT.PUT_LINE('Iniciando a Procedure p_ejud_mov_distribuicao: ' || TO_CHAR(SYSDATE, 'YY-MM-DD HH24:MI:SS'));
   
   	SELECT NVL(MAX(DTHRMOV), TO_DATE('1900-01-01', 'YYYY-MM-DD'))
    INTO max_dthrmov
    FROM EJUD_MPM_BASEDEDADOS 
    WHERE tipomov = 'DISTRIBUICAO';

    OPEN c_movimentos;

    LOOP
        -- Fetch a batch of records from the cursor into the collection
        FETCH c_movimentos BULK COLLECT INTO l_mov_data LIMIT l_batch_size;

        -- Exit the loop when no more records are fetched
        EXIT WHEN l_mov_data.COUNT = 0;

        -- Insert the fetched batch into the target table using FORALL
        FORALL i IN 1..l_mov_data.COUNT
            INSERT INTO EJUD_MPM_BASEDEDADOS (CODDOC, DTHRMOV, TIPOMOV, CODFASE, CODCOMPL1, CODCOMPL2, CODCOMPL3, CODLOCAL, DTETL)
            VALUES (
                l_mov_data(i).CODDOC,
                l_mov_data(i).DTHRMOV,
                l_mov_data(i).TIPOMOV,
                l_mov_data(i).CODFASE,
                l_mov_data(i).CODCOMPL1,
                l_mov_data(i).CODCOMPL2,
                l_mov_data(i).CODCOMPL3,
                l_mov_data(i).CODLOCAL,
                l_mov_data(i).DTETL
            );

        -- Count the number of inserted rows
        v_rows := v_rows + l_mov_data.COUNT;

    END LOOP;

    CLOSE c_movimentos;

    COMMIT;  -- Commit after processing all the batches
    
    DBMS_OUTPUT.PUT_LINE('Linhas inseridas: ' || v_rows);

    DBMS_OUTPUT.PUT_LINE('Finalizando a Procedure p_ejud_mov_distribuicao: ' || TO_CHAR(SYSDATE, 'YY-MM-DD HH24:MI:SS'));
   
	INSERT INTO mpm_log_execucao (nome, data_execucao) VALUES ('P_EJUD_MOV_DISTRIBUICAO', SYSTIMESTAMP);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;  -- Rollback in case of any error
        DBMS_OUTPUT.PUT_LINE('Error during batch insert: ' || SQLERRM);
        RAISE;     -- Reraise the error for further handling
END p_ejud_mov_distribuicao;
