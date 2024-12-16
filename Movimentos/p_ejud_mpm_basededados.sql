CREATE OR REPLACE PROCEDURE p_ejud_mpm_basededados AS
BEGIN
    -- Movimentos de distribuição
    BEGIN
        p_ejud_mov_distribuicao;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mov_distribuicao executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mov_distribuicao: ' || SQLERRM);
    END;

    -- Movimentos de Baixa
    BEGIN
        p_ejud_mov_baixa;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mov_baixa executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mov_baixa: ' || SQLERRM);
    END;

    -- Movimentos de decisão
    BEGIN
        p_ejud_mov_decisao;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mov_decisao executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mov_decisao: ' || SQLERRM);
    END;

    -- Movimentos de suspensão
    BEGIN
        p_ejud_mov_suspensao;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mov_suspensao executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mov_suspensao: ' || SQLERRM);
    END;

    -- Movimentos de saída de suspensão
    BEGIN
        p_ejud_mov_saida_suspensao;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mov_saida_suspensao executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mov_saida_suspensao: ' || SQLERRM);
    END;

    DBMS_OUTPUT.PUT_LINE('Procedure p_ejud_mpm_basededados completada');

END p_ejud_mpm_basededados;