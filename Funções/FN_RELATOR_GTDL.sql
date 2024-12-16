CREATE OR REPLACE FUNCTION FN_RELATOR_GTDL(
    p_codmag     IN NUMBER,
    p_dthrmov    IN DATE
) RETURN VARCHAR2 IS
    v_gtdl VARCHAR2(3);
BEGIN
    SELECT CASE 
               WHEN p_dthrmov > h.data_de_inatividade THEN 'SIM'
               ELSE 'NAO'
           END
    INTO v_gtdl
    FROM vw_relatorio_gtdl h
    WHERE h.codmag = p_codmag;

    RETURN v_gtdl;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'NAO';
END;
