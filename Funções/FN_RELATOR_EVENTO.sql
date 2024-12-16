CREATE OR REPLACE FUNCTION FN_RELATOR_EVENTO(
    p_codmag     IN NUMBER,
    p_dthrmov    IN DATE
) RETURN VARCHAR2 IS
    v_evento VARCHAR2(3);
BEGIN
    BEGIN
        SELECT 
            CASE
                WHEN p_dthrmov > dtinic THEN 'SIM'
                ELSE 'NAO'
            END
        INTO v_evento
        FROM (
            SELECT 
                MIN(dtinic) AS dtinic
            FROM ejud.historicomagistrado@tj01
            WHERE codmag = p_codmag
              AND codtipevntmag IN (2, 3, 4, 10)
        );

        RETURN v_evento;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'NAO';
    END;
END;
