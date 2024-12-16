CREATE OR REPLACE FUNCTION FN_RELATOR_COMPOSICAO(
    p_codmag     IN NUMBER,
    p_codorgjulg IN NUMBER,
    p_dthrmov    IN DATE
) RETURN VARCHAR2 IS
    v_valido VARCHAR2(3);
BEGIN
    SELECT CASE 
               WHEN p_dthrmov BETWEEN h.dtinic AND NVL(h.dtfinal, SYSDATE) THEN 'SIM'
               ELSE 'NAO'
           END
    INTO v_valido
    FROM ejud.historicomagistradocomposicao@tj01 h
    WHERE h.codmag = p_codmag
      AND h.codorgjulg = p_codorgjulg
      --AND p_dthrmov BETWEEN h.dtinic AND NVL(h.dtfinal, SYSDATE) THEN 'SIM'
      AND ROWNUM = 1; -- Ensure only one record is considered

    RETURN v_valido;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'NAO';
END;

