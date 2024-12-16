CREATE OR REPLACE FUNCTION DW_DEIGE.fn_get_ult_mov_relator (
    p_coddoc IN number,  
    p_dthrmov IN DATE     
)
RETURN DATE IS
    v_latest_dthrmov DATE;
BEGIN
    SELECT MAX(dthrmov)
    INTO v_latest_dthrmov
    FROM ejud.movimentorelator@tj01 b
    WHERE b.coddoc = p_coddoc
      AND b.dthrmov <= p_dthrmov 
      AND b.codmagrel IS NOT null;
 
    -- Se não há registro, retorne NULL
    RETURN v_latest_dthrmov;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Reorne NULL se não encontra um registro
        RETURN NULL;
END;