CREATE OR REPLACE FUNCTION DW_DEIGE.FN_GET_DTHR_ULT_DISTRIBUICAO(
    p_coddoc NUMBER, p_dataref DATE, p_ano NUMBER, p_mes number
)
RETURN DATE 
IS 
    v_data DATE;
BEGIN
    SELECT DTHR_MOV_DISTRIBUICAO INTO v_data FROM (
        SELECT 
            mov.coddoc AS COD_DOC,
            mov.dthrmov AS DTHR_MOV_DISTRIBUICAO,
            ROW_NUMBER() OVER (PARTITION BY mov.coddoc ORDER BY mov.dthrmov DESC) AS rn
        FROM EJUD.MOVIMENTO@TJ01 mov
        LEFT JOIN EJUD.FASE@TJ01 f 
            ON mov.codfase = f.codfase 
		WHERE f.indfaseext = 'S' 
		AND mov.codfase IN (26, 36)
		--AND mov.coddoc = 26992568 
        AND mov.coddoc = p_coddoc 
		AND mov.dthrmov < p_dataref
		AND MOV.DTHRMOV < ADD_MONTHS(TO_DATE(p_ano || '-' || LPAD(p_mes, 2, '0') || '-01', 'YYYY-MM-DD'), 1)
    ) WHERE rn = 1;

    RETURN v_data;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL; -- Return NULL if no data is found
    WHEN OTHERS THEN
        RAISE; -- Propagate other exceptions
END FN_GET_DTHR_ULT_DISTRIBUICAO;
