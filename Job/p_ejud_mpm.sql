CREATE OR REPLACE PROCEDURE p_ejud_mpm AS
    --v_cutoff_date DATE;
	--v_day_of_week NUMBER;
BEGIN
	/*
     Get the day of the week (1 = Sunday, 7 = Saturday)
    v_day_of_week := TO_NUMBER(TO_CHAR(SYSDATE, 'D'));

     Check if today is Sunday (1)
	IF v_day_of_week = 1 THEN
         Calculate the date 12 months ago from SYSDATE
        v_cutoff_date := ADD_MONTHS(SYSDATE, -12);
        Output message for deletion
    	DBMS_OUTPUT.PUT_LINE('Deletando os ultimos 12 meses da EJUD_MPM_BASEDEDADOS.');
    	 Delete records where dthrmov from the last 12 months (i.e., before the cutoff date)
        DELETE FROM EJUD_MPM_BASEDEDADOS WHERE dthrmov > v_cutoff_date;
	ELSE
        DBMS_OUTPUT.PUT_LINE('Hoje não é domingo. Nenhuma deleção realizada.');
	END IF;
	*/

    -- Execute the procedure p_ejud_mpm_basededados
    BEGIN
        p_ejud_mpm_basededados;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_basededados executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Erro na execução de p_ejud_mpm_basededados: ' || SQLERRM);
    END;

    -- Execute the procedure p_ejud_mpm_indicadores
    BEGIN
        p_ejud_mpm_indicadores;
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_indicadores executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Erro na execução de p_ejud_mpm_indicadores: ' || SQLERRM);
    END;


END p_ejud_mpm;