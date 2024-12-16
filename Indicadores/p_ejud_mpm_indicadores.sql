CREATE OR REPLACE PROCEDURE p_ejud_mpm_indicadores AS
BEGIN
	
	DBMS_OUTPUT.PUT_LINE('Procedure p_ejud_mpm_indicadores iniciada');
    
	--Indicador de Casos Novos
    BEGIN
        p_ejud_mpm_casosnovos(EXTRACT(YEAR FROM sysdate),EXTRACT(MONTH FROM sysdate));
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_casosnovos executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mpm_casosnovos: ' || SQLERRM);
    END;
   
   -- Indicador de Baixados
    BEGIN
        p_ejud_mpm_baixa(EXTRACT(YEAR FROM sysdate),EXTRACT(MONTH FROM sysdate));
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_baixa executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mpm_baixa: ' || SQLERRM);
    END;
   
   -- Indicador de Decisoes
    BEGIN
        p_ejud_mpm_decisoes(EXTRACT(YEAR FROM sysdate),EXTRACT(MONTH FROM sysdate));
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_decisoes executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mpm_decisoes: ' || SQLERRM);
    END;
   
   -- Indicador de Pendentes
    BEGIN
        p_ejud_mpm_pendentes(EXTRACT(YEAR FROM sysdate),EXTRACT(MONTH FROM sysdate));
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_pendentes executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mpm_pendentes: ' || SQLERRM);
    END;
   
    -- Indicador de Suspensos
    BEGIN
        p_ejud_mpm_suspensos(EXTRACT(YEAR FROM sysdate),EXTRACT(MONTH FROM sysdate));
        DBMS_OUTPUT.PUT_LINE('p_ejud_mpm_suspensos executada com sucesso.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in p_ejud_mpm_suspensos: ' || SQLERRM);
    END;
   
    DBMS_OUTPUT.PUT_LINE('Procedure p_ejud_mpm_indicadores completada');

END p_ejud_mpm_indicadores;