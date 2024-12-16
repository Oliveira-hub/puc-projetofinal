BEGIN
	p_ejud_mpm_basededados;
END;

BEGIN
	p_ejud_mov_distribuicao;
END;

SELECT * FROM USER_ERRORS WHERE name = 'p_ejud_mov_distribuicao' AND type = 'PROCEDURE';

BEGIN
	p_ejud_mov_baixa;
END;

BEGIN
	p_ejud_mov_decisao;
END;

BEGIN
	p_ejud_mov_suspensao;
END;

BEGIN
	p_ejud_mov_saida_suspensao;
END;


SELECT * FROM  MPM_LOG_EXECUCAO ORDER BY DATA_EXECUCAO DESC;