BEGIN
	POPULADGW_DISTRIBUI_BAIXA;
END;

SELECT * FROM USER_ERRORS WHERE NAME = 'POPULADGW_DISTRIBUI_BAIXA';

--movimentos baixados

SELECT * from ejud.movimento@tj01

--movimento de distribuição 
SELECT * FROM ejud.fase@tj01 WHERE codfase IN (26,60012);

SELECT * from ejud.movimento@tj01 m and  m.codfase in (26,60012) 
and  m.coddoc not exists (select 1 from ejud.movimentorelator@tj01 where coddoc = m.coddoc and dthrmov < m.dthrmov)

SELECT count(1) FROM ejud.DGW_DISTRIBUI_BAIXA@tj01; --7377544
SELECT * FROM ejud.DGW_DISTRIBUI_BAIXA@tj01;
SELECT * FROM ejud.DGW_DISTRIBUI_BAIXA@tj01 WHERE EXTRACT(YEAR FROM BAIXA) <= 2024 AND DISTRIB IS NULL ORDER BY BAIXA DESC;


SELECT * FROM ejud.GRP_BAIXAPEND@tj01;


---- BAIXADOS

SELECT * FROM EJUD_FAC_PROCESSO WHERE CODDOC = 27449950; 
--26608984
SELECT * FROM ejud_fac_ult_movimento WHERE coddoc = 26608984;

SELECT * FROM EJUD.DGW_DISTRIBUI_BAIXA@TJ01;
SELECT * FROM EJUD.EstatCNBaixaPend_Ana@tj01 WHERE codmag = 333 AND codorgjulg = 12261;
SELECT * FROM EJUD.EstatCNBaixaPend_Ana@tj01 WHERE dataini = TO_DATE('26/02/2024 18:41:00', 'DD/MM/YYYY HH24:MI:SS');

