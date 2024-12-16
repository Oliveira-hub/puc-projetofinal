--original
SELECT * FROM ejud.t_processo@tj01;
--replica
SELECT * FROM ejud_fac_processo;

--original
SELECT * FROM ejud.movimentofisico@tj01;
--replica
SELECT * FROM ejud_fac_local_fisico;

--original
select * FROM ejud.movimento@tj01;
--replica
select * FROM ejud_fac_ult_movimento;
	
--original
SELECT * FROM ejud.movimentorelator@tj01;
--replica
SELECT * FROM ejud_fac_mov_relator;

--original
SELECT * FROM ejud.processosituacaoatual@tj01;
--replica
SELECT * FROM ejud_fac_processo_atual;

--original
SELECT * FROM ejud.competencia@tj01;
--replica
SELECT * FROM ejud_dim_competencia;

--original
SELECT * from ejud.tipoprocesso@tj01;
--replica
SELECT * from ejud_dim_tipo;

select * FROM EJUD_DIM_LOCAL_FISICO;