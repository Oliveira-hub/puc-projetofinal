SELECT ano,mes,count(1) AS qtd FROM MPM_CASOSNOVOS_ANALITICO WHERE PRIMEIRA_FASE = 'S' GROUP BY ano,mes ORDER BY ano asc;
SELECT ano,mes,count(1) AS qtd FROM MPM_PENDENTES_ANALITICO GROUP BY ano,mes ORDER BY ano asc;
SELECT ano,mes,count(1) AS qtd FROM MPM_BAIXADOS_ANALITICO GROUP BY ano,mes ORDER BY ano asc;
SELECT ano,mes,count(1) AS qtd FROM MPM_SUSPENSOS_ANALITICO GROUP BY ano,mes ORDER BY ano asc;
SELECT ano,mes,count(1) AS qtd FROM MPM_DECISOES_ANALITICO GROUP BY ano,mes ORDER BY ano asc;

SELECT * FROM MPM_CASOSNOVOS_ANALITICO;
SELECT * FROM MPM_PENDENTES_ANALITICO;
SELECT * FROM MPM_BAIXADOS_ANALITICO;
SELECT * FROM MPM_SUSPENSOS_ANALITICO;
SELECT * FROM MPM_DECISOES_ANALITICO;

SELECT * FROM EJUD.EstatCNBaixaPend_ana@TJ01 WHERE EXTRACT(YEAR FROM DATAINI) = 2024 ORDER BY DATAINI DESC;
SELECT DISTINCT TIPO FROM EJUD.EstatCNBaixaPend_ana@TJ01;


SELECT * FROM EJUD_AUTOSCONCLUSOS;

SELECT * FROM EJUD_GRAFO_3VP;

