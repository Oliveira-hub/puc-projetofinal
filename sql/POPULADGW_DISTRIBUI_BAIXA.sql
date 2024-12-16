CREATE OR REPLACE procedure POPULADGW_DISTRIBUI_BAIXA

/* MTA - 07/05/2020 - REQ2020.0048744 - RelatÃ³rio de Mensal de Casos Novos, Baixados e Pendentes

                      AutomatizaÃ§Ã£o do prenchimento da tabela DGW_DISTRIBUI_BAIXA, para rodar em JOB */

is

	v_i			number;
	datapesqIni		date;
	datapesqFim 		date;
	datapesq		date;
	nchegou2014		boolean;
	qtdeProcesso		number;
	qtdeProcessoNCrim	number;
	antigo			number;
	nAntigo			number;
	qtdeelets		number;
	qtde1grau		number;

begin

	--dbms_output.put_line('Inicio '||to_char(sysdate,'hh24:mi:ss'));

		--MTA - Combinado com JoÃ£o Paulo para trocar o delete pelo execute immediate e truncate table, para performance

		--delete DGW_DISTRIBUI_BAIXA;

		--commit;

	--execute immediate ('truncate table DGW_DISTRIBUI_BAIXA');

	--dbms_output.put_line('Conteudo anterior apagado: '||to_char(sysdate,'hh24:mi:ss'));

	/* 
	v_i := 0;
	for v_doc in
	(
		SELECT	coddoc
		FROM	t_processo 
		order by 1
	)
	loop
		INSERT INTO DGW_DISTRIBUI_BAIXA
		SELECT  v_doc.CODDOC
			--DISTR
			, (
				select min(m.dthrmov)
				from   movimento m
				where  m.coddoc = v_doc.CODDOC
					and  m.codfase in (26,60012)
					and  not exists (select 1 from movimentorelator
					where coddoc = m.coddoc and dthrmov < m.dthrmov)
				)
			--BAIXA
			, (
					select  min(m1.dthrmov)
					from    Movimento m1
					where   v_doc.CODDOC = m1.coddoc
					and   (
						CodFase in (870,22,50011,50014)
					or  (CodFase = 861 and CodCompl1 = 246)
					or  (CodFase = 50002 and ( CodCompl1 in (50118,66103) or CodCompl2 in (50118,66103) ) )
					or  (CodFase = 60 and CodCompl1 = 50007 and CodCompl2 in ( 50119 , 66356 ) )
					or  (CodFase = 60 and CodCompl1 = 66103 and CodCompl2 = 296 )
					or  (CodFase in (123,50123) and CodCompl2 = 50052)
					or  (CodFase in (123,50123) and CodCompl1 in (50104,50648,50105,50649))
					or  (CodFase in (123,50123) --and CodCompl2 in (50078,50079,50623,50625,50898)
						and exists (  select 1 from MovimentoFisico mf
							where mf.CodDoc = m1.CodDoc
							and mf.DtHrMov = m1.DtHrMov
							and mf.CodLocal in (503, 519, 520, 521, 2593, 3146, 3147, 3148, 3149, 3150,
								3151, 2594, 3582, 3583, 3584, 3585, 3875, 2085, 5765, 3205, 3206, 549, 6919,
								3144, 4580, 8972, 10682, 10683, 10684, 10685, 10686)) )
					or  (CodFase in (123,50123) and CodCompl2 in (50079, 50080, 50078) )
					or  ( CodFase in (123,50123) and ( codcompl1 = 50723 or exists
							(
								select 1 from MovimentoFisico mf
								where mf.CodDoc = m1.CodDoc
								and mf.DtHrMov = m1.DtHrMov
								and mf.CodLocal = 515
							)
							) )
					or  ( CodFase in (123,50123) and ( codcompl1 in (50655, 50653, 50656) or exists
							(
								select 1 from MovimentoFisico mf
								where mf.CodDoc = m1.CodDoc
								and mf.DtHrMov = m1.DtHrMov
								and mf.CodLocal in ( 88, 2017, 3763 )
							)
							) )
					)
			)
		FROM dual;

		v_i := v_i + 1;
		if v_i > 999 then
			commit;
			v_i := 0;
		end if;
	end loop;

	commit;
	*/
	
	
	/*
	 
	update dgw_distribui_baixa a
	set    baixa = nvl((
					select  min(m1.dthrmov)
					from    Movimento m1
					where   a.coddoc = m1.coddoc
					and     m1.dthrmov > a.distrib
					and   (
						CodFase in (870,22,50011,50014)
					or  (CodFase = 861 and CodCompl1 = 246)
					or  (CodFase = 50002 and ( CodCompl1 in (50118,66103) or CodCompl2 in (50118,66103) ) )
					or  (CodFase = 60 and CodCompl1 = 50007 and CodCompl2 in ( 50119 , 66356 ) )
					or  (CodFase = 60 and CodCompl1 = 66103 and CodCompl2 = 296 )
					or  (CodFase in (123,50123) and CodCompl2 = 50052)
					or  (CodFase in (123,50123) and CodCompl1 in (50104,50648,50105,50649))
					or  (CodFase in (123,50123) --and CodCompl2 in (50078,50079,50623,50625,50898)
						and exists (  select 1 from MovimentoFisico mf
							where mf.CodDoc = m1.CodDoc
							and mf.DtHrMov = m1.DtHrMov
								and mf.CodLocal in (503, 519, 520, 521, 2593, 3146, 3147, 3148, 3149, 3150,
									3151, 2594, 3582, 3583, 3584, 3585, 3875, 2085, 5765, 3205, 3206, 549, 6919,
									3144, 4580, 8972, 10682, 10683, 10684, 10685, 10686)) )
					or  (CodFase in (123,50123) and CodCompl2 in (50079, 50080, 50078) )
					or  ( CodFase in (123,50123) and ( codcompl1 = 50723 or exists
							(
								select 1 from MovimentoFisico mf
								where mf.CodDoc = m1.CodDoc
								and mf.DtHrMov = m1.DtHrMov
								and mf.CodLocal = 515
							)
							) )
					or  ( CodFase in (123,50123) and ( codcompl1 in (50655, 50653, 50656) or exists
							(
								select 1 from MovimentoFisico mf
								where mf.CodDoc = m1.CodDoc
								and mf.DtHrMov = m1.DtHrMov
								and mf.CodLocal in ( 88, 2017, 3763 )
							)
							) )
					)), null)
	where a.baixa < a.distrib;

	commit;

	--dbms_output.put_line('Fim '||to_char(sysdate,'hh24:mi:ss'));

	delete EstatCNBaixaPend;

	execute immediate ('truncate table EstatCNBaixaPend_ana');*/

	-------------------------------

	------ GERACAO DOS NOVOS ------

	-------------------------------

	--MTA 01/02/2021 - REQ2021.0000420 - Erro de ORA-01843: not a valid month

	--datapesqIni := '01/01/2008';

	/*
	 
	datapesqIni := to_date('01/01/2008', 'dd/mm/yyyy');
	nchegou2014 := true;

	while nchegou2014
	loop
		datapesqFim := add_months( trunc(datapesqIni, 'MM'), 1)- 1/86400;
		select sum(case when NVL(c.codnat, 0) = 2 then 1 else 0 end) qtdeprocsCriminais
			   , sum(case when NVL(c.codnat, 0) <> 2 or c.codnat is null then 1 else 0 end) qtdeProcsNaoCriminais
			   --, sum(case when NVL(p.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsAntigos
			   --, sum(case when NVL(p.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsNaoAntigos
			   , sum(case when nvl(p.indeletr, 'N') = 'S' then 1 else 0 end) qtdeelets
			   , sum(case when nvl(tp.codtipproc, 9) in (1, 2, 50, 51, 76, 168, 227) then 1 else 0 end) qtde1grau
		into   qtdeProcesso, qtdeProcessoNCrim, qtdeelets, qtde1grau
		from   DGW_DISTRIBUI_BAIXA b
			   , processosituacaoatual psa
			   , processo p
			   , competencia c
			   , tipoprocesso tp
		where  b.distrib is not null
		  and  b.distrib between datapesqini and datapesqFim
		  and  b.coddoc = psa.coddoc
		  and  psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)
		  and  psa.coddoc = p.coddoc
		  and  p.codcompt = c.codcompt
		  and  c.indconsrec = 'N'
		  and  p.codtipproc = tp.codtipproc
		  and  p.idproccorp is not null
		  and  p.codcompt <> 9
		  and  tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95);

		--dbms_output.put_line(TO_CHAR(datapesqini, 'DD/MM/RRRR HH24:MI:SS') || ',' || to_char(datapesqfim, 'DD/MM/RRRR HH24:MI:SS') || ',' || qtdeProcesso || ',' || qtdeProcessoNCrim || ',' || qtdeelets || ',' || qtde1grau);

		insert into EstatCNBaixaPend  (tipo, dataini, datafim, qtdeProcesso, qtdeProcessoNCrim, qtdeelets, qtde1grau)
		values ('CN', datapesqini, datapesqfim, qtdeProcesso, qtdeProcessoNCrim, qtdeelets, qtde1grau);
		commit;

		if datapesqini = add_months(trunc(sysdate,'mm'),-1) then
			nchegou2014 := false;
		end if;
		datapesqini :=  add_months(datapesqini, 1);
	end loop;

	*/

	----------------------------------

	------ GERACAO DOS BAIXADOS ------

	----------------------------------

	--MTA 01/02/2021 - REQ2021.0000420 - Erro de ORA-01843: not a valid month

	--datapesqIni := '01/01/2008';

	/*
	 
	datapesqIni := to_date('01/01/2008', 'dd/mm/yyyy');
	nchegou2014 := true;
	while nchegou2014

	loop
		datapesqFim := add_months( trunc(datapesqIni, 'MM'), 1)- 1/86400;
		select sum(case when NVL(c.codnat, 0) = 2 then 1 else 0 end) qtdeprocsCriminais
			   , sum(case when NVL(c.codnat, 0) <> 2 or c.codnat is null then 1 else 0 end) qtdeProcsNaoCriminais
			   --, sum(case when NVL(p.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsAntigos
			   --, sum(case when NVL(p.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsNaoAntigos
			   , sum(case when nvl(p.indeletr, 'N') = 'S' then 1 else 0 end) qtdeelets
			   , sum(case when nvl(tp.codtipproc, 9) in (1, 2, 50, 51, 76, 168, 227) then 1 else 0 end) qtde1grau
		into   qtdeProcesso, qtdeProcessoNCrim, qtdeelets, qtde1grau
		from   DGW_DISTRIBUI_BAIXA b
			   , processosituacaoatual psa
			   , processo p
			   , competencia c
			   , tipoprocesso tp
		where  distrib is not null
		  and  distrib < datapesqFim
		  and  b.baixa between datapesqini and datapesqFim
		  and  b.coddoc = psa.coddoc
		  and  psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)
		  and  psa.coddoc = p.coddoc
		  and  p.codcompt = c.codcompt
		  and  c.indconsrec = 'N'
		  and  p.codtipproc = tp.codtipproc
		  and  p.idproccorp is not null
		  and  p.codcompt <> 9
		  and  tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95);

		--dbms_output.put_line(TO_CHAR(datapesqini, 'DD/MM/RRRR HH24:MI:SS') || ',' || to_char(datapesqfim, 'DD/MM/RRRR HH24:MI:SS') || ',' || qtdeProcesso || ',' || qtdeProcessoNCrim || ',' || qtdeelets || ',' || qtde1grau);

		insert into EstatCNBaixaPend (tipo, dataini, datafim, qtdeProcesso, qtdeProcessoNCrim, qtdeelets, qtde1grau)
		values ('BAIXA', datapesqini, datapesqfim, qtdeProcesso, qtdeProcessoNCrim, qtdeelets, qtde1grau);
		commit;
		if datapesqini = add_months(trunc(sysdate,'mm'),-1) then
			nchegou2014 := false;
		end if;
		datapesqini :=  add_months(datapesqini, 1);
	  end loop;

	*/

	-----------------------------------

	------ GERACAO DOS PENDENTES ------

	-----------------------------------

	--IF sval_Const('$AtivarPopularDgwDistriBaixaPe') = 'S' THEN
	--PopularDgwDistriBaixaPend;
	--ELSE
		--MTA 01/02/2021 - REQ2021.0000420 - Erro de ORA-01843: not a valid month
		--datapesq := '01/01/2008';
		
		datapesq := to_date('01/01/2008', 'dd/mm/yyyy');
		nchegou2014 := true;
	
		while nchegou2014
		loop
			SELECT	
				sum(case when NVL(c.codnat, 0) = 2 then 1 else 0 end) qtdeprocsCriminais,
				sum(case when NVL(c.codnat, 0) <> 2 or c.codnat is null then 1 else 0 end) qtdeProcsNaoCriminais,
				sum(case when NVL(p.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsAntigos,
				sum(case when NVL(p.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsNaoAntigos
			INTO qtdeProcesso, qtdeProcessoNCrim, antigo, nantigo
			FROM ejud.DGW_DISTRIBUI_BAIXA@tj01 b, ejud.processosituacaoatual@tj01 psa, ejud.processo@tj01 p, ejud.competencia@tj01 c, ejud.tipoprocesso@tj01 tp
			WHERE distrib is not null
			AND	distrib < (datapesq  - 1/86400 )
			AND	(b.baixa >= datapesq or b.baixa is null)
			AND	b.coddoc = psa.coddoc
			AND	psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)
			AND	psa.coddoc = p.coddoc
			AND	p.codcompt = c.codcompt
			AND	c.indconsrec = 'N'
			AND	p.codcompt <> 9
			AND	p.codtipproc = tp.codtipproc
			AND	p.idproccorp is not null
			AND	tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95);
			--dbms_output.put_line(to_char((datapesq - 1/86400), 'DD/MM/RRRR HH24:MI:SS') || ',' || qtdeProcesso || ',' || qtdeProcessoNCrim || ',' || antigo || ',' || nantigo);
			insert into EstatCNBaixaPend (tipo, dataini, qtdeProcesso, qtdeProcessoNCrim, antigo, nantigo)
			values ('PEND', datapesq, qtdeProcesso, qtdeProcessoNCrim, antigo, nantigo);

			commit;

			if datapesq = trunc(sysdate,'mm') THEN
				nchegou2014 := false;
			end if;
			datapesq := add_months(datapesq, 1);
		end loop;

		--------------------------------------------------

		------ GERACAO DOS PENDENTES POR MAGISTRADO ------

		--------------------------------------------------

		--MTA 01/02/2021 - REQ2021.0000420 - Erro de ORA-01843: not a valid month

		--datapesq := '01/01/2008';

		/*
		 
		datapesq := to_date('01/01/2008', 'dd/mm/yyyy');
		nchegou2014 := true;

		while nchegou2014
		loop
			for v_dados in
			(
				select	r.codmagrel, r.codorgjulg, sum(case when NVL(tp.codnat, 0) = 2 then 1 else 0 end) qtdeprocsCriminais,
					sum(case when NVL(tp.codnat, 0) <> 2 or tp.codnat is null then 1 else 0 end) qtdeProcsNaoCriminais,
					sum(case when NVL(p.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsAntigos,
					sum(case when NVL(p.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsNaoAntigos,
					sum(decode(
						(
							select	max(dthrmov)
							from	Movimento m1
							where 	m1.coddoc = p.coddoc
							AND	m1.dthrmov < datapesq
							and	(
									(m1.codfase in (3,20) and ((m1.codcompl1 = 25) OR (m1.codcompl1 = 1013 and m1.codcompl2 = 1015))) or
									(m1.codfase in (11009,21,30) and m1.codcompl1 = 11025) or
									(m1.codfase = 50002 and m1.codcompl1 in (306, 12065)) or
									(m1.codfase = 50040) or
									(m1.CodFase = 29 and m1.codcompl1 = 1013) OR
									(m1.CodFase = 861 and ((m1.codcompl1 in (690,50068)) OR (m1.codcompl1 in (359,360,361,362,363,445,50069) and m1.codcompl2 in (413,414,421))))
								)
							and	not exists
									(
										select	1
										from	Movimento m2
										where	m2.coddoc = m1.coddoc
										and	m2.dthrmov > m1.dthrmov
										and	m2.dthrmov < datapesq
										and 	((m2.codfase in (24, 193, 893)) OR (m2.codfase = 50002 and m2.codcompl1 = 12066))
									)
						), null,0,1)) QtdeSusp
				from	DGW_DISTRIBUI_BAIXA b, processosituacaoatual psa, processo p, competencia c, tipoprocesso tp, movimentorelator r
				WHERE	distrib is not null
				and	distrib < (datapesq  - 1/86400)
				and	(b.baixa >= datapesq or b.baixa is null)
				and	b.coddoc = psa.coddoc
				and	psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)
				and	psa.coddoc = p.coddoc
				and	p.codcompt = c.codcompt
				and	c.indconsrec = 'N'
				and	p.codcompt <> 9
				and	p.codtipproc = tp.codtipproc
				and	p.idproccorp is not null
				and	tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95, 310)
				and	r.coddoc = p.coddoc
				and	r.dthrmov = (select max(dthrmov) from movdistribuicao where codfase in (26,36) and coddoc = p.coddoc and dthrmov < datapesq)
				group	by r.codmagrel, r.codorgjulg
			)
			loop
				insert into EstatCNBaixaPend (tipo, dataini, qtdeProcesso, qtdeProcessoNCrim, codmag, codorgjulg, antigo, nantigo, sus2)
				values ('PENDMAG', datapesq, v_dados.qtdeprocsCriminais, v_dados.qtdeProcsNaoCriminais, v_dados.codmagrel, v_dados.codorgjulg, v_dados.QtdeProcsAntigos, v_dados.QtdeProcsNaoAntigos, v_dados.QtdeSusp);
				commit;
			end loop;


			if datapesq = trunc(sysdate,'mm') then
				nchegou2014 := false;
			end if;
			datapesq :=  add_months(datapesq, 1);
		end loop;

		datapesq := to_date('01/01/2021', 'dd/mm/yyyy');
		nchegou2014 := true;

		while nchegou2014
		loop
			for v_dados_ana in
			(
				select 	r.codmagrel, r.codorgjulg, b.CodDoc, case when NVL(tp.codnat, 0) = 2 then 1 else 0 end as procsCriminais,
					case when NVL(tp.codnat, 0) <> 2 or tp.codnat is null then 1 else 0 end as ProcsNaoCriminais,
					case when NVL(p.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end as ProcsAntigos,
					case when NVL(p.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end as ProcsNaoAntigos,
					decode((select	max(dthrmov)
								from	Movimento m1
								where 	m1.coddoc = p.coddoc and m1.dthrmov < datapesq
								and		((m1.codfase in (3,20) and ((m1.codcompl1 = 25) OR (m1.codcompl1 = 1013 and m1.codcompl2 = 1015))) or
											(m1.codfase in (11009,21,30) and m1.codcompl1 = 11025) or
											(m1.codfase = 50002 and m1.codcompl1 in (306, 12065)) or
											(m1.codfase = 50040) or
											(m1.CodFase = 29 and m1.codcompl1 = 1013) OR
											(m1.CodFase = 861 and ((m1.codcompl1 in (690,50068)) OR
																(m1.codcompl1 in (359,360,361,362,363,445,50069) and m1.codcompl2 in (413,414,421))
																))
										)
								and		not exists (select	1
													from	Movimento m2
													where	m2.coddoc = m1.coddoc
													and		m2.dthrmov > m1.dthrmov
													and		m2.dthrmov < datapesq
													and 	((m2.codfase in (24, 193, 893)) or
																(m2.codfase = 50002 and m2.codcompl1 = 12066)
															)
													)
								), null,0,1) as Susp
				from	DGW_DISTRIBUI_BAIXA b, processosituacaoatual psa, processo p, competencia c, tipoprocesso tp, movimentorelator r
				where	distrib is not null
				and	distrib < (datapesq  - 1/86400)
				and	(b.baixa >= datapesq or b.baixa is null)
				and	b.coddoc = psa.coddoc
				and	psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)
				and	psa.coddoc = p.coddoc
				and	p.codcompt = c.codcompt
				and	c.indconsrec = 'N'
				and	p.codcompt <> 9
				and	p.codtipproc = tp.codtipproc
				and	p.idproccorp is not null
				and	tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95, 310)
				and	r.coddoc = p.coddoc
				and	r.dthrmov = (select max(dthrmov) from movdistribuicao where codfase in (26,36) and coddoc = p.coddoc and dthrmov < datapesq)
			)
			loop
				insert into EstatCNBaixaPend_Ana (tipo, dataini, CodDoc, procsCriminais, ProcsNaoCriminais, codmag, codorgjulg, ProcsAntigos, ProcsNaoAntigos, sus2)
				values ('PENDMAG', datapesq, v_dados_ana.CodDoc, v_dados_ana.procsCriminais, v_dados_ana.ProcsNaoCriminais, v_dados_ana.codmagrel, v_dados_ana.codorgjulg, v_dados_ana.ProcsAntigos, v_dados_ana.ProcsNaoAntigos, v_dados_ana.Susp);

				commit;
			end loop;

			if datapesq = trunc(to_date('01/01/2022', 'dd/mm/yyyy'),'mm') then
				nchegou2014 := false;
			end if;
			datapesq :=  add_months(datapesq, 1);
		end loop;
	END IF;

*/

	--------------------------------------------------

	--------- ALTERAÃÃO DA REQ2020.0174484 -----------

	--------------------------------------------------

/*	delete GRP_BAIXAPEND;

	commit;



	-- Erro de ORA-01843: not a valid month

	-- datapesqIni := '01/10/2020';

	datapesqIni := to_date('01/01/2020', 'dd/mm/yyyy');

	datapesqFim := trunc(sysdate,'mm')-(1/(24*60));



	-- BAIXADOS --

	insert	into GRP_BAIXAPEND

		(tipo, dataini, CodOrgJulg, qtdeProcesso)

	select	'BAIX-OJ', trunc(b.baixa,'mm'), psa.CodUltOrgJulg, count(1)

	from	DGW_DISTRIBUI_BAIXA b

		, processosituacaoatual psa

		, processo p

		, competencia c

		, tipoprocesso tp

	where	b.distrib is not null

	and	b.distrib < datapesqFim

	and	b.baixa between datapesqini and datapesqFim

	and	b.coddoc = psa.coddoc

	and	psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)

	and	psa.coddoc = p.coddoc

	and	p.codcompt = c.codcompt

	--and	c.indconsrec = 'N'

	and	p.codtipproc = tp.codtipproc

	and	p.idproccorp is not null

	and	p.codcompt <> 9

	and	tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95)

	group	by trunc(b.baixa,'mm'), psa.CodUltOrgJulg;



	commit;



	datapesq := add_months(datapesqini,1);



	-- PENDENTES --

	while (datapesq-(1/(24*60))) <= datapesqFim loop



		insert	into GRP_BAIXAPEND

			(tipo, dataini, CodOrgJulg, qtdeProcesso)

		select	'PEND-OJ', add_months(datapesq,-1), psa.CodUltOrgJulg, count(1)

		from	DGW_DISTRIBUI_BAIXA b

			, processosituacaoatual psa

			, processo p

			, competencia c

			, tipoprocesso tp

		where	distrib is not null

		and	distrib < (datapesq-(1/(24*60)))

		and	(b.baixa >= datapesq or b.baixa is null)

		and	b.coddoc = psa.coddoc

		and	psa.CodUltLocal not in (419,420,421,422,423,424,425,426,427,428,429,430,583,584,1713,1714,1715,1771,2085,5765)

		and	psa.coddoc = p.coddoc

		and	p.codcompt = c.codcompt

		--and	c.indconsrec = 'N'

		and	p.codcompt <> 9

		and	p.codtipproc = tp.codtipproc

		and	p.idproccorp is not null

		and	tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95)

		group	by datapesq, psa.CodUltOrgJulg;



		commit;



		datapesq :=  add_months(datapesq, 1);

	end loop;*/

END POPULADGW_DISTRIBUI_BAIXA;