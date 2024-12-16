PROCEDURE PopularDgwDistriBaixaPend

IS

	datapesq		DATE;
	nchegou2014		BOOLEAN;
	qtdeProcesso		NUMBER;
	qtdeProcessoNCrim	NUMBER;
	antigo			NUMBER;
	nantigo			NUMBER;

BEGIN

	--execute immediate ('truncate table EstatCNBaixaPend_ana');

	--execute immediate ('truncate table EstatCNBaixaPend');



	datapesq := to_date('01/01/2008', 'dd/mm/yyyy');

	nchegou2014 := true;

	while nchegou2014
	loop
		for v in
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
							and	not exists (select	1
												from	Movimento m2
												where	m2.coddoc = m1.coddoc
												and		m2.dthrmov > m1.dthrmov
												and		m2.dthrmov < datapesq
												and 	((m2.codfase in (24, 193, 893)) or
															(m2.codfase = 50002 and m2.codcompl1 = 12066)
														)
												)
							), null,0,1) as Susp,
				p.CodTipProc, c.CodNat, p.NumAnoAnt,
				(
					select max(m.dthrmov)
					from movimento m
					where m.coddoc = b.coddoc
					and m.dthrmov < datapesq
				) as DtHrUltMov,
				(
					select mf.codlocal
					from MovimentoFisico mf
					where mf.coddoc = b.coddoc
					and mf.dthrmov = (select max(mf2.dthrmov) from MovimentoFisico mf2
								where mf2.coddoc = b.coddoc and mf2.dthrmov < datapesq )
				) as CodUltLocal,
				(

					SELECT	Max(lvd.CodLocalVirt)

					FROM	LocalVirtualDocumento lvd

					WHERE	lvd.coddoc = b.coddoc

					AND	lvd.DtHrEntr =

						(

							SELECT	max(lvd2.DtHrEntr)

							FROM	LocalVirtualDocumento lvd2

							WHERE	lvd2.coddoc = b.coddoc

							AND	lvd2.DtHrEntr < datapesq

						)

				) as CodLocalVirt

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

			and	p.idproccorp is not NULL

			--	A classe 310 não deve aparecer no relatório análitico e sintético de pendentes por magistrado, porém comentamos aqui

			--	para que essa classe inicialmente entre na tabela análitica para que possamos gerar o relatório sintético de pendentes que

			--	não leva em consideração a classe 310. Após o cálculo dos pendentes, iremos remover os processos cuja classe seja 310.

			and	tp.codtipproc not in (5, 54, 30, 73, 40, 41, 42, 93, 94 , 95 /*, 310*/)

			and	r.coddoc = p.coddoc

			and	r.dthrmov = (select max(dthrmov) from movdistribuicao where codfase in (26,36) and coddoc = p.coddoc and dthrmov < datapesq)

		)

		loop

			insert into EstatCNBaixaPend_Ana

			(

				tipo, dataini, CodDoc, procsCriminais, ProcsNaoCriminais, codmag, codorgjulg,

				ProcsAntigos, ProcsNaoAntigos, sus2, CodTipProc, CodNat, NumAnoAnt,

				DtHrUltMov, CodUltLocal, CodLocalVirt

			)

			values

			(

				'PENDMAG', datapesq, v.CodDoc, v.procsCriminais, v.ProcsNaoCriminais, v.codmagrel, v.codorgjulg,

				v.ProcsAntigos, v.ProcsNaoAntigos, v.Susp, v.CodTipProc, v.CodNat, v.NumAnoAnt,

				v.DtHrUltMov, v.CodUltLocal, v.CodLocalVirt

			);



			commit;

		end loop;





		if datapesq = trunc(sysdate,'mm') then

			nchegou2014 := false;

		end if;



		datapesq :=  add_months(datapesq, 1);



	end loop;



	COMMIT;



	-- Geração do relatório Sintético de Pendentes

	datapesq := to_date('01/01/2008', 'dd/mm/yyyy');

	nchegou2014 := true;

	while nchegou2014

	loop

		SELECT	sum(case when NVL(e.codnat, 0) = 2 then 1 else 0 end) qtdeprocsCriminais,

			sum(case when NVL(e.codnat, 0) <> 2 or e.codnat is null then 1 else 0 end) qtdeProcsNaoCriminais,

			sum(case when NVL(e.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsAntigos,

			sum(case when NVL(e.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsNaoAntigos

		INTO	qtdeProcesso, qtdeProcessoNCrim, antigo, nantigo

		FROM	EstatCNBaixaPend_Ana e

		WHERE	Tipo = 'PENDMAG'

		AND	DataIni = datapesq;



		--dbms_output.put_line(to_char((datapesq - 1/86400), 'DD/MM/RRRR HH24:MI:SS') || ',' || qtdeProcesso || ',' || qtdeProcessoNCrim || ',' || antigo || ',' || nantigo);

		insert into EstatCNBaixaPend

		(

			tipo, dataini, qtdeProcesso, qtdeProcessoNCrim, antigo, nantigo

		)

		values

		(

			'PEND', datapesq, qtdeProcesso, qtdeProcessoNCrim, antigo, nantigo

		);



		commit;



		if datapesq = trunc(sysdate,'mm') THEN

			nchegou2014 := false;

		end if;



		datapesq := add_months(datapesq, 1);



	end loop;



	COMMIT;



	-- Removendo os processos cuja classe seja 310, pois os pendentes do magistrado não inclui essa classe

	DELETE	EstatCNBaixaPend_Ana

	WHERE	CodTipProc = 310;



	COMMIT;



	datapesq := to_date('01/01/2008', 'dd/mm/yyyy');

	nchegou2014 := true;



	while nchegou2014

	loop

		for v in

		(

			select	e.codmag, e.codorgjulg, sum(case when NVL(tp.codnat, 0) = 2 then 1 else 0 end) qtdeprocsCriminais,

				sum(case when NVL(tp.codnat, 0) <> 2 or tp.codnat is null then 1 else 0 end) qtdeProcsNaoCriminais,

				sum(case when NVL(e.numanoant, 2000) <= (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsAntigos,

				sum(case when NVL(e.numanoant, 2000) > (to_char(datapesq, 'RRRR')-2) then 1 else 0 end) QtdeProcsNaoAntigos,

				sum(decode(

					(

						select	max(dthrmov)

						from	Movimento m1

						where 	m1.coddoc = e.coddoc

						AND	m1.dthrmov < datapesq

						and	(

								(m1.codfase in (3,20) and ((m1.codcompl1 = 25) OR (m1.codcompl1 = 1013 and m1.codcompl2 = 1015))) or

								(m1.codfase in (11009,21,30) and m1.codcompl1 = 11025) or

								(m1.codfase = 50002 and m1.codcompl1 in (306, 12065)) or

								(m1.codfase = 50040) or

								(m1.CodFase = 29 and m1.codcompl1 = 1013) OR

								(m1.CodFase = 861 and ((m1.codcompl1 in (690,50068)) OR (m1.codcompl1 in (359,360,361,362,363,445,50069) and m1.codcompl2 in (413,414,421))))

							)

						and		not exists

								(

									select	1

									from	Movimento m2

									where	m2.coddoc = m1.coddoc

									and	m2.dthrmov > m1.dthrmov

									and	m2.dthrmov < datapesq

									and 	((m2.codfase in (24, 193, 893)) OR (m2.codfase = 50002 and m2.codcompl1 = 12066))

								)

					), null,0,1)) QtdeSusp

			FROM	EstatCNBaixaPend_Ana e, TipoProcesso tp

			WHERE	e.CodTipProc = tp.CodTipProc

			AND	Tipo = 'PENDMAG'

			AND	DataIni = datapesq

			group	by e.codmag, e.codorgjulg

		)

		loop

			insert into EstatCNBaixaPend

			(

				tipo, dataini, qtdeProcesso, qtdeProcessoNCrim, codmag, codorgjulg, antigo, nantigo, sus2

			)

			values

			(

				'PENDMAG', datapesq, v.qtdeprocsCriminais, v.qtdeProcsNaoCriminais, v.codmag, v.codorgjulg, v.QtdeProcsAntigos, v.QtdeProcsNaoAntigos, v.QtdeSusp

			);



			commit;

		end loop;



		if datapesq = trunc(sysdate,'mm') then
			nchegou2014 := false;
		end if;
		datapesq :=  add_months(datapesq, 1);
	end loop;


END;