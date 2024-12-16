PROCEDURE EstatJulgados_CONSULT

(

/*

	MPSMTA - 02/10/2020 - REQ2020.0063528

	Pode ser passado como parâmetro qualquer dia e hora do mês desejado, pois a rotina só leva em conta o mês da data informada

*/

	p_DtMes	DATE := sysdate

)

is

	v_DtIni DATE;

	v_DtFim	DATE;

	v_i		number;

	v_oj	number;

	v_rel	number;

	v_dist	date;



begin



	v_DtIni := trunc(nvl(p_DtMes,sysdate),'mm');

	v_DtFim := add_months(v_DtIni,1)-1*(1/24/60/60);--um segundo



	delete EstatJulgados;



	insert into EstatJulgados  (coddoc, dthrmov)

	select 	coddoc, min(dthrmov) as dthrmov

	from	Movimento m

	where	dthrmov > v_DtIni and dthrmov <= v_DtFim

	and 	CodFase = 3

	and	dthrmov = (select min(dthrmov) from movimento where coddoc = m.coddoc

				and CodFase in (3,20,24,193) )

	group	by coddoc;



	insert into EstatJulgados   (coddoc, dthrmov)

	select 	coddoc, min(dthrmov) as dthrmov

	from	Movimento m

	where	dthrmov > v_DtIni and dthrmov <= v_DtFim

	and 	CodFase = 20

	and	dthrmov = (select min(dthrmov) from movimento where coddoc = m.coddoc

				and CodFase in (3,20,24,193) )

	group	by coddoc;



	insert into EstatJulgados   (coddoc, dthrmov)

	select 	coddoc, min(dthrmov) as dthrmov

	from	Movimento m

	where	dthrmov > v_DtIni and dthrmov <= v_DtFim

	and 	CodFase = 24

	and	dthrmov = (select min(dthrmov) from movimento where coddoc = m.coddoc

				and CodFase in (3,20,24,193) )

	group	by coddoc;



	insert into EstatJulgados   (coddoc, dthrmov)

	select 	coddoc, min(dthrmov) as dthrmov

	from	Movimento m

	where	dthrmov > v_DtIni and dthrmov <= v_DtFim

	and 	CodFase = 193

	and	dthrmov = (select min(dthrmov) from movimento where coddoc = m.coddoc

				and CodFase in (3,20,24,193) )

	group	by coddoc;



	v_i := 0;



	for v_proc in (	select r.coddoc, r.dthrmov,

				p.codtipproc, p.dtautua, processoequivalente(null, 'N', p.NumAno, p.CodTipProc, p.NumProc) as numant,

				p.indeletr, p.codcompt,

				m.codcompl1, m.codcompl2, m.codcompl3

			from EstatJulgados r, processo p, movimento m

			where exists (select 1 from processo where coddoc = r.coddoc)

			and r.dtautua is null

			and r.coddoc = p.coddoc

			and r.coddoc = m.coddoc

			and r.dthrmov = m.dthrmov)

	loop

		v_dist := null;

		v_oj := null;

		v_rel := null;



		select	max(dthrmov)

		into	v_dist

		from	movdistribuicao d

		where	coddoc = v_proc.coddoc

		and	dthrmov <= v_proc.dthrmov

		and	codfase in (26,36) and (codcompl1 != 50065 or codfase != 36)

		and	not exists (

				select 1 from movcancedistr

				where coddoc = d.coddoc and dthrmovdistrcance = d.dthrmov);



		if v_dist is not null then

			select	mr.codorgjulg, mr.codmagrel

			into	v_oj, v_rel

			from	movimentorelator mr

			where	mr.coddoc = v_proc.coddoc

			and	mr.dthrmov = v_dist;

		end if;



		update EstatJulgados

		set codmagrel = v_rel, codorgjulg = v_oj, dthrdistr = v_dist,

			codtipproc = v_proc.codtipproc, numant = v_proc.numant, dtautua = v_proc.dtautua,

			indeletr = v_proc.indeletr, codcompt = v_proc.codcompt,

			codcompl1 = v_proc.codcompl1, codcompl2 = v_proc.codcompl2, codcompl3 = v_proc.codcompl3

		where coddoc = v_proc.coddoc and dthrmov = v_proc.dthrmov;



		v_i := v_i + 1;



		if v_i > 99 then

			commit;

			v_i := 0;

		end if;



	end loop;



	update EstatJulgados set codorgjulg = null, codmagrel = null where dthrdistr is null;

	update EstatJulgados set dthrdistr = null, codmagrel = null where codorgjulg is null;

	update EstatJulgados set dthrdistr = null, codorgjulg = null where codmagrel is null;

	delete EstatJulgados where codorgjulg in (select codorgjulg from orgaojulgador where codcompt in

						(select CodCompt from Competencia where IndConsRec = 'S'));



	commit;



end;