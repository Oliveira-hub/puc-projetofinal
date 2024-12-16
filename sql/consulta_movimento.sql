SELECT
	mov.coddoc,
	p.codtipproc,
	tp.descr AS classe,
	mov.codfase,
	f.descr,
	mov.dthrmov,
	mov.codcompl1,
	c1.descr AS compl1,
	mov.codcompl2,
	c2.descr AS compl2,
	mov.codcompl3,
	c3.descr AS compl3,
	mf.codlocal,
	lf.descr AS localfisico
FROM ejud.movimento@tj01 mov
LEFT JOIN ejud.fase@tj01 f ON mov.codfase = f.codfase 
LEFT JOIN ejud.complemento@tj01 c1 ON mov.codcompl1 = c1.codcompl 
LEFT JOIN ejud.complemento@tj01 c2 ON mov.codcompl1 = c2.codcompl 
LEFT JOIN ejud.complemento@tj01 c3 ON mov.codcompl1 = c3.codcompl 
LEFT JOIN ejud.movimentofisico@tj01 mf ON (mov.coddoc = mf.coddoc AND mov.dthrmov = mf.dthrmov)
LEFT JOIN ejud.t_processo@tj01 p ON (mov.coddoc = p.coddoc)
LEFT JOIN ejud.localfisico@tj01 lf ON (mf.codlocal = lf.codlocal)
LEFT JOIN ejud.tipoprocesso@tj01 tp ON (p.codtipproc = tp.codtipproc)
WHERE mov.coddoc = 27623717
ORDER BY dthrmov desc;

SELECT * FROM ejud.movimentofisico@tj01 WHERE coddoc = 26495966 ORDER BY dthrmov desc;