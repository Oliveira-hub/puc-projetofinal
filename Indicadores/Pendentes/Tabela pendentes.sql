DROP TABLE MPM_PENDENTES_HML

CREATE TABLE MPM_PENDENTES_HML (
	NUM_PROCESSO				VARCHAR2(100),
	COD_DOC						NUMBER,	
	COD_CLASSE					NUMBER,
	ORGAO_JULGADOR				VARCHAR2(100),
	RELATOR						VARCHAR2(100),
	DATA_ULT_MOV				DATE,
	COD_ULT_MOV					VARCHAR2(100),
	DESCRICAO_ULT_MOV			VARCHAR2(100),
	LOCAL_FISICO_ATUAL			VARCHAR2(300),
	LOCAL_VIRTUAL_ATUAL			VARCHAR2(100),
	ANO							NUMBER,
	MES							NUMBER,
	COD_MAG						NUMBER,
	COD_ORG_JULG				NUMBER
	)
	
ALTER TABLE MPM_PENDENTES_HML ADD IND_COMPOSICAO VARCHAR2(10);	