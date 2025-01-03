CREATE TABLE MPM_SUSPENSOS_HML (
	CNJ						VARCHAR2(100),
	cod_doc					NUMBER,
	CLASSE					NUMBER,
	ORGAO_JULGADOR			VARCHAR2(200),
	RELATOR					VARCHAR2(200),
	DATA_ULT_MOV			DATE,
	COD_ULT_MOV				VARCHAR2(100),
	DESCRICAO_ULT_MOV		VARCHAR2(200),
	LOCAL_FISICO_ATUAL		VARCHAR2(200),
	LOCAL_VIRTUAL_ATUAL		VARCHAR2(200),
	ANO						NUMBER,
	MES						NUMBER,
	COD_MAG					NUMBER,
	COD_ORG_JULG			NUMBER
)

SELECT * FROM MPM_SUSPENSOS_HML;

ALTER TABLE MPM_SUSPENSOS_HML 
ADD CODFASE NUMBER;

SELECT * FROM MPM_SUSPENSOS_HML;

ALTER TABLE MPM_SUSPENSOS_HML ADD IND_VALIDO VARCHAR2(10);	
ALTER TABLE MPM_SUSPENSOS_HML ADD IND_EVENTO VARCHAR2(10);	
ALTER TABLE MPM_SUSPENSOS_HML ADD IND_GTDL VARCHAR2(10);	

ALTER TABLE MPM_SUSPENSOS_HML
DROP COLUMN IND_COMPOSICAO;
