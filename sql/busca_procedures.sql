SELECT text
FROM dba_source
WHERE owner = 'EJUD'
  AND type = 'PROCEDURE'
  AND name = 'POPULADGW_DISTRIBUI_BAIXA'
ORDER BY line;

SELECT text
FROM dba_source
WHERE owner = 'EJUD'
  AND type = 'PROCEDURE'
  AND name = 'ESTATJULGADOS_CONSULT'
ORDER BY line;

SELECT text
FROM dba_source
WHERE owner = 'EJUD'
  AND type = 'PROCEDURE'
  AND name = UPPER('PopularDgwDistriBaixaPend')
ORDER BY line;