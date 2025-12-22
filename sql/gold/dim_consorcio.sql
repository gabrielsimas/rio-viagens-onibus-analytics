-- TEMPO DE CRIAÇÃO: 5s
CREATE TABLE "gcs_gold".dim_consorcio AS
SELECT DISTINCT
    consorcio as consorcio_id,
    consorcio as nome_consorcio
FROM mvp_rio_transportes.silver.viagens_processadas;

 CREATE OR REPLACE VIEW mvp_rio_transportes.gold.dim_consorcio AS SELECT * FROM "gcs_gold".dim_consorcio;