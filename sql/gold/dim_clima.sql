-- TEMPO DE RESPOSTA: 5s
CREATE TABLE "gcs_gold".dim_clima AS
SELECT DISTINCT
    condicao_clima as clima_id,
    condicao_clima as desc_intensidade
FROM mvp_rio_transportes.silver.viagens_processadas;

CREATE OR REPLACE VIEW mvp_rio_transportes.gold.dim_clima AS SELECT * FROM "gcs_gold".dim_clima;