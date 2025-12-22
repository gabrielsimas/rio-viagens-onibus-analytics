-- TEMPO DE CRIAÇÃO: 8s
CREATE TABLE "gcs_gold".dim_calendario AS
SELECT DISTINCT
    data_viagem as data_id,
    EXTRACT(DAY FROM data_viagem) as dia,
    EXTRACT(MONTH FROM data_viagem) as mes,
    EXTRACT(YEAR FROM data_viagem) as ano,
    CASE WHEN is_weekend THEN 'Final de Semana' ELSE 'Dia Útil' END as desc_categoria_dia,
    tipo_dia as desc_sazonalidade -- Ex: "Domingo - Verão"
FROM mvp_rio_transportes.silver.viagens_processadas;

CREATE OR REPLACE VIEW mvp_rio_transportes.gold.dim_calendario AS SELECT * FROM "gcs_gold".dim_calendario;