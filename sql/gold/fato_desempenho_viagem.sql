-- TEMPO DE RESPOSTA: 1m01s
CREATE TABLE "gcs_gold".fato_desempenho_viagem AS
SELECT
    id_viagem,
    data_viagem as data_id,
    consorcio as consorcio_id,
    condicao_clima as clima_id,
    linha,
    hora_partida,
    tempo_viagem,
    distancia_planejada,
    chuva_no_horario
FROM mvp_rio_transportes.silver.viagens_processadas;

CREATE OR REPLACE VIEW mvp_rio_transportes.gold.fato_viagens AS SELECT * FROM "gcs_gold".fato_desempenho_viagem;