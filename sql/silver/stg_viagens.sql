CREATE TABLE "gcs_silver".stg_viagens AS
WITH clima_agregado AS (
    SELECT
        data_particao,
        EXTRACT(HOUR FROM horario) as hora_medicao,
        AVG(acumulado_chuva_1_h) as avg_chuva_1h
    FROM mvp_rio_transportes.bronze.clima_pluviometria
    GROUP BY data_particao, EXTRACT(HOUR FROM horario)
)
SELECT
    v."data" as data_viagem,
    v.id_viagem,
    v.servico as linha,
    v.consorcio,
    v.tipo_dia,
    v.distancia_planejada,
    v.tempo_viagem,
    EXTRACT(HOUR FROM v.datetime_partida) as hora_partida,
    CASE WHEN EXTRACT(DOW FROM v."data") IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
    COALESCE(c.avg_chuva_1h, 0) as chuva_no_horario,
    CASE
        WHEN c.avg_chuva_1h BETWEEN 0.1 AND 5 THEN 'Fraca'
        WHEN c.avg_chuva_1h BETWEEN 5 AND 25 THEN 'Moderada'
        WHEN c.avg_chuva_1h BETWEEN 25.1 AND 50 THEN 'Forte'
        WHEN c.avg_chuva_1h > 50 THEN 'Muito Forte'
        ELSE 'Sem Chuva'
    END as condicao_clima
FROM mvp_rio_transportes.bronze.viagens_onibus v
LEFT JOIN clima_agregado c
    ON v."data" = c.data_particao
    AND EXTRACT(HOUR FROM v.datetime_partida) = c.hora_medicao
LIMIT 10;

CREATE TABLE "gcs_silver".stg_viagens AS
SELECT
    v."data" as data_viagem,
    v.id_viagem,
    v.servico,
    v.consorcio,
    v.tipo_dia,
    -- Campos para responder Perguntas 1 e 2 (Picos e Sazonalidade)
    EXTRACT(HOUR FROM v.datetime_partida) as hora_partida,
    CASE
        WHEN EXTRACT(DOW FROM v."data") IN (0, 6) THEN TRUE
        ELSE FALSE
    END as is_weekend,
    -- Campos para responder Perguntas 3, 4 e 5 (Duração e Consistência)
    v.tempo_viagem,
    v.distancia_planejada,
    -- Campos para responder Perguntas 5 e 6 (Impacto da Chuva)
    COALESCE(c.chuva_1h_no_inicio, 0) as chuva_no_horario,
    COALESCE(c.tipo_chuva, 'Sem Chuva') as condicao_clima
FROM mvp_rio_transportes.bronze.viagens_onibus v
LEFT JOIN mvp_rio_transportes.silver.viagens_com_clima c
    ON v."data" = c.data_particao
    AND EXTRACT(HOUR FROM v.datetime_partida) = EXTRACT(HOUR FROM c.datetime_partida);

-- -- View no Space Silver
CREATE OR REPLACE VIEW "mvp_rio_transportes"."silver"."viagens_processadas" AS
SELECT * FROM "gcs_silver".stg_viagens;