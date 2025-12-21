{{ config(
    materialized='table',
    datalake_format='iceberg',
    root_path='nessie_catalog.silver',
    partition_by=['data_particao']
) }}

WITH source_viagens AS (
    SELECT
        -- IDs
        CAST(id_viagem AS VARCHAR) AS id_viagem,
        CAST(id_veiculo AS VARCHAR) AS id_veiculo,
        CAST(id_empresa AS VARCHAR) AS id_empresa,
        CAST(shape_id AS VARCHAR) AS shape_id,

        -- Categorias
        consorcio,
        servico AS linha,
        sentido,

        -- Temporal
        CAST(data AS DATE) AS data_particao,
        CAST(datetime_partida AS TIMESTAMP) AS data_hora_partida,
        CAST(datetime_chegada AS TIMESTAMP) AS data_hora_chegada,

        -- Métricas Originais
        CAST(tempo_viagem AS INTEGER) AS tempo_viagem_informado_min,
        distancia_planejada,
        perc_conformidade_shape,
        perc_conformidade_registros

    FROM {{ source('bronze', 'viagens_onibus') }}
    WHERE datetime_partida IS NOT NULL
      AND id_viagem IS NOT NULL
)

SELECT
    v.id_viagem,
    v.id_veiculo,
    v.consorcio,
    v.linha,
    v.sentido,

    -- Dados Temporais Enriquecidos
    v.data_particao,
    v.data_hora_partida,
    v.data_hora_chegada,

    -- Correção 1: Usando a função direta DAYOFWEEK()
    -- Retorna 1 (Domingo) a 7 (Sábado)
    DAYOFWEEK(v.data_hora_partida) AS dia_semana_num,

    CASE
        WHEN DAYOFWEEK(v.data_hora_partida) IN (1, 7) THEN true
        ELSE false
    END AS is_final_de_semana,

    -- EXTRACT funciona bem para HOUR
    EXTRACT(HOUR FROM v.data_hora_partida) AS hora_inicio_viagem,

    -- Métricas de Duração
    v.tempo_viagem_informado_min,

    -- Correção 2: Usando TIMESTAMPDIFF para diferença em minutos
    -- Sintaxe: TIMESTAMPDIFF(UNIDADE, INICIO, FIM)
    TIMESTAMPDIFF(MINUTE, v.data_hora_partida, v.data_hora_chegada) AS tempo_viagem_calculado_min,

    -- Indicadores de Qualidade
    v.distancia_planejada,
    v.perc_conformidade_shape,

    -- Metadados
    CURRENT_TIMESTAMP AS data_processamento

FROM source_viagens v