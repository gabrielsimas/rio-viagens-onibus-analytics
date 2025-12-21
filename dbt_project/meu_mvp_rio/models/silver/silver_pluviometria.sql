{{ config(
    materialized='table',
    datalake_format='iceberg',
    root_path='nessie_catalog.silver',
    partition_by=['data_particao']
) }}

WITH source_pluviometria AS (
    SELECT
        CAST(primary_key AS VARCHAR) AS id,
        id_estacao,
        -- Substituindo nulos por zero conforme planejado
        COALESCE(CAST(acumulado_chuva_1_h AS DOUBLE), 0.0) AS acumulado_1h,
        horario,
        data_particao
    FROM {{ source('bronze', 'clima_pluviometria') }}
),

source_estacoes AS (
    SELECT
        id_estacao,
        estacao AS nome_estacao
    FROM {{ source('bronze', 'estacoes_clima') }}
)

SELECT
    p.id,
    e.nome_estacao,
    p.acumulado_1h,
    -- Classificação sem gaps e tratando nulos
    CASE
        WHEN p.acumulado_1h <= 5.0 THEN 'Fraca'
        WHEN p.acumulado_1h > 5.0 AND p.acumulado_1h <= 25.0 THEN 'Moderada'
        WHEN p.acumulado_1h > 25.0 AND p.acumulado_1h <= 50.0 THEN 'Forte'
        WHEN p.acumulado_1h > 50.0 THEN 'Muito Forte'
        ELSE 'Não Identificada'
    END AS tipo_chuva,
    CASE WHEN p.acumulado_1h > 0.0 THEN 1 ELSE 0 END AS flag_chuva,
    p.horario,
    p.data_particao
FROM source_pluviometria p
LEFT JOIN source_estacoes e ON p.id_estacao = e.id_estacao