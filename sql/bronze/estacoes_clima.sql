CREATE TABLE "gcs_bronze"."mvp-rio-transportes-bronze-f7c4".estacoes_clima_iceberg AS
SELECT
    CAST(id_estacao AS VARCHAR) as id_estacao,
    CAST(estacao AS VARCHAR) as estacao,
    CAST(latitude AS DOUBLE) as latitude,
    CAST(longitude AS DOUBLE) as longitude,
    CAST(cota AS DOUBLE) as cota,
    CAST(x AS DOUBLE) as x,
    CAST(y AS DOUBLE) as y,
    CAST(endereco AS VARCHAR) as endereco,
    CAST(situacao AS VARCHAR) as situacao,
    CAST(data_inicio_operacao AS TIMESTAMP) as data_inicio_operacao,
    CAST(data_fim_operacao AS TIMESTAMP) as data_fim_operacao,
    CAST(data_atualizacao AS TIMESTAMP) as data_atualizacao
FROM TABLE(gcs_bronze."mvp-rio-transportes-bronze-f7c4"."estacoes_clima" (type => 'parquet'));