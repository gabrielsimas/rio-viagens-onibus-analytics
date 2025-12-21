CREATE TABLE "gcs_bronze"."mvp-rio-transportes-bronze-f7c4".viagens_onibus_icebeger AS
SELECT
    CAST("data" AS DATE) as "data",
    CAST(consorcio AS VARCHAR) as consorcio,
    CAST(tipo_dia AS VARCHAR) as tipo_dia,
    CAST(id_empresa AS VARCHAR) as id_empresa,
    CAST(id_veiculo AS VARCHAR) as id_veiculo,
    CAST(id_viagem AS VARCHAR) as id_viagem,
    CAST(servico AS VARCHAR) as servico,
    CAST(shape_id AS VARCHAR) as shape_id,
    CAST(sentido AS VARCHAR) as sentido,
    CAST(datetime_partida AS TIMESTAMP) as datetime_partida,
    CAST(datetime_chegada AS TIMESTAMP) as datetime_chegada,
    CAST(tempo_viagem AS INT) as tempo_viagem,
    CAST(distancia_planejada AS DOUBLE) as distancia_planejada,
    CAST(perc_conformidade_shape AS DOUBLE) as perc_conformidade_shape,
    CAST(perc_conformidade_registros AS DOUBLE) as perc_conformidade_registros,
    CAST(versao_modelo AS VARCHAR) as versao_modelo
FROM TABLE(gcs_bronze."mvp-rio-transportes-bronze-f7c4"."viagens_onibus" (type => 'parquet'))