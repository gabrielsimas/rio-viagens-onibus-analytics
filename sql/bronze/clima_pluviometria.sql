CREATE TABLE "gcs_bronze"."mvp-rio-transportes-bronze-f7c4".clima_pluviometria_iceberg AS AS
SELECT
    CAST(primary_key AS VARCHAR) as primary_key,
    CAST(id_estacao AS VARCHAR) as id_estacao,
    CAST(acumulado_chuva_15_min AS DOUBLE) as acumulado_chuva_15_min,
    CAST(acumulado_chuva_1_h AS DOUBLE) as acumulado_chuva_1_h,
    CAST(acumulado_chuva_4_h AS DOUBLE) as acumulado_chuva_4_h,
    CAST(acumulado_chuva_24_h AS DOUBLE) as acumulado_chuva_24_h,
    CAST(acumulado_chuva_96_h AS DOUBLE) as acumulado_chuva_96_h,
    CAST(horario AS TIME) as horario,
    CAST(data_particao AS DATE) as data_particao
FROM TABLE(gcs_bronze."mvp-rio-transportes-bronze-f7c4"."clima_pluviometria" (type => 'parquet'));