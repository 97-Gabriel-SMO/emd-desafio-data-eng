
{{ config(
  materialized='view',
  unique_key='id'
) }}

SELECT DISTINCT ON (codigo)
       codigo,
       latitude,
       longitude,
       velocidade
FROM  public.brt_raw
ORDER BY codigo, dataHora DESC