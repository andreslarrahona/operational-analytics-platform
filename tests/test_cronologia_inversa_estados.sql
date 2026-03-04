{{ config(severity = 'warn') }}
-- If it returns rows, the database has impossible dates (e.g.: left the lab before entering).
select 
    id_orden, 
    ts_ingresado, 
    ts_calibrado, 
    ts_retiro
from {{ ref('fct_ordenes') }}
where ts_calibrado < ts_ingresado
   or ts_retiro < ts_ingresado