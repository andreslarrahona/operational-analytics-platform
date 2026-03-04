{{ config(severity = 'warn') }}
-- A certificate cannot be issued or approved if the equipment does not have a calibration date.
select 
    id_orden,
    ts_calibrado,
    ts_emitido,
    ts_aprobado
from {{ ref('fct_ordenes') }}
where (ts_emitido is not null or ts_aprobado is not null)
  and ts_calibrado is null