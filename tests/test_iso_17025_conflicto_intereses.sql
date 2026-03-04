{{ config(severity = 'warn') }}
-- If it returns rows, there is a very serious Non-Conformity: lack of task segregation.
select 
    id_orden, 
    user_calibrado, 
    user_aprobado
from {{ ref('fct_ordenes') }}
where es_mismo_usuario = true