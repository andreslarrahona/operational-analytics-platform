{{ config(severity = 'warn') }}

-- Detect consecutive duplicate states in the order workflow timeline.

with state_sequence as (

    select 
        id_orden,
        id_estado,
        fecha_cambio,
        lag(id_estado) over (
            partition by id_orden 
            order by fecha_cambio asc, id_cambio_estado asc
        ) as previous_state

    from {{ ref('stg_cambios_estados_ordenes') }}

)

select 
    id_orden,
    id_estado as duplicated_state,
    fecha_cambio

from state_history
where id_estado = previous_state