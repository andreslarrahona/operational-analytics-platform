with ordenes as (
    select * from {{ ref('stg_ordenes') }}
),

instrumentos as (
    select * from {{ ref('dim_instrumentos') }}
),

tiempos_proceso as (
    select * from {{ ref('int_ordenes_pivoted') }}
),

logistica_salida as (
    select * from {{ ref('int_remitos_paired') }}
),
actuales as (
    select * from {{ ref('int_ordenes_actuales') }}
)

select
    -- 1. Identifiers
    o.id_orden,
    o.id_nota_ingreso,
    i.id_instrumento,
    i.id_cliente,

    -- 2. Order Attributes 
    o.prioridad,
    o.es_calibracion_in_situ,

    -- 3. Milestone Timestamps
    tp.ts_ingresado,
    tp.ts_calibrado,
    tp.ts_emitido,
    tp.ts_aprobado,
    ls.ts_retiro,
    
    -- 4. Metrics
    tp.dias_ingresado,
    tp.dias_en_proceso,
    tp.dias_para_emitir,
    tp.dias_para_aprobar,
    tp.dias_para_subir,
    ls.dias_en_laboratorio,
    ls.dias_demora_cliente,
    tp.dias_para_liberar,
    tp.dias_para_notificar,

    -- 5. Final Business Logic
    tp.user_calibrado,
    tp.user_aprobado,
    (tp.user_calibrado = tp.user_aprobado) as es_mismo_usuario,

    -- 6. Info to Detect Stalled Orders
    act.id_estado_actual,
    act.ts_ultimo_movimiento

from ordenes o
inner join instrumentos i on o.id_instrumento = i.id_instrumento
inner join tiempos_proceso tp on o.id_orden = tp.id_orden
left join logistica_salida ls on o.id_orden = ls.id_orden
left join actuales act on o.id_orden = act.id_orden