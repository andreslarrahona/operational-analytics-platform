with cambios_estado as (
    select id_orden, id_estado, fecha_cambio, id_usuario from {{ ref('stg_cambios_estados_ordenes') }}
),
hitos as (
	select 
        ceo.id_orden,
        min(case when ceo.id_estado = 1 then ceo.fecha_cambio end) as ts_ingresado,
        min(case when ceo.id_estado = 2 then ceo.fecha_cambio end) as ts_enproceso,
        min(case when ceo.id_estado = 3 then ceo.fecha_cambio end) as ts_calibrado,
        min(case when ceo.id_estado = 4 then ceo.fecha_cambio end) as ts_notificar_retiro,
        max(case when ceo.id_estado = 4 then ceo.fecha_cambio end) as ts_emitido,
        max(case when ceo.id_estado = 8 then ceo.fecha_cambio end) as ts_aprobado,
        min(case when ceo.id_estado = 5 then ceo.fecha_cambio end) as ts_subido,
        min(case when ceo.id_estado = 5 then ceo.fecha_cambio end) as ts_notificar_certificado,
        min(case when ceo.id_estado = 1 then ceo.id_usuario end) as user_ingreso,
        min(case when ceo.id_estado = 2 then ceo.id_usuario end) as user_enproceso,
        min(case when ceo.id_estado = 3 then ceo.id_usuario end) as user_calibrado,
        max(case when ceo.id_estado = 4 then ceo.id_usuario end) as user_emitido,
        max(case when ceo.id_estado = 8 then ceo.id_usuario end) as user_aprobado,
        min(case when ceo.id_estado = 5 then ceo.id_usuario end) as user_subido
        from cambios_estado ceo
        group by 1
)

select
    h.*,
    EXTRACT(EPOCH FROM (h.ts_enproceso - h.ts_ingresado)) / 86400 AS dias_ingresado,
    EXTRACT(EPOCH FROM (h.ts_calibrado - h.ts_enproceso)) / 86400 AS dias_en_proceso,
    EXTRACT(EPOCH FROM (h.ts_emitido - h.ts_calibrado)) / 86400 AS dias_para_emitir,
    EXTRACT(EPOCH FROM (h.ts_aprobado - h.ts_emitido)) / 86400 AS dias_para_aprobar,
    EXTRACT(EPOCH FROM (h.ts_subido - h.ts_aprobado)) / 86400 AS dias_para_subir,
    extract(epoch from (h.ts_notificar_retiro - h.ts_ingresado))/86400 as dias_para_liberar,
    extract(epoch from (h.ts_notificar_retiro - h.ts_calibrado))/86400 as dias_para_notificar,
    extract(epoch from (h.ts_subido - h.ts_calibrado))/86400 as dias_demora_certificado
from hitos h
