with source as (
    -- Reference the table using dbt's source() function
    select * from {{ source('centec_raw', 'ordenes') }}
),
deduplicated as (
    select 
        *,
        row_number() over (
            partition by id 
            order by _ingested_at desc
        ) as rn
    from source
),
renamed_and_casted as (
    select
        -- PKs and FKs
        id as id_orden,
        id_instrumento_cliente as id_instrumento,
        remito_in as id_nota_ingreso,
        remito_out as id_remito,

        -- Dimensions / Texts
        -- Translation of Magic Numbers to business language
        case prioridad::int
            when 1 then 'Alta'
            when 2 then 'Urgente'
            else 'Normal'
        end as prioridad,
        
        -- Standardization to native Postgres Boolean
        case 
            when lower(trim(calibracion_in_situ)) = 'si' then true
            when lower(trim(calibracion_in_situ)) = 'no' then false
            else false
        end as es_calibracion_in_situ,

        -- Explicit date casting (use ::timestamp if they have time, or ::date if only day)
        created_at::timestamp as fecha_creacion,
        fecha_ingreso::timestamp as fecha_ingreso,
        fecha_calibracion::timestamp as fecha_calibracion,
        fecha_aprobado::timestamp as fecha_aprobado,
        fecha_certificado::timestamp as fecha_certificado,
        fecha_entrega::timestamp as fecha_entrega,
        fechapactada::date as fecha_pactada
        
    from deduplicated where rn = 1
)

select * from renamed_and_casted