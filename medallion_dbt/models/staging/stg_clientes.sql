with source as (
    select * from {{ source('bronze_layer','clientes')}}
),
deduplicado as (
    select DISTINCT * from source
),
limpio as (
    select
        id as cliente_id,
        trim(nombre) as nombre,
        lower(trim(email)) as email,
        trim(ciudad) as ciudad,
        try_cast(fecha_registro as date) as fecha_registro,
        _batch_id,
        _inserted_at,
        row_number() over (
            partition by lower(trim(email))
            order by _inserted_at desc
        ) as rn_email
    from deduplicado
    where email is not null
      and email like '%@%'
)
select
    cliente_id,
    nombre,
    email,
    ciudad,
    fecha_registro,
    _batch_id,
    _inserted_at
from limpio
where rn_email = 1  -- se queda con el registro más reciente por email (dedup de negocio)
  and fecha_registro between date('2020-01-01') and current_date()