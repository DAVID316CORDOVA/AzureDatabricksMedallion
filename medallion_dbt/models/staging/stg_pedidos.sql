with source as (
    select * from {{ source('bronze_layer','pedidos') }}
),
deduplicado as (
    select DISTINCT * from source
),
tipado as (
    select
        id as pedido_id,
        cliente_id,
        trim(producto) as producto,
        try_cast(cantidad as int) as cantidad,
        try_cast(precio_unitario as decimal(10,2)) as precio_unitario,
        try_cast(fecha_pedido as date) as fecha_pedido,
        estado,
        _batch_id,
        _inserted_at
    from deduplicado
)
select
    pedido_id,
    cliente_id,
    producto,
    cantidad,
    precio_unitario,
    fecha_pedido,
    estado,
    _batch_id,
    _inserted_at,
    -- flags de calidad, en vez de borrar directamente (mejor para practicar/analizar después)
    case when cantidad is null or cantidad <= 0 then true else false end as flag_cantidad_invalida,
    case when precio_unitario is null or precio_unitario <= 0 then true else false end as flag_precio_invalido,
    case when fecha_pedido > current_date() then true else false end as flag_fecha_futura
from tipado
where fecha_pedido is not null  -- descarta fechas que ni siquiera se pudieron castear