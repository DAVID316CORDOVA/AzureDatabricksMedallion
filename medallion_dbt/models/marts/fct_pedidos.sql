select
    p.pedido_id,
    p.cliente_id,
    c.ciudad,
    p.producto,
    p.cantidad,
    p.precio_unitario,
    p.cantidad * p.precio_unitario as monto_total,
    p.fecha_pedido,
    p.estado
from {{ ref('stg_pedidos') }} p
inner join {{ ref('dim_clientes') }} c
    on p.cliente_id = c.cliente_id
where p.flag_cantidad_invalida = false
  and p.flag_precio_invalido = false
  and p.flag_fecha_futura = false
