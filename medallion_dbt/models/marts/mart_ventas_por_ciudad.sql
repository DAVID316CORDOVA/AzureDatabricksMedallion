select
    ciudad,
    producto,
    count(*) as total_pedidos,
    sum(cantidad) as unidades_vendidas,
    sum(monto_total) as ingresos_totales,
    avg(monto_total) as ticket_promedio
from {{ ref('fct_pedidos') }}
group by ciudad, producto
order by ingresos_totales desc
