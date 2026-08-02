select
    cliente_id,
    nombre,
    email,
    ciudad,
    fecha_registro,
    datediff(current_date(), fecha_registro) as dias_como_cliente
from {{ ref('stg_clientes') }}
