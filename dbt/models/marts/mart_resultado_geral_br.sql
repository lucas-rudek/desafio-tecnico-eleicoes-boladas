with fct as (
    select *
    from {{ ref('fct_eleicao') }}
    where ds_cargo = 'Presidente'
)

select
    nr_votavel,
    nm_votavel,
    sum(qt_votos) as total_votos
from fct
group by nr_votavel, nm_votavel
order by total_votos desc
