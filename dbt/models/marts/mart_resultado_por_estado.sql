with fct as (
    select *
    from {{ ref('fct_eleicao') }}
    where ds_cargo in ('Presidente', 'Governador')
)

select
    sg_uf,
    ds_cargo,
    nr_votavel,
    nm_votavel,
    sum(qt_votos) as total_votos
from fct
group by 1, 2, 3, 4
order by sg_uf, total_votos desc
