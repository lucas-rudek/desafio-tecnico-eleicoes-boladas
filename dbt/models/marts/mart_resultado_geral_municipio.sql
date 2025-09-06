with fct as (
    select *
    from {{ ref('fct_eleicao') }}
)

select
    sg_uf,
    nm_municipio,
    ds_cargo,
    nr_votavel,
    nm_votavel,
    sum(qt_votos) as total_votos
from fct
group by 1,2,3,4,5
order by 1,2,3, total_votos desc
