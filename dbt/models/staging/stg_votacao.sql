
with raw as(
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_AC
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_AL
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_AP
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_AM
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_BA
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_CE
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_DF
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_ES
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_GO
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_MA
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_MT
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_MS
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_MG
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_PA
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_PB
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_PR
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_PE
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_PI
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_RJ
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_RN
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_RS
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_RO
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_RR
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_SC
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_SE
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_TO
UNION ALL
SELECT 
    "NR_TURNO" as nr_turno,
    "SG_UF" as sg_uf,
    "NM_MUNICIPIO" as nm_municipio,
    "DS_CARGO" AS ds_cargo,
    "NR_CANDIDATO" AS nr_votavel,
    "NM_CANDIDATO" AS nm_votavel,
    CAST("QT_VOTOS_NOMINAIS" AS INTEGER) AS qt_votos
FROM PUBLIC.VOTACAO_CANDIDATO_MUNZONA_2022_BRASIL
)
select * 
from raw where ds_cargo in ('Presidente', 'Governador')


