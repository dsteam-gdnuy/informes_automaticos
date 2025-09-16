with cl_current as (
select distinct '3-Clientes Activos en el mes '||monthname(date_trunc('month',dateadd('month',-1,current_date()))) detalle , 
count(distinct case when date_trunc('month',tiem_dia_id)=dateadd('year',-1,date_trunc('month',dateadd('month',-1,current_date())))then soci_soci_id end) as "2023",
count(distinct case when date_trunc('month',tiem_dia_id)=date_trunc('month',dateadd('month',-1,current_date())) then soci_soci_id end) as "2024"

from mstrdb.dwh.ft_fdln_movimientos where fdln_movt_tipo like '%RP%'
and tiem_dia_id>= dateadd('month',-13,date_trunc('year',current_date()))
and geog_locl_id in (Select distinct geog_locl_id from mstrdb.dwh.lu_geog_local where geog_unng_id=2)
group by 1
)

,cl_acum as (
select distinct '2-Clientes Activos en el año' detalle , 


--dateadd('year',-1,date_trunc('year',dateadd('month',-1,current_date()))),  dateadd('year',-1,date_trunc('month',dateadd('month',-1,current_date()))),
count(distinct case when tiem_dia_id between 
dateadd('year',-1,date_trunc('year',dateadd('month',-1,current_date())))
and  dateadd('year',-1,date_trunc('month',dateadd('month',-1,current_date())))
then soci_soci_id end) as  "2023",

--date_trunc('year',dateadd('month',-1,current_date())),  date_trunc('month',dateadd('month',-1,current_date())),
count(distinct case when tiem_dia_id between 
date_trunc('year',dateadd('month',-1,current_date()))
and  date_trunc('month',dateadd('month',-1,current_date()))
then soci_soci_id end) as  "2024"




from mstrdb.dwh.ft_fdln_movimientos where fdln_movt_tipo like '%RP%'
and tiem_dia_id>= dateadd('year',-2,date_trunc('year',current_date()))
and geog_locl_id in (Select distinct geog_locl_id from mstrdb.dwh.lu_geog_local where geog_unng_id=2)
group by 1
) 
, totales as (select '1-Clientes Totales' detalle, null "2023", count(distinct soci_soci_id) "2024" from mstrdb.dwh.lu_clie_cliente)

, prop as ( Select '4-Proporcion sobre total de Clientes' detalle,null "2023","2024"/(select count(distinct soci_soci_id) from mstrdb.dwh.lu_clie_cliente) "2024" from cl_current)

,generados as (

    select 
        '5-Puntos Generados' detalle,
        sum(case when date_trunc('month',tiem_dia_id)=dateadd('year',-1,date_trunc('month',dateadd('month',-1,current_date()))) then fdln_movt_ptos_comunes end ) "2023",
        sum(case when date_trunc('month',tiem_dia_id)=date_trunc('month',dateadd('month',-1,current_date())) then fdln_movt_ptos_comunes end) "2024"
    from 
        mstrdb.dwh.ft_fdln_movimientos 
    where 
        fdln_movt_tipo like '%RP%'
        and tiem_dia_id>= dateadd('month',-13,date_trunc('year',current_date()))
        and geog_locl_id in (Select distinct geog_locl_id from mstrdb.dwh.lu_geog_local where geog_unng_id=2)
) 

,canjeados as (select '6-Puntos Canjeados' detalle,
sum(case when date_trunc('month',tiem_dia_id)=dateadd('year',-1,date_trunc('month',dateadd('month',-1,current_date())))  then  fdln_movt_ptos_comunes end ) "2023",
 sum(case when date_trunc('month',tiem_dia_id)=date_trunc('month',dateadd('month',-1,current_date())) then fdln_movt_ptos_comunes end) "2024"
 from mstrdb.dwh.ft_fdln_movimientos where fdln_movt_tipo like '%CP%'
and tiem_dia_id>= dateadd('month',-13,date_trunc('year',current_date()))
and geog_locl_id in (Select distinct geog_locl_id from mstrdb.dwh.lu_geog_local where geog_unng_id=2)
) 
,nps as (select '7-NPS' detalle,
                    (select round((count(case when NPS='Promotores' then 1 end)-count(case when NPS='Detractores' then 1 end))/count(*) ,2)*100 
                    from sandbox_plus.dwh.dossier_nps_general
                    where date_trunc('month',dia)= dateadd('month',-13,date_trunc('month',current_date())) 
                    having count(*)>0) "2023",

                    (select round((count(case when NPS='Promotores' then 1 end)-count(case when NPS='Detractores' then 1 end))/count(*) ,2)*100
                    from sandbox_plus.dwh.dossier_nps_general
                    where date_trunc('month',dia)= dateadd('month',-1,date_trunc('month',current_date())) having count(*)>0 ) "2024" 

)
select * from (select * from cl_current
union
select * from cl_acum
union 
select * from totales
union 
select * from prop
union
select * from generados
union
select * from canjeados
union 
select * from nps) order by detalle;