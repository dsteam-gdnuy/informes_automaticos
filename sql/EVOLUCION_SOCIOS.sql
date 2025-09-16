with datos_clientes as (select distinct count(soci_soci_id) socios_tot from mstrdb.dwh.lu_clie_cliente)

,socios as (select ano, --socios,
            SUM(socios) OVER (ORDER BY ano ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS socios_acum
            from (
                 select distinct ano,
                 count(Distinct soci_soci_id) socios


                 from (
                     select distinct soci_soci_id,  min_compra,
                     case when min_compra<=dateadd('year',-2,date_trunc('month',(date_trunc('month',current_date())-1))) then dateadd('year',-2,date_trunc('month',(date_trunc('month',current_date()-1)-1)))
                      when  min_compra<=dateadd('year',-1,date_trunc('month',(date_trunc('month',current_date())-1))) then dateadd('year',-1,date_trunc('month',(date_trunc('month',current_date())-1)))
                     when  min_compra<=date_trunc('month',(date_trunc('month',current_date())-1)) then date_trunc('month',(date_trunc('month',current_date())-1)) end ano
                        from (
                        select distinct soci_soci_id, date_trunc('month', min(tiem_dia_id)) min_compra from mstrdb.dwh.ft_fdln_movimientos a
                        inner join mstrdb.dwh.lu_geog_local l on l.geog_locl_id=a.geog_locl_id and geog_unng_id in (2,3,5)
                        group by all)
            )
        where ano is not null group by all))

,socios_comp as (select ano, --socios,
            SUM(socios) OVER (ORDER BY ano ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS socios_acum_comp
            from (
                 select distinct ano,
                 count(Distinct soci_soci_id) socios


                 from (
                     select distinct soci_soci_id,  min_compra,
                     case when min_compra<=dateadd('year',-2,date_trunc('month',(date_trunc('month',current_date())-1))) then dateadd('year',-2,date_trunc('month',(date_trunc('month',current_date()-1)-1)))
                      when  min_compra<=dateadd('year',-1,date_trunc('month',(date_trunc('month',current_date())-1))) then dateadd('year',-1,date_trunc('month',(date_trunc('month',current_date())-1)))
                     when  min_compra<=date_trunc('month',(date_trunc('month',current_date())-1)) then date_trunc('month',(date_trunc('month',current_date())-1)) end ano
                        from (
                        select distinct soci_soci_id, date_trunc('month', min(tiem_dia_id)) min_compra from mstrdb.dwh.ft_fdln_movimientos a
                        inner join mstrdb.dwh.lu_geog_local l on l.geog_locl_id=a.geog_locl_id and geog_unng_id in (2,3,5)
                                                                and geog_locl_cod not in ( 166,168,361,367,368,395,521,513,531)


                        group by all)
            )
        where ano is not null group by all))

, ecomm as (select distinct date_trunc( 'month',a.tiem_dia_id) ano,
            '0' ticket_tot,
             a.pedido_nbr||a.geog_locl_id ticket_plus ,
            c.soci_soci_id socios_activos
from MSTRDB.DWH.RPT_BI_CUBO_REEMPLAZO a
inner join mstrdb.dwh.lu_clie_cliente c on concat(a.clie_tipo_doc,a.clie_clie_ndoc)= concat(c.clie_tipo_doc,c.clie_clie_ndoc)
where month(a.tiem_dia_id)= month(date_trunc('month',date_trunc('month',current_date())-1))
and extract(year from a.tiem_dia_id) >2020 )
--select  count(distinct ticket_tot)-count(distinct ticket_plus) from ecomm; group by 1;

 , bas as (select  date_trunc( 'month',a.tiem_dia_id) ano,
            '0' ticket_tot,
            a.ec_nro_orden||a.ec_orden_clie_id  ticket_plus,
            c.soci_soci_id socios_activos
from MSTRDB.ECOMMERCE.BAS_FT_ORDEN_CABECERA a
inner join mstrdb.dwh.lu_clie_cliente c on concat(a.clie_tipo_doc,a.clie_clie_ndoc)= concat(c.clie_tipo_doc,c.clie_clie_ndoc)
where  month(a.tiem_dia_id)= month(date_trunc('month',date_trunc('month',current_date())-1))
and extract(year from a.tiem_dia_id) >2020  and ec_estado_orden='APROBADA'
)
--select  count(distinct ticket_tot)-count(distinct ticket_plus) from bas;

, fisico as (
            select date_trunc( 'month',a.tiem_dia_id) ano,
            a.ticket ticket_tot,
            b.ticket ticket_plus,
            soci_soci_id socios_activos
from mstrdb.dwh.ft_ventas a
left join mstrdb.dwh.ft_fdln_movimientos b on a.ticket=b.ticket and fdln_movt_tipo like '%RP%'
left join mstrdb.dwh.lu_geog_local l on l.geog_locl_id=a.geog_locl_id
where  month(a.tiem_dia_id)= month(date_trunc('month',date_trunc('month',current_date())-1))
and extract(year from a.tiem_dia_id) >2020
and geog_unng_id in (2,3,5)
) ---select * from fisico;

, comp as (
select ano ,  count(distinct ticket_tot_comp) ticket_tot_comp,
                count(distinct ticket_plus_comp) ticket_plus_comp,
                count(distinct socios_activos_comp) socios_activos_comp

from (
            select date_trunc( 'month',a.tiem_dia_id) ano,
            a.ticket ticket_tot_comp,
            b.ticket ticket_plus_comp,
            soci_soci_id socios_activos_comp
from mstrdb.dwh.ft_ventas a
left join mstrdb.dwh.ft_fdln_movimientos b on a.ticket=b.ticket and fdln_movt_tipo like '%RP%'
left join mstrdb.dwh.lu_geog_local l on l.geog_locl_id=a.geog_locl_id
where  month(a.tiem_dia_id)= month(date_trunc('month',date_trunc('month',current_date())-1))
and extract(year from a.tiem_dia_id) >2020
and geog_unng_id in (2,3,5) and geog_locl_cod not in ( 166,168,361,367,368,395,521,513,531)
)group by all) ---select * from fisico;

--select * from ecomm union all select * from fisico union all select * from bas;

 select a.ano, cl.socios_tot,socios.socios_acum, socios_activos,
    socios_activos/(lag(socios_activos , 1) over (order by  a.ano))-1 evolucion_socios,
    ticket_plus,
    ticket_plus/(lag(ticket_plus , 1) over (order by  a.ano))-1 evolucion_tickets_plus, ticket_plus/ticket_tot participacion_plus ,
    ticket_plus_comp,socios_acum_comp,socios_activos_comp, socios_activos_comp/(lag(socios_activos_comp , 1) over (order by  a.ano))-1 evolucion_socios_comp,
   ticket_plus_comp/(lag(ticket_plus_comp , 1) over (order by  a.ano))-1 evolucion_tickets_plus_Comp, ticket_plus_comp/ticket_tot_comp participacion_plus_comp


   from   (select ano,
                count(distinct ticket_tot) ticket_tot,
                count(distinct ticket_plus) ticket_plus,
                count(distinct socios_activos) socios_activos
        from
            (select * from ecomm
                union all
            select * from fisico
                union all
            select * from bas)
        group by 1) a
        left join socios on socios.ano=a.ano
        left join socios_comp on socios_comp.ano=a.ano
        left join comp on comp.ano=a.ano
 ,datos_clientes cl;