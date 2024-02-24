drop table if exists dim_geo;
create table dim_geo as (
    select distinct d.com_cp,left(d.com_cp,2) as short_cp  
    ,com_nom as nom_commune,g.nom_departement,g.nom_region 
        from vaccination_centers as d
            LEFT JOIN geo_etendue as g ON g.code_postal = d.com_cp
                WHERE left(d.com_cp,2)<>'99'

);

CREATE INDEX dim_geo_index
ON dim_geo (com_cp,short_cp);