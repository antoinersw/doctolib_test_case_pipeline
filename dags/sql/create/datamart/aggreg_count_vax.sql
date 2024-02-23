DROP TABLE IF EXISTS aggreg_count_vax;
CREATE TABLE IF NOT EXISTS aggreg_count_vax (
    gid TEXT,
    nom_centre TEXT,
    nb_of_appointments INTEGER,
    is_with_doctolib INTEGER,
    cp_commune TEXT
);

 insert into aggreg_count_vax  (
    gid ,
    nom_centre ,
    nb_of_appointments ,
    is_with_doctolib 
    ,cp_commune 

)
    with base_table as (
        select vc.gid, vc.nom,vc.com_cp
            ,case
                when vc.rdv_site_web ~ 'doctolib' then 1
                else 0
            END as is_with_doctolib
        from vaccination_centers as vc
    ),
    aggreg_count_vax_bis as (
        select bt.gid, bt.nom as nom_centre, sum(abc.nb) as nb_of_appointments
            ,bt.is_with_doctolib,bt.com_cp as cp_commune
            from base_table as bt
                 left join appointments_by_centers as abc on Cast(bt.gid as varchar(120))= abc.id_centre
                    group by abc.id_centre,abc.nom_centre,bt.gid,bt.nom,bt.is_with_doctolib,abc.nb,bt.com_cp 
                        order by abc.nb desc
    )   
    select gid, nom_centre,nb_of_appointments,is_with_doctolib, cp_commune
    from aggreg_count_vax_bis as vax 
 