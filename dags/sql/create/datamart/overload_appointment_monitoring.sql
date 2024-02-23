DROP TABLE IF EXISTS overload_appointment_monitoring;
CREATE TABLE if not exists overload_appointment_monitoring (
    mois TEXT,
    annee TEXT,
    week_num TEXT,
    id_centre TEXT,
    departement TEXT,
    nb_rdv_pris INTEGER,
    doses_allouees INTEGER,
    rdv_planifies INTEGER,
    mois_anee TEXT,
    semaine_annee TEXT
    );


insert into overload_appointment_monitoring (
    mois, annee, week_num, id_centre, departement, nb_rdv_pris, doses_allouees, rdv_planifies, mois_anee, semaine_annee
)
    with 
        grouped_table as ( 
                select da.mois,da.annee,da.week_num
                ,bt.id_centre,bt.departement
                , sum(bt.nb) as nb_rdv_pris,bt."date_debut_semaine"
            from appointments_by_centers as bt 
            left join dim_date as da on date(da.Jour) = date(bt.date_debut_semaine)
                group by da.mois,da.annee,da.week_num,bt.id_centre,bt.departement,bt.date_debut_semaine
            
)
 ,   vax_vs_appointments as (
        select vva."date_debut_semaine", vva.id_centre
            ,vva.code_departement, sum(CAST(vva.doses_allouees as INTEGER)) as doses_allouees, sum(CAST(vva.rdv_pris as INTEGER)) as rdv_planifies
           ,da.mois,da.annee,da.week_num
            from vaccination_vs_appointments as vva
            left join dim_date as da on date(da.Jour) = date(vva.date_debut_semaine)
            group by vva."date_debut_semaine", vva.id_centre, vva.code_departement, 
                       da.annee,da.mois,da.annee,da.week_num,vva.rdv_pris, vva.doses_allouees 

    )
    ,final_table as (
        select gt.mois, gt.annee, gt.week_num, gt.id_centre, gt.departement, gt.nb_rdv_pris,
       vax.doses_allouees, vax.rdv_planifies, concat(gt.mois,' ',gt.annee) as mois_anee,concat(gt.week_num, ' ',gt.annee) as semaine_annee 
        from grouped_table as gt
            left join vax_vs_appointments as vax  
            on date(vax."date_debut_semaine") = date(gt."date_debut_semaine")
            and vax.id_centre = gt.id_centre
            and vax.code_departement = gt.departement
    )

    select * from final_table  order by rdv_planifies desc;
 
 