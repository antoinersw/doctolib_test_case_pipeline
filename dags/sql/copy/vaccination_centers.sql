 COPY vaccination_centers(id,gid,nom,arrete_pref_numero,xy_precis,id_adr,adr_num,adr_voie,com_cp,com_insee,com_nom,lat_coor1,long_coor1,structure_siren,structure_type,structure_rais,structure_num,structure_voie,structure_cp,structure_insee,structure_com,_userid_creation,_userid_modification,_edit_datemaj,lieu_accessibilite,rdv_lundi,rdv_mardi,rdv_mercredi,rdv_jeudi,rdv_vendredi,rdv_samedi,rdv_dimanche,rdv,date_fermeture,date_ouverture,rdv_site_web,rdv_tel,rdv_tel2,rdv_modalites,rdv_consultation_prevaccination,centre_svi_repondeur,centre_fermeture,reserve_professionels_sante,centre_type)
    FROM '/opt/data/processed/vaccination_centers_ds.csv'
        DELIMITER ','
        CSV HEADER ; 