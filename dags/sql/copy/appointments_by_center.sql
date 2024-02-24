 COPY appointments_by_centers(
    id,
 code_region
 , region
 , departement
 , id_centre
 , nom_centre
 , rang_vaccinal
 , date_debut_semaine
 , nb
 , nb_rdv_cnam
 , nb_rdv_rappel
 ,execution_date
 ) 
            FROM '/opt/data/transformed/appointments_by_center_ds.csv' 
                DELIMITER ',' 
                CSV HEADER; 