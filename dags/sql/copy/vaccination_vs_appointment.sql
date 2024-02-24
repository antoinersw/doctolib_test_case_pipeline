 COPY vaccination_vs_appointments(id,
 id_centre, 
 date_debut_semaine, 
 code_region, 
 nom_region, 
 code_departement, 
 nom_departement, 
 commune_insee, 
 nom_centre, 
 nombre_ucd, 
 doses_allouees, 
 rdv_pris,
 execution_date) 
        FROM '/opt/data/transformed/vaccination_vs_appointment_ds.csv' 
            DELIMITER ',' 
            CSV HEADER; 