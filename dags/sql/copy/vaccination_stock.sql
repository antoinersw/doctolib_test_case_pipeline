 COPY stock(  
    id,
    code_departement
    , departement
    , raison_sociale
    , libelle_pui
    , finess
    , type_de_vaccin, nb_ucd, nb_doses, date,execution_date)
        FROM '/opt/data/transformed/vaccination_stock_ds.csv'
            DELIMITER ','
            CSV HEADER; 