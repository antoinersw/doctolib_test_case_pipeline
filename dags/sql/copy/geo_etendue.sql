COPY geo_etendue(id,code_commune_INSEE, nom_commune_postal, code_postal, libelle_acheminement, ligne_5, latitude, longitude, code_commune, article, nom_commune, nom_commune_complet, code_departement, nom_departement, code_region, nom_region,execution_date)
FROM '/opt/data/transformed/geo_etendue_ds.csv'
DELIMITER ','
CSV HEADER;
