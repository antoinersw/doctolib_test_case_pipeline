drop table if exists geo_etendue;
CREATE TABLE if not exists geo_etendue (
  code_commune_INSEE TEXT,
  nom_commune_postal TEXT,
  code_postal TEXT,
  libelle_acheminement TEXT,
  ligne_5 TEXT,
  latitude FLOAT,
  longitude FLOAT,
  code_commune TEXT,
  article TEXT,
  nom_commune TEXT,
  nom_commune_complet TEXT,
  code_departement TEXT,
  nom_departement TEXT,
  code_region TEXT,
  nom_region TEXT
);