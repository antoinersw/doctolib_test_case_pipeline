DROP TABLE if exists stock;

CREATE TABLE if not exists stock (
    id INT,
    code_departement TEXT,
    departement TEXT,
    raison_sociale TEXT,
    libelle_pui TEXT,
    finess TEXT,
    type_de_vaccin TEXT,
    nb_ucd INTEGER,
    nb_doses INTEGER,
 
    date DATE
);