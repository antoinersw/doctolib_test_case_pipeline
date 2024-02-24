DROP TABLE if exists appointments_by_centers;

CREATE TABLE if not exists appointments_by_centers (
    id INT,
    code_region TEXT,
    region TEXT,
    departement TEXT,
    id_centre TEXT,
    nom_centre TEXT,
    rang_vaccinal INTEGER,
    date_debut_semaine DATE,
    nb INTEGER,
    nb_rdv_cnam INTEGER,
    nb_rdv_rappel INTEGER,
    execution_date DATE
);
CREATE INDEX if not exists appointments_by_centers_index
ON appointments_by_centers (id_centre,date_debut_semaine,departement);