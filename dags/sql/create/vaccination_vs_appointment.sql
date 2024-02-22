CREATE TABLE if not exists vaccination_vs_appointments (
    id INT,
    id_centre TEXT,
    date_debut_semaine DATE,
    code_region TEXT,
    nom_region TEXT,
    code_departement TEXT,
    nom_departement TEXT,
    commune_insee TEXT,
    nom_centre TEXT,
    nombre_ucd TEXT,
    doses_allouees TEXT,
    rdv_pris TEXT
);
CREATE INDEX if not exists vaccination_vs_appointments_index
ON vaccination_vs_appointments(id_centre,code_departement,date_debut_semaine);