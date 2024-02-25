COPY type_de_vaccin_tranche_dage_et_departement (
    id,
    date_reference,
    semaine_injection,
    region_residence,
    libelle_region,
    departement_residence,
    libelle_departement,
    population_insee,
    classe_age,
    libelle_classe_age,
    type_vaccin,
    effectif_1_inj,
    effectif_termine,
    effectif_cumu_1_inj,
    effectif_cumu_termine,
    taux_1_inj,
    taux_termine,
    taux_cumu_1_inj,
    taux_cumu_termine,
    date,
    effectif_rappel,
    effectif_cumu_rappel,
    effectif_rappel_parmi_eligible,
    effectif_eligible_au_rappel,
    taux_rappel,
    taux_cumu_rappel,
    taux_cumu_rappeleli
    ,execution_date
)
FROM '/opt/data/transformed/type_de_vaccin_tranche_dage_et_departement_ds.csv'
DELIMITER ','
CSV HEADER;
