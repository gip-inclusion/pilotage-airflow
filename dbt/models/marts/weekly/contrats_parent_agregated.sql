select
    contrat_id_structure,
    type_dispositif                          as type_structure,
    nom_departement_structure,
    nom_region_structure,
    code_dept_structure,
    motif_sortie,
    categorie_sortie,
    salarie_sorti,
    structure_denomination_unique,
    extract(year from contrat_date_embauche) as annee_embauche,
    sum(duree_contrat_jours)                 as sum_duree_contrat_jours,
    sum(duree_contrat_mois)                  as sum_duree_contrat_mois,
    sum(duree_contrat_asp_mois)              as sum_duree_contrat_asp_mois,
    sum(contrat_nb_heures_travail)           as sum_contrat_nb_heures_travail,
    count(*)                                 as nombre_contrats
from {{ ref("stg_contrats_parent") }}
where contrat_nb_heures_travail > 0
group by
    contrat_id_structure,
    type_dispositif,
    nom_departement_structure,
    nom_region_structure,
    code_dept_structure,
    extract(year from contrat_date_embauche),
    motif_sortie,
    categorie_sortie,
    salarie_sorti,
    structure_denomination_unique
