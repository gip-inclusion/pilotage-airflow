select
    id_structure_asp id_struct,
    annee_sortie,
    --categorie sortie
    count(categorie_sortie) filter (where categorie_sortie = 'Emploi durable') as emploi_durable,
    count(categorie_sortie) filter (where categorie_sortie = 'Emploi de transition') as emploi_de_transition,
    count(categorie_sortie) filter (where categorie_sortie = 'Sorties positives') as sorties_positives,
    count(categorie_sortie) filter (where categorie_sortie = 'Autres sorties') as autres_sorties,
    count(categorie_sortie) filter (where categorie_sortie = 'Retrait des sorties constatées') as "retrait_des_sorties_constatées"
from
    sorties
where
    sorties.type_siae = 'EITI'
group by
    id_structure_asp,
    annee_sortie;