with all_fdp as (
    select
        {{ pilo_star(ref('stg_fdp')) }},
        '1- Fiches de poste' as etape,
        nb_fdp_struct        as valeur
    from stg_fdp
    union all
    select
        {{ pilo_star(ref('stg_fdp_actives')) }},
        '2- Fiches de poste actives' as etape,
        nb_fdp_struct                as valeur
    from stg_fdp_actives
    union all
    select
        {{ pilo_star(ref('stg_fdp_actives_sans_rec_30jrs')) }},
        '3- Fiches de poste actives sans recrutement dans les 30 derniers jours' as etape,
        nb_fdp_struct                                                            as valeur
    from stg_fdp_actives_sans_rec_30jrs
    union all
    select
        {{ pilo_star(ref('stg_fdp_actives_sans_rec_30jrs_sans_motif_pasdeposteouvert')) }},
        '4- Fiches de poste actives sans recrutement dans les 30 derniers jours et sans motif pas de poste ouvert' as etape,
        nb_fdp_struct                                                                                              as valeur
    from stg_fdp_actives_sans_rec_30jrs_sans_motif_pasdeposteouvert
    union all
    select
        {{ pilo_star(ref('stg_fdp_difficulte_recrutement')) }},
        '5- Fiches de poste en difficulté de recrutement' as etape,
        nb_fdp_struct                                     as valeur
    from stg_fdp_difficulte_recrutement
    union all
    select
        {{ pilo_star(ref('stg_fdp_difficulte_recrutement_sans_candidatures')) }},
        '6- Fiches de poste en difficulté de recrutement n ayant jamais reçu de candidatures' as etape,
        nb_fdp_struct                                                                         as valeur
    from stg_fdp_difficulte_recrutement_sans_candidatures
)

select
    domaine_professionnel,
    grand_domaine,
    rome,
    "nom_département_structure",
    "département_structure",
    "région_structure",
    epci_structure,
    bassin_emploi_structure,
    type_structure,
    id_structure,
    nom_structure,
    etape,
    sum(valeur) as valeur
from all_fdp
group by
    domaine_professionnel,
    grand_domaine,
    rome,
    "nom_département_structure",
    "département_structure",
    "région_structure",
    epci_structure,
    bassin_emploi_structure,
    type_structure,
    id_structure,
    nom_structure,
    etape
