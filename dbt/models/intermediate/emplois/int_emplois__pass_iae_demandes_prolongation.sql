with demandes as (
    select
        *,
        row_number() over (
            partition by id_pass_iae
            order by date_de_demande desc nulls last, id_demande_prolongation desc
        ) as rang
    from {{ ref('stg_emplois__demandes_prolongation') }}
)

select
    id_pass_iae,
    count(*)                                   as nombre_demandes_prolongation,
    count(*) filter (where etat = 'Acceptée')  as nombre_demandes_acceptees,
    count(*) filter (where etat = 'Refusée')   as nombre_demandes_refusees,
    count(*) filter (where etat = 'À traiter') as nombre_demandes_en_attente,
    min(date_de_demande)                       as date_premiere_demande_prolongation,
    max(date_de_demande)                       as date_derniere_demande_prolongation,
    max(date_traitement)                       as date_dernier_traitement_demande,
    max(etat) filter (where rang = 1)          as etat_derniere_demande_prolongation
from demandes
group by id_pass_iae
