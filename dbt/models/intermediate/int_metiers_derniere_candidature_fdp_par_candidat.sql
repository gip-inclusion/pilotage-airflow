with candidatures as (
    select
        date_candidature,
        id_candidat,
        hash_nir,
        code_rome_fpd as code_rome_fdp,
        nom_rome_fdp
    from {{ ref('candidatures_recues_par_fiche_de_poste') }}
),

derniere_candidature_par_candidat as (
    select
        hash_nir,
        max(date_candidature) as date_candidature
    from candidatures
    group by hash_nir
)

select candidatures.*
from candidatures
inner join derniere_candidature_par_candidat
    on candidatures.hash_nir = derniere_candidature_par_candidat.hash_nir and candidatures.date_candidature = derniere_candidature_par_candidat.date_candidature
where hash_nir is not null
