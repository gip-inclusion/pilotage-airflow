select
    cel.*,
    crdp.grand_domaine as metier,
    fdp.nom_rome,
    crdp.code_rome
from
    {{ ref('candidatures_echelle_locale') }} as cel
left join {{ ref('candidats') }} as cand on cel.id_candidat = cand.id
inner join
    {{ source('emplois', 'fiches_de_poste_par_candidature') }} as fdpc
    on
        cel.id = fdpc.id_candidature
inner join
    {{ source('emplois', 'fiches_de_poste') }} as fdp
    on
        fdpc.id_fiche_de_poste = fdp.id
inner join {{ ref('code_rome_domaine_professionnel') }} as crdp on
    fdp.code_rome = crdp.code_rome
