select
    cel.*,
    crdp."grand_domaine" as metier,
    fdp."nom_rome"       as nom_rome,
    crdp."code_rome"     as code_rome
from
    {{ ref('candidatures_echelle_locale') }} as cel
left join {{ ref('candidats') }} as cand on cand.id = cel.id_candidat
inner join
    {{ source('emplois', 'fiches_de_poste_par_candidature') }} as fdpc
    on
        fdpc.id_candidature = cel.id
inner join
    {{ source('emplois', 'fiches_de_poste') }} as fdp
    on
        fdpc.id_fiche_de_poste = fdp.id
inner join {{ ref('code_rome_domaine_professionnel') }} as crdp on
    fdp.code_rome = crdp.code_rome
