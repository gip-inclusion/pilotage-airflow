/*On créé une colonne par réseau au cas ou une même structure appartienne à différents réseaux */
with adherents_coorace as (
    select distinct
        ria."Réseau IAE" as reseau_coorace,
        s.id             as id_structure,
        (ria."SIRET")    as siret
    from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
    inner join structures as s
        on s.siret = ria."SIRET"
    where ria."Réseau IAE" = 'Coorace'
),

adherents_fei as (
    select distinct
        ria."Réseau IAE" as reseau_fei,
        s.id             as id_structure,
        (ria."SIRET")    as siret
    from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
    inner join structures as s
        on s.siret = ria."SIRET"
    where ria."Réseau IAE" = 'FEI'
),

adherents_emmaus as (
    select distinct
        ria."Réseau IAE" as reseau_emmaus,
        s.id             as id_structure,
        (ria."SIRET")    as siret
    from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
    inner join structures as s
        on s.siret = ria."SIRET"
    where ria."Réseau IAE" = 'Emmaus'
),

adherents_unai as (
    select distinct
        ria."Réseau IAE" as reseau_unai,
        s.id             as id_structure,
        (ria."SIRET")    as siret
    from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
    inner join structures as s
        on s.siret = ria."SIRET"
    where ria."Réseau IAE" = 'Unai'
),

adherents_cocagne as (
    select distinct
        ria."Réseau IAE" as reseau_cocagne,
        s.id             as id_structure,
        (ria."SIRET")    as siret
    from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
    inner join structures as s
        on s.siret = ria."SIRET"
    where ria."Réseau IAE" = 'Cocagne'
)
select
    {% if env_var('CI', '') %}
        id,
    {% else %}
        {{ dbt_utils.star(ref('stg_candidatures'), relation_alias='candidatures') }},
        {{ dbt_utils.star(ref('stg_org_prescripteur'), except=["id_org"], relation_alias='org_prescripteur') }},
        {{ dbt_utils.star(ref('stg_bassin_emploi'), except=["nom_departement","nom_region","type_epci","id_structure"] ,relation_alias='bassin_emploi') }},
        {{ dbt_utils.star(ref('stg_reseaux'), except=["SIRET","id_structure"] ,relation_alias='rsx') }},
    {% endif %}
    nom_org.type_auteur_diagnostic_detaille,
    case
        when adherents_emmaus.reseau_emmaus = 'Emmaus' then 'Oui'
        else 'Non'
    end as reseau_emmaus,
    case
        when adherents_coorace.reseau_coorace = 'Coorace' then 'Oui'
        else 'Non'
    end as reseau_coorace,
    case
        when adherents_fei.reseau_fei = 'FEI' then 'Oui'
        else 'Non'
    end as reseau_fei,
    case
        when adherents_unai.reseau_unai = 'Unai' then 'Oui'
        else 'Non'
    end as reseau_unai,
    case
        when adherents_cocagne.reseau_cocagne = 'Cocagne' then 'Oui'
        else 'Non'
    end as reseau_cocagne
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('stg_bassin_emploi') }} as bassin_emploi
    on bassin_emploi.id_structure = candidatures.id_structure
left join adherents_emmaus
    on adherents_emmaus.id_structure = candidatures.id_structure
left join adherents_coorace
    on adherents_coorace.id_structure = candidatures.id_structure
left join adherents_fei
    on adherents_fei.id_structure = candidatures.id_structure
left join adherents_unai
    on adherents_unai.id_structure = candidatures.id_structure
left join adherents_cocagne
    on adherents_cocagne.id_structure = candidatures.id_structure
left join {{ ref('stg_org_prescripteur') }} as org_prescripteur
    on org_prescripteur.id_org = candidatures.id_org_prescripteur
left join {{ ref('stg_reseaux') }} as rsx
    on rsx.id_structure = candidatures.id_structure
left join {{ ref('nom_prescripteur') }} as nom_org
    on nom_org.origine_detaille = candidatures."origine_détaillée"
