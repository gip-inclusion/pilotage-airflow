select
    asp.numero_annexe_financiere,
    asp."Année"            as annee_asp,
    {{ pilo_star(source('fluxIAE','fluxIAE_BenefSorties_ASP'), relation_alias = 'asp', except=['numero_annexe_financiere', 'Année', '0 - SIRET  ', '0 - Dénomination']) }},
    asp."0 - SIRET  "      as siret_asp,
    strct.structure_id_siae,
    strct.structure_siret_actualise,
    strct.structure_denomination_unique,
    strct.structure_denomination,
    asp."0 - Dénomination" as denomination_asp,
    strct.zone_emploi_structure,
    strct.nom_epci_structure,
    strct.nom_region_structure,
    strct.nom_departement_structure
from {{ source('fluxIAE','fluxIAE_BenefSorties_ASP') }} as asp
left join {{ ref('fluxIAE_Structure_v2') }} as strct
    on asp."0 - SIRET  " = strct.structure_siret_actualise
