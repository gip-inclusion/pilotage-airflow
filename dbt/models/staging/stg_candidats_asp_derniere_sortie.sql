select
    pass."hash_numéro_pass_iae"                                       as pass_iae,
    c.id                                                              as id_salarie_emplois,
    salarie.salarie_id                                                as id_salarie_asp,
    sortie.rcs_libelle                                                as cause_sortie,
    sortie.rms_libelle                                                as motif_sortie,
    to_date(max(sortie.contrat_date_sortie_definitive), 'DD/MM/YYYY') as date_derniere_sortie,
    max(cddr.date_candidature)                                        as date_derniere_candidature
from {{ source('fluxIAE','fluxIAE_Salarie') }} as salarie
-- pour récupérer le nir pour ensuite pouvoir filtrer les candidats non ft
left join {{ ref('pass_agrements_valides') }} as pass
    on salarie."hash_numéro_pass_iae" = pass."hash_numéro_pass_iae"
left join {{ ref('candidats') }} as c
    on c.id = pass.id_candidat
left join {{ ref('candidatures_echelle_locale') }} as cddr
    on c.id = cddr.id_candidat
left join {{ ref('sorties_v2') }} as sortie
    on salarie.salarie_id = sortie.emi_pph_id
where pass.validite_pass = 'pass valide' and sortie.contrat_date_sortie_definitive is not null
group by
    pass."hash_numéro_pass_iae",
    c.id,
    sortie.rcs_libelle,
    sortie.rms_libelle,
    salarie.salarie_id
