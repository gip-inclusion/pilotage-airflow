select
    {{ pilo_star(ref('suivi_etp_realises_v2'), relation_alias='etp_r') }},
    etp_c."effectif_mensuel_conventionné",
    etp_c."effectif_annuel_conventionné",
    salarie.hash_nir,
    (etp_r.nombre_etp_consommes_reels_annuels * etp_r.af_montant_unitaire_annuel_valeur) as montant_utilise,
    etp_c.af_montant_total_annuel                                                        as montant_alloue,
    case
        when salarie.salarie_rci_libelle = 'MME' then 'Femme'
        when salarie.salarie_rci_libelle = 'M.' then 'Homme'
        else 'Non renseigné'
    end                                                                                  as genre_salarie
from
    {{ ref('suivi_etp_realises_v2') }} as etp_r
left join {{ ref('fluxIAE_Salarie_v2') }} as salarie
    on etp_r.identifiant_salarie = salarie.salarie_id
left join {{ ref('suivi_etp_conventionnes_v2') }} as etp_c
    on etp_c.id_annexe_financiere = etp_r.id_annexe_financiere
where etp_r.type_structure not in ('ACIPA_DC', 'EIPA_DC', 'FDI')
