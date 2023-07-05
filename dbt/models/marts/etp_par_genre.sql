select
    {{ pilo_star(ref('suivi_etp_realises_v2'), relation_alias='etp_r') }},
    etp_c."effectif_mensuel_conventionné",
    etp_c."effectif_annuel_conventionné",
    case
        when salarie.salarie_rci_libelle = 'MME' then 'Femme'
        when salarie.salarie_rci_libelle = 'M.' then 'Homme'
        else 'Non renseigné'
    end as genre_salarie
from
    {{ ref('suivi_etp_realises_v2') }} as etp_r
left join {{ ref('stg_salarie') }} as salarie
    on etp_r.identifiant_salarie = salarie.salarie_id
left join {{ ref('suivi_etp_conventionnes_v2') }} as etp_c
    on etp_c.id_annexe_financiere = etp_r.id_annexe_financiere
