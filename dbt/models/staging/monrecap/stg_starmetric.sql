select
    starm.derniere_reponse_barometre,
    starm.date_de_derniere_reponse_au_barometre,
    starm.email,
    starm.email_commande,
    starm.structure,
    starm.date_de_premiere_commande,
    starm.date_de_derniere_commande,
    starm.nombre_total_carnets_commandes,
    starm.combien_de_carnets_distribues,
    starm.combien_dusagers_ont_perdu_le_carnet,
    starm.amelioration_avec_mon_recap,
    dpt.nom_departement,
    0.971 as taux_de_perte,
    case
        when starm.date_de_derniere_reponse_au_barometre is not null then 'Oui'
        else 'Non'
    end   as reponse_barometre
from {{ ref('stg_baro_commandes_starmetric') }} as starm
left join {{ ref('stg_departement_derniere_commandes') }} as dpt
    on starm.email_commande = dpt.email_commande
where (starm.date_de_derniere_reponse_au_barometre = starm.derniere_reponse_barometre or starm.derniere_reponse_barometre is null or starm.date_de_derniere_reponse_au_barometre is null)
group by
    starm.derniere_reponse_barometre,
    starm.date_de_derniere_reponse_au_barometre,
    starm.email,
    starm.email_commande,
    starm.structure,
    starm.date_de_premiere_commande,
    starm.date_de_derniere_commande,
    starm.nombre_total_carnets_commandes,
    starm.combien_de_carnets_distribues,
    starm.combien_dusagers_ont_perdu_le_carnet,
    starm.amelioration_avec_mon_recap,
    dpt.nom_departement
