select
    "Combien de carnets avez-vous distribués depuis le début de vo"  as combien_de_carnets_distribues,
    "Parmi ces usagers, combien d'usagers ont perdu le carnet ou ne" as combien_dusagers_ont_perdu_le_carnet,
    "Avez-vous constaté une amélioration dans le parcours de vos u"  as amelioration_avec_mon_recap,
    max("Submitted at"::DATE)                                        as derniere_reponse_barometre,
    coalesce(email, "Votre adresse mail ?")                          as votre_adresse_mail
from {{ ref('barometre') }}
group by
    combien_de_carnets_distribues,
    combien_dusagers_ont_perdu_le_carnet,
    amelioration_avec_mon_recap,
    votre_adresse_mail
