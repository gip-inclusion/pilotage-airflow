select
    hash_nir,
    t.critere
from {{ ref('stg_candidats') }}
cross join
    lateral (
        values
        ('critère_n1_bénéficiaire_du_rsa', "critère_n1_bénéficiaire_du_rsa"),
        ('critère_n1_allocataire_ass', "critère_n1_allocataire_ass"),
        ('critère_n1_allocataire_aah', "critère_n1_allocataire_aah"),
        ('critère_n1_detld_plus_de_24_mois', "critère_n1_detld_plus_de_24_mois"),
        ('critère_n2_senior_plus_de_50_ans', "critère_n2_senior_plus_de_50_ans"),
        ('critère_n2_jeune_moins_de_26_ans', "critère_n2_jeune_moins_de_26_ans"),
        ('critère_n2_sortant_de_l_ase', "critère_n2_sortant_de_l_ase"),
        ('critère_n2_deld_12_à_24_mois', "critère_n2_deld_12_à_24_mois"),
        ('critère_n2_travailleur_handicapé', "critère_n2_travailleur_handicapé"),
        ('critère_n2_parent_isolé', "critère_n2_parent_isolé"),
        ('critère_n2_personne_sans_hébergement_ou_hébergée_ou_ayant_u', "critère_n2_personne_sans_hébergement_ou_hébergée_ou_ayant_u"),
        ('critère_n2_réfugié_statutaire_bénéficiaire_d_une_protection', "critère_n2_réfugié_statutaire_bénéficiaire_d_une_protection"),
        ('critère_n2_résident_zrr', "critère_n2_résident_zrr"),
        ('critère_n2_résident_qpv', "critère_n2_résident_qpv"),
        ('critère_n2_sortant_de_détention_ou_personne_placée_sous_main', "critère_n2_sortant_de_détention_ou_personne_placée_sous_main"),
        ('critère_n2_maîtrise_de_la_langue_française_inférieure_au_ni', "critère_n2_maîtrise_de_la_langue_française_inférieure_au_ni"),
        ('critère_n2_problème_de_mobilité', "critère_n2_problème_de_mobilité")
    ) as t (critere, valeur)
where valeur = 1 and hash_nir is not null
