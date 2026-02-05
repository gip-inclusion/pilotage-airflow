with freins_par_dimensions as (
    select
        id_clpe                                                          as territoire_id,
        libelle_clpe                                                     as territoire_libelle,
        beneficiaire_rsa,
        beneficiaire_obligation_emploi,
        resident_zrr,
        resident_qpv,
        sexe,
        tranche_age,
        categorie_demandeurs_emploi,
        diagnostic_effectue,
        sum(nombre_demandeurs_emploi)                                    as nombre_demandeurs_emploi,
        sum(frein_numerique)                                             as frein_numerique,
        sum(frein_mobilite)                                              as frein_mobilite,
        sum(frein_familial)                                              as frein_familial,
        sum(frein_sante)                                                 as frein_sante,
        sum(frein_savoir)                                                as frein_savoir,
        sum(frein_logement)                                              as frein_logement,
        sum(frein_financier)                                             as frein_financier,
        sum(frein_admin_jur)                                             as frein_admin_jur,
        sum(demandeurs_emploi_avec_freins)                               as demandeurs_emploi_avec_freins,
        {{ count_intensite_frein('intensite_frein_numerique',  'frein_numerique') }},
        {{ count_intensite_frein('intensite_frein_mobilite',   'frein_mobilite') }},
        {{ count_intensite_frein('intensite_frein_familiale',  'frein_familiale') }},
        {{ count_intensite_frein('intensite_frein_sante',      'frein_sante') }},
        {{ count_intensite_frein('intensite_frein_savoir',     'frein_savoir') }},
        {{ count_intensite_frein('intensite_frein_logement',   'frein_logement') }},
        {{ count_intensite_frein('intensite_frein_financiere', 'frein_financiere') }},
        {{ count_intensite_frein('intensite_frein_admin_jur',  'frein_admin_jur') }}
    from {{ ref("int_nombre_freins_par_groupe") }}
    group by
        id_clpe,
        libelle_clpe,
        beneficiaire_rsa,
        beneficiaire_obligation_emploi,
        resident_zrr,
        resident_qpv,
        sexe,
        tranche_age,
        categorie_demandeurs_emploi,
        diagnostic_effectue
)

select
    f.territoire_id,
    f.territoire_libelle,
    f.beneficiaire_rsa,
    f.beneficiaire_obligation_emploi,
    f.resident_zrr,
    f.resident_qpv,
    f.sexe,
    f.tranche_age,
    f.categorie_demandeurs_emploi,
    f.diagnostic_effectue,
    f.nombre_demandeurs_emploi,
    f.demandeurs_emploi_avec_freins,
    frein_type.type_frein,
    frein_type.nbr_freins_declares,
    frein_type.frein_non_renseigne,
    frein_type.frein_faible,
    frein_type.frein_moyen,
    frein_type.frein_fort
from freins_par_dimensions as f
cross join lateral (
    values
    ('Numérique', f.frein_numerique, f.frein_numerique_non_renseigne, f.frein_numerique_faible, f.frein_numerique_moyen, f.frein_numerique_fort),
    ('Mobilité', f.frein_mobilite, f.frein_mobilite_non_renseigne, f.frein_mobilite_faible, f.frein_mobilite_moyen, f.frein_mobilite_fort),
    ('Lecture/Écriture/Calcul', f.frein_savoir, f.frein_savoir_non_renseigne, f.frein_savoir_faible, f.frein_savoir_moyen, f.frein_savoir_fort),
    ('Famille', f.frein_familial, f.frein_familiale_non_renseigne, f.frein_familiale_faible, f.frein_familiale_moyen, f.frein_familiale_fort),
    ('Difficultés Administratives/Juridiques', f.frein_admin_jur, f.frein_admin_jur_non_renseigne, f.frein_admin_jur_faible, f.frein_admin_jur_moyen, f.frein_admin_jur_fort),
    ('Logement/Hébergement', f.frein_logement, f.frein_logement_non_renseigne, f.frein_logement_faible, f.frein_logement_moyen, f.frein_logement_fort),
    ('Difficultés Financières', f.frein_financier, f.frein_financiere_non_renseigne, f.frein_financiere_faible, f.frein_financiere_moyen, f.frein_financiere_fort),
    ('Santé', f.frein_sante, f.frein_sante_non_renseigne, f.frein_sante_faible, f.frein_sante_moyen, f.frein_sante_fort)
) as frein_type (type_frein, nbr_freins_declares, frein_non_renseigne, frein_faible, frein_moyen, frein_fort)
