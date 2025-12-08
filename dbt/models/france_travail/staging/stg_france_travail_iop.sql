select
    iop.index                                                        as id,
    iop."Cat."                                                       as cat_demandeur_emploi,
    iop."Date mise à jour du diagnostic"                             as date_maj_diagnostic,
    iop."Age"                                                        as age,
    iop."Résident QPV"                                               as qpv,
    iop."Code postal"                                                as code_postal,
    iop."Code INSEE commune"::TEXT                                   as code_insee,
    iop.code_safir,
    iop.structure_suivie,
    iop."AAH"                                                        as aah,
    iop."ASS"                                                        as ass,
    iop."PPA"                                                        as ppa,
    iop."PPA Majorée"                                                as ppa_majoree,
    iop."RSA"                                                        as rsa,
    iop."Accéder au numérique et en maîtriser les fondamentaux"      as numerique_frein,
    iop."Développer sa mobilité"                                     as mobilite_frein,
    iop."Développer ses capacités en lecture, écriture et calcul"    as lecture_ecriture_calcul_frein,
    iop."Faire face à des contraintes familiales"                    as famille_frein,
    iop."Faire face à des difficultés administratives ou juridiques" as difficultes_administratives_ou_juridiques_frein,
    iop."Faire face à des difficultés de logement"                   as logement_hebergement_frein,
    iop."Faire face à des difficultés financières"                   as difficultes_financieres_frein,
    iop."Prendre en compte son état de santé"                        as sante_frein,
    case
        when iop."Civilité" = 'M.' then 'H'
        else 'F'
    end                                                              as sexe
from {{ source('france_travail', 'raw_ft_iop_data') }} as iop
