select
    oi.id_clpe,
    oi.libelle_clpe,
    oi.date_extraction,
    clpe.departement_id,
    clpe.nom_departement,
    clpe.nom_region,
    oi.nombre_demandeurs_emploi,
    oi.demandeurs_emploi_avec_freins,
    oi.type_frein,
    oi.nbr_total_thematiques,
    oi.nbr_de_par_frein,
    oi.nbr_thematique,
    oi.indice_couverture,
    case
        when clpe.nom_departement is not null then concat(clpe.departement_id, '-', clpe.nom_departement)
    end as departement,
    case
        when indice_couverture >= 0.8 then 'Très faible couverture'
        when indice_couverture >= 0.6 then 'Faible couverture'
        when indice_couverture >= 0.4 then 'Couverture intermédiaire'
        when indice_couverture >= 0.2 then 'Bonne couverture'
        when indice_couverture < 0.2 then 'Très bonne couverture'
        else 'information de couverture absente'
    end as niveau_de_couverture
from {{ ref('fct_offre_insertion') }} as oi
left join {{ ref('dim_clpe_ft') }} as clpe
    on oi.id_clpe = clpe.territoire_id
