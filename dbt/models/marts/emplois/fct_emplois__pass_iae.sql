with pass_iae as (
    select *
    from {{ ref('stg_emplois__pass_iae') }}
),

suspensions as (
    select *
    from {{ ref('int_emplois__pass_iae_suspensions') }}
),

prolongations as (
    select *
    from {{ ref('int_emplois__pass_iae_prolongations') }}
),

demandes_prolongation as (
    select *
    from {{ ref('int_emplois__pass_iae_demandes_prolongation') }}
),

parcours as (
    select
        pass.id_pass_iae,
        pass.id_candidat,
        pass.hash_numero_pass_iae,
        pass.injection_ai,
        pass.id_structure,
        pass.date_debut,
        pass.date_fin,
        pass.type_pass,
        pass.duree,
        pass.is_duree_indeterminee,
        suspensions.date_premiere_suspension,
        suspensions.date_derniere_suspension,
        suspensions.date_de_fin_derniere_suspension,
        prolongations.date_premiere_prolongation,
        prolongations.date_derniere_prolongation,
        prolongations.date_debut_derniere_prolongation,
        prolongations.date_fin_derniere_prolongation,
        demandes.date_premiere_demande_prolongation,
        demandes.date_derniere_demande_prolongation,
        demandes.date_dernier_traitement_demande,
        demandes.etat_derniere_demande_prolongation,
        cast({{ dbt.current_timestamp() }} at time zone 'Europe/Paris' as date) as date_execution,
        coalesce(suspensions.nombre_suspensions, 0)                             as nombre_suspensions,
        case
            when suspensions.id_pass_iae is null then false
            else suspensions.is_suspension_en_cours
        end                                                                     as is_suspension_en_cours,
        coalesce(prolongations.nombre_prolongations, 0)                         as nombre_prolongations,
        coalesce(demandes.nombre_demandes_prolongation, 0)                      as nombre_demandes_prolongation,
        coalesce(demandes.nombre_demandes_acceptees, 0)                         as nombre_demandes_acceptees,
        coalesce(demandes.nombre_demandes_refusees, 0)                          as nombre_demandes_refusees,
        coalesce(demandes.nombre_demandes_en_attente, 0)                        as nombre_demandes_en_attente
    from pass_iae as pass
    left join suspensions
        on pass.id_pass_iae = suspensions.id_pass_iae
    left join prolongations
        on pass.id_pass_iae = prolongations.id_pass_iae
    left join demandes_prolongation as demandes
        on pass.id_pass_iae = demandes.id_pass_iae
),

validite as (
    select
        *,
        date_execution >= date_debut
        and (is_duree_indeterminee or date_execution <= date_fin) as is_pass_valide
    from parcours
),

final as (
    select
        id_pass_iae,
        id_candidat,
        type_pass,
        hash_numero_pass_iae,
        injection_ai,
        id_structure,
        date_debut,
        date_fin,
        duree,
        is_duree_indeterminee,
        nombre_suspensions,
        date_premiere_suspension,
        date_derniere_suspension,
        date_de_fin_derniere_suspension,
        is_suspension_en_cours,
        nombre_prolongations,
        date_premiere_prolongation,
        date_derniere_prolongation,
        date_debut_derniere_prolongation,
        date_fin_derniere_prolongation,
        nombre_demandes_prolongation,
        nombre_demandes_acceptees,
        nombre_demandes_refusees,
        nombre_demandes_en_attente,
        date_premiere_demande_prolongation,
        date_derniere_demande_prolongation,
        date_dernier_traitement_demande,
        etat_derniere_demande_prolongation,
        is_pass_valide,
        case
            when date_fin < date_execution then 'Expiré'
            when date_debut > date_execution then 'À venir'
            when is_pass_valide is null then null
            when is_suspension_en_cours then 'Suspendu'
            when is_suspension_en_cours is null then null
            else 'Actif'
        end                                           as statut_pass,
        is_pass_valide and not is_suspension_en_cours as is_pass_actif
    from validite
)

select *
from final
