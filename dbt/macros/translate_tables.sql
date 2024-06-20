{% macro translate_motif_refus(field) -%}
case {{ field }}
    when 'other' then 'Autre motif saisi sur les emplois de l''inclusion'
    when 'hired_elsewhere' then 'Candidat indisponible (en emploi)'
    when 'training' then 'Candidat indisponible (en formation)'
    when 'unavailable' then 'Candidat indisponible ou non intéressé par le poste'
    when 'did_not_come_to_interview' then 'Candidat ne s’étant pas présenté à l’entretien'
    when 'non_eligible' then 'Candidat non éligible'
    when 'not_interested' then 'Candidat non intéressé'
    when 'did_not_come' then 'Candidat non joignable'
    when 'not_mobile' then 'Candidat non mobile'
    when 'duplicate' then 'Candidature en doublon'
    when 'poorly_informed' then 'Candidature pas assez renseignée'
    when 'eligibility_doubt' then 'Doute sur l''éligibilité du candidat'
    when 'prevent_objectives' then 'Embauche incompatible avec les objectifs du dialogue de gestion'
    when 'approval_expiration_too_close' then 'La date de fin du pass est trop proche'
    when 'deactivation' then 'La structure n''est plus conventionnée'
    when 'lacking_skills' then 'Compétences insuffisantes pour le poste'
    when 'no_position' then 'Pas de recrutement en cours'
    when 'incompatible' then 'Freins à l''emploi incompatible avec le poste proposé'
    else {{ field }}
end
{%- endmacro -%}
