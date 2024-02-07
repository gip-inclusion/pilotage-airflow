select count(distinct nom_organisation)
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
where type_utilisateur = 'institution'
