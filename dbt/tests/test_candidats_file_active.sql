-- if there is a date_derniere_candidature_acceptee, the candidate is not in the file active
select id
from candidats_recherche_active
where file_active_30_jours = true and date_derniere_candidature_acceptee is not null
