select distinct on (id_structure_asp)
    date_saisie,
    id_structure_asp,
    af_etp_postes_insertion
from "saisies_mensuelles_iae"
order by id_structure_asp, date_saisie desc
