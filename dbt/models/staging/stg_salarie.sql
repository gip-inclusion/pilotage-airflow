/* This table is needed to gather the gender of all the distincts employees in the asp flux
Indeed, some employees are referenced more than once which induces a wrong amount of ETP per gender */

select distinct
    salarie.salarie_id,
    salarie.salarie_rci_libelle
from
    {{ source('fluxIAE', 'fluxIAE_Salarie') }} as salarie
