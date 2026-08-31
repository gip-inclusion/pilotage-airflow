select
    orientation_id,
    emplois_sync_uid,
    beneficiary_id,
    structure_id,
    structure_name,
    structure_siret,
    prescriber_id,
    prescriber_email,
    prescriber_first_name,
    prescriber_last_name,
    prescriber_phone
from {{ source('dora', 'orientations_emploisorientationdata') }}
