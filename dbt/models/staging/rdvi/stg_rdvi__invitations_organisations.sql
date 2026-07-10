select
    invitation_id,
    organisation_id
from {{ source('rdv_insertion', 'invitations_organisations') }}
