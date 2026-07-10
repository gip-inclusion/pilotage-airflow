select
    id,
    rdv_solidarites_motif_category_id,
    name,
    short_name,
    motif_category_type,
    template_id
from {{ source('rdv_insertion', 'motif_categories') }}
