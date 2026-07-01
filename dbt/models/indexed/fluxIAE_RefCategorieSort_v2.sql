select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_RefCategorieSort')) }}
from
    {{ source('fluxIAE', 'fluxIAE_RefCategorieSort') }}
