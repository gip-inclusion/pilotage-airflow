select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_RefMotifSort')) }}
from
    {{ source('fluxIAE', 'fluxIAE_RefMotifSort') }}
