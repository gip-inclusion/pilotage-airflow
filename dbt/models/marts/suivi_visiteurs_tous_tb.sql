select
    {{ pilo_star(ref('stg_visiteurs_prives'), relation_alias='svtp') }}
from
    {{ ref('stg_visiteurs_prives') }} as svtp
where
    svtp.semaine is not null
union all
select
    {{ pilo_star(ref('stg_visiteurs_prives_v1'), relation_alias='svtpv1') }}
from
    {{ ref('stg_visiteurs_prives_v1') }} as svtpv1
where
    svtpv1.semaine is not null
union all
select
    {{ pilo_star(ref('stg_visiteurs_publics'), relation_alias='svtpb') }}
from
    {{ ref('stg_visiteurs_publics') }} as svtpb
where
    svtpb.semaine is not null
union all
select
    {{ pilo_star(ref('stg_c1_private_dashboards_visits_v1'), relation_alias='cp') }}
from
    {{ ref('stg_c1_private_dashboards_visits_v1') }} as cp
where
    cp.semaine is not null
