with type_seed as (
    select * from {{ ref('types') }}
)

select

    cast(`Type ID` as int64) as type_id, --PK
    cast(`Type` as string) as type_code,
    cast(`Type Name` as string) as type_description

from type_seed

