select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
    



select LO_REVENUE
from homework_db.lineorder_flat
where LO_REVENUE is null



      
    ) as dbt_internal_test