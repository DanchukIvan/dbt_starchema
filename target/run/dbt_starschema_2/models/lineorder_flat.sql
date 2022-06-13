
      

  
    create table homework_db.lineorder_flat
    
  
    
    engine = MergeTree()
    order by (LO_ORDERDATE,LO_ORDERKEY)
    
    partition by (toYear(LO_ORDERDATE))
  as (
    

    
    
    
    
        
        
            
        
    
        
        
            
        
    
        
        
            
        
    
        
        
            
        
    
        
        
    SELECT
      MD5(cast(coalesce(cast(LO_CUSTKEY as 
    String
), '') || '-' || coalesce(cast(C_CUSTKEY as 
    String
), '') || '-' || coalesce(cast(S_SUPPKEY as 
    String
), '') as 
    String
)) As Surrogate_key
      ,"LO_ORDERKEY",
  "LO_LINENUMBER",
  "LO_CUSTKEY",
  "LO_PARTKEY",
  "LO_SUPPKEY",
  "LO_ORDERDATE",
  "LO_ORDERPRIORITY",
  "LO_SHIPPRIORITY",
  "LO_QUANTITY",
  "LO_EXTENDEDPRICE",
  "LO_ORDTOTALPRICE",
  "LO_DISCOUNT",
  "LO_REVENUE",
  "LO_SUPPLYCOST",
  "LO_TAX",
  "LO_COMMITDATE",
  "LO_SHIPMODE"
      ,"C_CUSTKEY",
  "C_NAME",
  "C_ADDRESS",
  "C_CITY",
  "C_NATION",
  "C_REGION",
  "C_PHONE",
  "C_MKTSEGMENT"
      ,"S_SUPPKEY",
  "S_NAME",
  "S_ADDRESS",
  "S_CITY",
  "S_NATION",
  "S_REGION",
  "S_PHONE"
      ,"P_PARTKEY",
  "P_NAME",
  "P_MFGR",
  "P_CATEGORY",
  "P_BRAND",
  "P_COLOR",
  "P_TYPE",
  "P_SIZE",
  "P_CONTAINER"
  FROM homework_db.src_lineorder AS l
  INNER JOIN homework_db.src_customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
  INNER JOIN homework_db.src_supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
  INNER JOIN homework_db.src_part AS p ON p.P_PARTKEY = l.LO_PARTKEY
  
  )
  