{{ 
  config(
    materialized='table'
  ) 
}}

SELECT *
FROM `rare-guide-433209-e6.AdAccounts.Hubspot_Leads`
WHERE Date IS NOT NULL
