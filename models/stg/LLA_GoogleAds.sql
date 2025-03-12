{{ 
  config(
    materialized='table'
  ) 
}}

SELECT Date, Total_Cost
FROM `rare-guide-433209-e6.AdAccounts.Google Ads`
WHERE Date IS NOT NULL
