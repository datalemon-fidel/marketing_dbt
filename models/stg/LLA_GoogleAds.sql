{{ 
  config(
    materialized='table'
  ) 
}}

SELECT *
FROM `rare-guide-433209-e6.AdAccounts.Google Ads`
WHERE Date IS NOT NULL
