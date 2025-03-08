{{ 
  config(
    materialized='table'
  ) 
}}

SELECT *
FROM `rare-guide-433209-e6.AdAccounts.Rio_Google_Ads`
WHERE Date IS NOT NULL