{{ 
  config(
    materialized='table'
  ) 
}}

SELECT *
FROM `rare-guide-433209-e6.AdAccounts.YouTube Ads`
WHERE Date IS NOT NULL