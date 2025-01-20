--Hubspot_Leads_Campaign_Ads.sql

{{ config(
    materialized='table'  
) }}

WITH base_data AS (
  SELECT
    Date,
    Source_Campaign, 
    Source_Ad,
    Source_Adset, 
    Last_Keywords,
    COUNT(CASE 
            WHEN (Jot_Form_Date IS NULL OR Jot_Form_Date = '') 
                 AND LOWER(Source_Traffic) NOT LIKE '%organic%' 
            THEN 1 
          END) AS Total_Leads,
    COUNT(CASE 
            WHEN LOWER(Source_Traffic) NOT LIKE '%organic%' 
                 AND _New__Marketing_Lead_Status = 'Qualified' 
            THEN 1 
          END) AS Qualified_Leads,
    COUNT(CASE 
            WHEN LOWER(Source_Traffic) NOT LIKE '%organic%' 
                 AND Contact_lead_status = 'Retained'
                 AND FORMAT_DATE('%Y-%m', Retained_Date) = FORMAT_DATE('%Y-%m', Date) 
            THEN 1 
          END) AS Total_Retained,
    COUNT(CASE 
            WHEN LOWER(Source_Traffic) NOT LIKE '%organic%'
                 AND Contact_lead_status = 'Retained' 
                 AND DATE_DIFF(Retained_Date, Date, DAY) <= 60 
                 AND DATE_DIFF(Retained_Date, Date, DAY) >= 0
            THEN 1 
          END) AS Rolling_60_Retained,
    COUNT(CASE 
            WHEN LOWER(Source_Traffic) NOT LIKE '%organic%'
                 AND Contact_lead_status = 'Retained' 
                 AND DATE_DIFF(Retained_Date, Date, DAY) <= 365 
                 AND DATE_DIFF(Retained_Date, Date, DAY) >= 0
            THEN 1 
          END) AS Rolling_365_Retained
  FROM
    `rare-guide-433209-e6.AdAccounts.Hubspot_Leads` 
  GROUP BY 1,2,3,4,5
)

SELECT
  *
FROM
  base_data
ORDER BY
  Date
