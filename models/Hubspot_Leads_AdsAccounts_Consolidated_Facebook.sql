{{ config(
    materialized='table'  
) }}

SELECT
  hl.Date,
  COUNT(CASE 
          WHEN (hl.Jot_Form_Date IS NULL OR hl.Jot_Form_Date = '') 
               AND LOWER(hl.Source_Traffic) NOT LIKE '%organic%' 
          THEN 1 
        END) AS Monthly_Leads,
  COUNT(CASE 
          WHEN LOWER(hl.Source_Traffic) NOT LIKE '%organic%' 
               AND hl._New__Marketing_Lead_Status = 'Qualified' 
          THEN 1 
        END) AS Monthly_Qualified_Leads,
  COUNT(CASE 
          WHEN LOWER(hl.Source_Traffic) NOT LIKE '%organic%' 
               AND hl.Contact_lead_status = 'Retained'
               AND FORMAT_DATE('%Y-%m', hl.Retained_Date) = FORMAT_DATE('%Y-%m', hl.Date) 
          THEN 1 
        END) AS In_Period_Retained,
  IFNULL(fa.Total_Cost, 0) AS FacebookAds_Cost
FROM
  `rare-guide-433209-e6.AdAccounts.Hubspot_Leads` AS hl
LEFT JOIN
  `rare-guide-433209-e6.AdAccounts.Facebook Ads` AS fa
ON
  hl.Date = fa.Date
WHERE
  hl.Source_Traffic = 'Facebook' 
GROUP BY
  hl.Date, FacebookAds_Cost
ORDER BY
  hl.Date;
