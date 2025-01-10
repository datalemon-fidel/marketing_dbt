--hubspot_leads_ads_accounts_consolidated_table.sql
{{ config(
    materialized='table'  
) }}

SELECT
  hl.Date,
  hl.Retained_Date,
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
  COUNT(CASE 
          WHEN LOWER(hl.Source_Traffic) NOT LIKE '%organic%'
               AND hl.Contact_lead_status = 'Retained' 
               AND DATE_DIFF(hl.Retained_Date, hl.Date, DAY) <= 60 
               AND DATE_DIFF(hl.Retained_Date, hl.Date, DAY) >= 0
          THEN 1 
        END) AS Rolling_Window_Retained,
  COUNT(CASE
          WHEN hl.Date >= DATE('2024-01-01')
               AND LOWER(hl.Source_Traffic) NOT LIKE '%organic%'
               AND hl.Contact_lead_status = 'Retained'
          THEN 1
        END) AS Retained_that_Month,
  IFNULL(fa.Total_Cost, 0) AS FacebookAds_Cost,
  IFNULL(ga.Total_Cost, 0) AS GoogleAds_Cost,
  IFNULL(ta.Total_Cost, 0) AS TikTokAds_Cost,
  IFNULL(ya.Total_Cost, 0) AS YouTubeAds_Cost
FROM
  `rare-guide-433209-e6.AdAccounts.Hubspot_Leads` AS hl
LEFT JOIN
  `rare-guide-433209-e6.AdAccounts.Facebook Ads` AS fa
ON
  hl.Date = fa.Date
LEFT JOIN
  `rare-guide-433209-e6.AdAccounts.Google Ads` AS ga
ON
  hl.Date = ga.Date
LEFT JOIN
  `rare-guide-433209-e6.AdAccounts.Tiktok Ads` AS ta
ON
  hl.Date = ta.Date
LEFT JOIN
  `rare-guide-433209-e6.AdAccounts.YouTube Ads` AS ya
ON
  hl.Date = ya.Date
WHERE
  hl.Date IS NOT NULL
GROUP BY
  hl.Date, hl.Retained_Date, FacebookAds_Cost, GoogleAds_Cost, TikTokAds_Cost, YouTubeAds_Cost
ORDER BY
  hl.Date
