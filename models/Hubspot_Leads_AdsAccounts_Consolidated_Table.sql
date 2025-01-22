--Hubspot_Leads_AdsAccounts_Consolidated_Table.sql

{{ config(
    materialized='table'  
) }}

WITH base_date_scaffold AS (
  -- Combine date ranges from all source tables
  SELECT DISTINCT Date
  FROM (
    SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Hubspot_Leads`
    UNION ALL
    SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Facebook Ads`
    UNION ALL
    SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Google Ads`
    UNION ALL
    SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Tiktok Ads`
    UNION ALL
    SELECT Date FROM `rare-guide-433209-e6.AdAccounts.YouTube Ads`
  )
),
base_data AS (
  SELECT
    ad.Date,
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
    base_date_scaffold AS ad
  LEFT JOIN
    `rare-guide-433209-e6.AdAccounts.Hubspot_Leads` AS hl
  ON
    ad.Date = hl.Date
  LEFT JOIN
    `rare-guide-433209-e6.AdAccounts.Facebook Ads` AS fa
  ON
    ad.Date = fa.Date
  LEFT JOIN
    `rare-guide-433209-e6.AdAccounts.Google Ads` AS ga
  ON
    ad.Date = ga.Date
  LEFT JOIN
    `rare-guide-433209-e6.AdAccounts.Tiktok Ads` AS ta
  ON
    ad.Date = ta.Date
  LEFT JOIN
    `rare-guide-433209-e6.AdAccounts.YouTube Ads` AS ya
  ON
    ad.Date = ya.Date
  GROUP BY
    ad.Date, fa.Total_Cost, ga.Total_Cost, ta.Total_Cost, ya.Total_Cost
),
aggregated_metrics AS (
  SELECT
    *,
    -- Consolidated Metrics
    SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Ad_Spend,
    SUM(Monthly_Leads) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Leads,
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
      SUM(Monthly_Qualified_Leads) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
    ) AS Annual_CPQL,
    SUM(Retained_that_Month) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Retained,
    SAFE_DIVIDE(
      SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
      SUM(In_Period_Retained) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
    ) AS Annual_CPA,

    -- Rolling 60-Day Metrics
    SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Ad_Spend,
    SUM(Monthly_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Leads,
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
      SUM(Monthly_Qualified_Leads) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    ) AS Rolling_60_CPQL,
    SUM(Retained_that_Month) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Retained,
    SAFE_DIVIDE(
      SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
      SUM(In_Period_Retained) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    ) AS Rolling_60_CPA,

    -- Rolling 365-Day Metrics
    SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Ad_Spend,
    SUM(Monthly_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Leads,
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
      SUM(Monthly_Qualified_Leads) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
    ) AS Rolling_365_CPQL,
    SUM(Retained_that_Month) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Retained,
    SAFE_DIVIDE(
      SUM(FacebookAds_Cost + GoogleAds_Cost + TikTokAds_Cost + YouTubeAds_Cost) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
      SUM(In_Period_Retained) 
        OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
    ) AS Rolling_365_CPA
  FROM
    base_data
)
SELECT
  *
FROM
  aggregated_metrics
ORDER BY
  Date
