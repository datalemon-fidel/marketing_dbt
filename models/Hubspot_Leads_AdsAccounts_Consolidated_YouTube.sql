--hubspot_leads_ads_accounts_consolidated_youtube.sql

{{ config(
    materialized='table'  
) }}

WITH base_data AS (
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
    IFNULL(ya.Total_Cost, 0) AS YouTubeAds_Cost
  FROM
    `rare-guide-433209-e6.AdAccounts.Hubspot_Leads` AS hl
  LEFT JOIN
    `rare-guide-433209-e6.AdAccounts.YouTube Ads` AS ya
  ON
    hl.Date = ya.Date
  WHERE
    hl.Source_Traffic = 'YouTube'
  GROUP BY
    hl.Date, ya.Total_Cost
)

SELECT
  *,
  -- Annual Metrics
  SUM(YouTubeAds_Cost) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Ad_Spend,
  SUM(Monthly_Leads) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Leads,
  SUM(Monthly_Qualified_Leads) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Qualified_Leads,
  SAFE_DIVIDE(
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
  ) AS Annual_CPQL,
  SUM(Retained_that_Month) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Retained,
  SAFE_DIVIDE(
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
    SUM(In_Period_Retained) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
  ) AS Annual_CPA,

  -- Rolling 60-Day Metrics
  SUM(YouTubeAds_Cost) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Ad_Spend,
  SUM(Monthly_Leads) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Leads,
  SUM(Monthly_Qualified_Leads) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Qualified_Leads,
  SAFE_DIVIDE(
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
  ) AS Rolling_60_CPQL,
  SUM(Retained_that_Month) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Retained,
  SAFE_DIVIDE(
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    SUM(In_Period_Retained) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
  ) AS Rolling_60_CPA,

  -- Rolling 365-Day Metrics
  SUM(YouTubeAds_Cost) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Ad_Spend,
  SUM(Monthly_Leads) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Leads,
  SUM(Monthly_Qualified_Leads) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Qualified_Leads,
  SAFE_DIVIDE(
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
  ) AS Rolling_365_CPQL,
  SUM(Retained_that_Month) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Retained,
  SAFE_DIVIDE(
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
    SUM(In_Period_Retained) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
  ) AS Rolling_365_CPA

FROM
  base_data
ORDER BY
  Date