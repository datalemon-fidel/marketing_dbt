--hubspot_leads_ads_accounts_consolidated_facebook.sql

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
    hl.Date, fa.Total_Cost
)

SELECT
  *,
  -- Annual Metrics
  SUM(FacebookAds_Cost) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Ad_Spend,
  SUM(Monthly_Leads) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Leads,
  SUM(Monthly_Qualified_Leads) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Qualified_Leads,
  SAFE_DIVIDE(
    SUM(FacebookAds_Cost) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
  ) AS Annual_CPQL,
  SUM(Retained_that_Month) 
    OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Retained,
  SAFE_DIVIDE(
    SUM(FacebookAds_Cost) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
    SUM(In_Period_Retained) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
  ) AS Annual_CPA,

  -- Rolling 60-Day Metrics
  SUM(FacebookAds_Cost) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Ad_Spend,
  SUM(Monthly_Leads) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Leads,
  SUM(Monthly_Qualified_Leads) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Qualified_Leads,
  SAFE_DIVIDE(
    SUM(FacebookAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
  ) AS Rolling_60_CPQL,
  SUM(Retained_that_Month) 
    OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Retained,
  SAFE_DIVIDE(
    SUM(FacebookAds_Cost) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    SUM(In_Period_Retained) 
      OVER (PARTITION BY NULL ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
  ) AS Rolling_60_CPA

FROM
  base_data
ORDER BY
  Date
