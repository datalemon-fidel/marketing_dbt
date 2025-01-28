--Hubspot_Leads_AdsAccounts_Consolidated_Table.sql

{{ config(
    materialized='table'  
) }}


WITH filtered_hubspot_leads AS (
  -- Filter rows where Source_Traffic matches the required conditions
  SELECT *
  FROM `rare-guide-433209-e6.AdAccounts.Hubspot_Leads`
  WHERE REGEXP_CONTAINS(LOWER(Source_Traffic), r'facebook|google|youtube|tiktok')
    AND NOT REGEXP_CONTAINS(LOWER(Source_Traffic), r'organic')
),

base_date_scaffold AS (
  -- Combine date ranges from all source tables
  SELECT Date
  FROM `rare-guide-433209-e6.AdAccounts.Hubspot_Leads`
  UNION DISTINCT
  SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Facebook Ads`
  UNION DISTINCT
  SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Google Ads`
  UNION DISTINCT
  SELECT Date FROM `rare-guide-433209-e6.AdAccounts.Tiktok Ads`
  UNION DISTINCT
  SELECT Date FROM `rare-guide-433209-e6.AdAccounts.YouTube Ads`
),

ads_costs AS (
  -- Aggregate ad costs by date
  SELECT
    Date,
    SUM(IFNULL(Total_Cost, 0)) AS Total_Ad_Cost
  FROM (
    SELECT Date, Total_Cost FROM `rare-guide-433209-e6.AdAccounts.Facebook Ads`
    UNION ALL
    SELECT Date, Total_Cost FROM `rare-guide-433209-e6.AdAccounts.Google Ads`
    UNION ALL
    SELECT Date, Total_Cost FROM `rare-guide-433209-e6.AdAccounts.Tiktok Ads`
    UNION ALL
    SELECT Date, Total_Cost FROM `rare-guide-433209-e6.AdAccounts.YouTube Ads`
  )
  GROUP BY Date
),

base_data AS (
  SELECT
    ad.Date,
    COUNT(CASE WHEN hl.Jot_Form_Date IS NULL OR hl.Jot_Form_Date = '' THEN 1 END) AS Monthly_Leads,
    COUNT(CASE WHEN hl._New__Marketing_Lead_Status = 'Qualified' THEN 1 END) AS Monthly_Qualified_Leads,
    COUNT(CASE WHEN hl.Contact_lead_status = 'Retained'
               AND FORMAT_DATE('%Y-%m', hl.Retained_Date) = FORMAT_DATE('%Y-%m', hl.Date) THEN 1 END) AS In_Period_Retained,
    COUNT(CASE WHEN hl.Contact_lead_status = 'Retained'
               AND DATE_DIFF(hl.Retained_Date, hl.Date, DAY) BETWEEN 0 AND 60 THEN 1 END) AS Rolling_Window_Retained,
    COUNT(CASE WHEN hl.Date >= '2024-01-01' AND hl.Contact_lead_status = 'Retained' THEN 1 END) AS Retained_that_Month,
    IFNULL(ac.Total_Ad_Cost, 0) AS Total_Ad_Cost
  FROM base_date_scaffold AS ad
  LEFT JOIN filtered_hubspot_leads AS hl ON ad.Date = hl.Date
  LEFT JOIN ads_costs AS ac ON ad.Date = ac.Date
  GROUP BY ad.Date, ac.Total_Ad_Cost
),

aggregated_metrics AS (
  SELECT
    *,
    -- Consolidated Metrics
    SUM(Total_Ad_Cost) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Ad_Spend,
    SUM(Monthly_Leads) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Leads,
    SUM(Monthly_Qualified_Leads) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
      SUM(Monthly_Qualified_Leads) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
    ) AS Annual_CPQL,
    SUM(Retained_that_Month) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date) AS Annual_Retained,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date),
      SUM(In_Period_Retained) OVER (PARTITION BY EXTRACT(YEAR FROM Date) ORDER BY Date)
    ) AS Annual_CPA,

    -- Rolling 60-Day Metrics
    SUM(Total_Ad_Cost) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Ad_Spend,
    SUM(Monthly_Leads) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Leads,
    SUM(Monthly_Qualified_Leads) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
      SUM(Monthly_Qualified_Leads) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    ) AS Rolling_60_CPQL,
    SUM(Retained_that_Month) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS Rolling_60_Retained,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
      SUM(In_Period_Retained) OVER (ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    ) AS Rolling_60_CPA,

    -- Rolling 365-Day Metrics
    SUM(Total_Ad_Cost) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Ad_Spend,
    SUM(Monthly_Leads) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Leads,
    SUM(Monthly_Qualified_Leads) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
      SUM(Monthly_Qualified_Leads) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
    ) AS Rolling_365_CPQL,
    SUM(Retained_that_Month) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS Rolling_365_Retained,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
      SUM(In_Period_Retained) OVER (ORDER BY Date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
    ) AS Rolling_365_CPA
  FROM base_data
)

SELECT *
FROM aggregated_metrics
WHERE Date IS NOT NULL
ORDER BY Date