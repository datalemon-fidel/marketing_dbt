--hubspot_leads_ads_accounts_consolidated_youtube.sql

{{ config(
    materialized='table'  
) }}

WITH filtered_hubspot_leads AS (
  -- Filter for rows where Source_Traffic contains "YouTube" and excludes "Organic"
  SELECT *
  FROM `rare-guide-433209-e6.AdAccounts.Hubspot_Leads`
  WHERE REGEXP_CONTAINS(LOWER(Source_Traffic), r'youtube')
    AND NOT REGEXP_CONTAINS(LOWER(Source_Traffic), r'organic')
),

date_scaffold AS (
  -- Get the minimum and maximum dates from Created Date, Retained Date, and Ad Date
  SELECT 
    LEAST(MIN(hl.Date), MIN(hl.Retained_Date), MIN(fa.Date)) AS start_date, 
    GREATEST(MAX(hl.Date), MAX(hl.Retained_Date), MAX(fa.Date)) AS end_date
  FROM 
    filtered_hubspot_leads AS hl
  FULL OUTER JOIN 
    `rare-guide-433209-e6.AdAccounts.YouTube Ads` AS fa
  ON 
    hl.Date = fa.Date
),

all_dates AS (
  -- Generate a complete date range
  SELECT 
    DATE_ADD(start_date, INTERVAL n DAY) AS Aggregation_Date
  FROM 
    date_scaffold, 
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
),

youtube_ads_aggregated AS (
  -- Aggregate YouTube Ads Cost to avoid duplication
  SELECT 
    Date AS Aggregation_Date,
    SUM(Total_Cost) AS YouTubeAds_Cost
  FROM `rare-guide-433209-e6.AdAccounts.YouTube Ads`
  GROUP BY Date
),

base_data AS (
  SELECT
    ad.Aggregation_Date,

    -- Monthly Leads based on Created Date
    COUNT(CASE 
            WHEN hl.Date = ad.Aggregation_Date 
                 AND (hl.Jot_Form_Date IS NULL OR hl.Jot_Form_Date = '') 
            THEN 1 
          END) AS Monthly_Leads,
    
    -- Monthly Qualified Leads based on Created Date
    COUNT(CASE 
            WHEN hl.Date = ad.Aggregation_Date 
                 AND hl._New__Marketing_Lead_Status = 'Qualified' 
            THEN 1 
          END) AS Monthly_Qualified_Leads,

    -- In Period Retained: Based on Created Date
    COUNT(CASE 
            WHEN hl.Date = ad.Aggregation_Date 
                 AND hl.Contact_lead_status = 'Retained'
                 AND FORMAT_DATE('%Y-%m', hl.Retained_Date) = FORMAT_DATE('%Y-%m', hl.Date) 
            THEN 1 
          END) AS In_Period_Retained,

    -- Rolling 60-Day Retained: Based on Created Date
    COUNT(CASE 
            WHEN hl.Date = ad.Aggregation_Date 
                 AND hl.Contact_lead_status = 'Retained' 
                 AND DATE_DIFF(hl.Retained_Date, hl.Date, DAY) BETWEEN 0 AND 60
            THEN 1 
          END) AS Rolling_Window_Retained,

    -- Retained_that_Month: Now based on Retained_Date instead of Created Date
    COUNT(CASE
            WHEN hl.Retained_Date = ad.Aggregation_Date 
                 AND hl.Contact_lead_status = 'Retained'
            THEN 1
          END) AS Retained_that_Month,

    -- Ad Spend: Pulled from pre-aggregated table to prevent duplication
    COALESCE(fa.YouTubeAds_Cost, 0) AS YouTubeAds_Cost

  FROM
    all_dates AS ad
  LEFT JOIN
    filtered_hubspot_leads AS hl
  ON
    hl.Date = ad.Aggregation_Date OR hl.Retained_Date = ad.Aggregation_Date  -- Include both dates
  LEFT JOIN
    youtube_ads_aggregated AS fa
  ON
    fa.Aggregation_Date = ad.Aggregation_Date  -- Pre-aggregated Ads join
  GROUP BY
    ad.Aggregation_Date, fa.YouTubeAds_Cost
),

aggregated_metrics AS (
  SELECT
    *,
    
    -- Annual Metrics
    SUM(YouTubeAds_Cost) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date) AS Annual_Ad_Spend,
    SUM(Monthly_Leads) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date) AS Annual_Leads,
    SUM(Monthly_Qualified_Leads) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date) AS Annual_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date),
      SUM(Monthly_Qualified_Leads) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date)
    ) AS Annual_CPQL,

    SUM(Retained_that_Month) 
      OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date) AS Annual_Retained,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date),
      SUM(In_Period_Retained) 
        OVER (PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date)
    ) AS Annual_CPA

  FROM base_data
),

rolling_calculations AS (
  SELECT
    *,
    
    -- Rolling 60-Day Metrics
    (SELECT SUM(YouTubeAds_Cost)
     FROM aggregated_metrics AS sub
     WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                     AND aggregated_metrics.Aggregation_Date) AS Rolling_60_Ad_Spend,

    (SELECT SUM(Monthly_Leads)
     FROM aggregated_metrics AS sub
     WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                     AND aggregated_metrics.Aggregation_Date) AS Rolling_60_Leads,

    (SELECT SUM(Retained_that_Month)
     FROM aggregated_metrics AS sub
     WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                     AND aggregated_metrics.Aggregation_Date) AS Rolling_60_Retained,

    SAFE_DIVIDE(
      (SELECT SUM(YouTubeAds_Cost)
       FROM aggregated_metrics AS sub
       WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                       AND aggregated_metrics.Aggregation_Date),
      (SELECT SUM(Monthly_Qualified_Leads)
       FROM aggregated_metrics AS sub
       WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                       AND aggregated_metrics.Aggregation_Date)
    ) AS Rolling_60_CPQL,

    SAFE_DIVIDE(
      (SELECT SUM(YouTubeAds_Cost)
       FROM aggregated_metrics AS sub
       WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                       AND aggregated_metrics.Aggregation_Date),
      (SELECT SUM(In_Period_Retained)
       FROM aggregated_metrics AS sub
       WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 59 DAY) 
                                       AND aggregated_metrics.Aggregation_Date)
    ) AS Rolling_60_CPA,

    -- Rolling 365-Day Metrics
    (SELECT SUM(YouTubeAds_Cost)
     FROM aggregated_metrics AS sub
     WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 364 DAY) 
                                     AND aggregated_metrics.Aggregation_Date) AS Rolling_365_Ad_Spend,

    (SELECT SUM(Retained_that_Month)
     FROM aggregated_metrics AS sub
     WHERE sub.Aggregation_Date BETWEEN DATE_SUB(aggregated_metrics.Aggregation_Date, INTERVAL 364 DAY) 
                                     AND aggregated_metrics.Aggregation_Date) AS Rolling_365_Retained

  FROM aggregated_metrics
)

SELECT
  *
FROM rolling_calculations
WHERE Aggregation_Date IS NOT NULL
ORDER BY Aggregation_Date