-- Rio_Hubspot_Leads_AdsAccounts_Consolidated_YouTube.sql

{{
    config(
        materialized='table'  
    )
}}

WITH filtered_hubspot_leads AS (
  SELECT *
  FROM {{ source('stg', 'Rio_Hubspot_Leads') }}
  WHERE REGEXP_CONTAINS(LOWER(Source_Traffic), r'youtube')
    AND NOT REGEXP_CONTAINS(LOWER(Source_Traffic), r'organic')
    AND REGEXP_CONTAINS(LOWER(Case_Profile), r'employment')  
),

date_scaffold AS (
  SELECT 
    LEAST(
      MIN(hl.Date), 
      MIN(hl.Retained_Date), 
      MIN(fa.Date)
    ) AS start_date,
    GREATEST(
      MAX(hl.Date), 
      MAX(hl.Retained_Date), 
      MAX(fa.Date)
    ) AS end_date
  FROM filtered_hubspot_leads AS hl
  CROSS JOIN {{ source('stg', 'Rio_YouTubeAds') }} AS fa  
),

all_dates AS (
  SELECT 
    DATE_ADD(start_date, INTERVAL n DAY) AS Aggregation_Date
  FROM date_scaffold, 
  UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
),

google_ads_aggregated AS (
  SELECT
    Date AS Aggregation_Date,
    SUM(Total_Cost) AS YouTubeAds_Cost
  FROM {{ source('stg', 'Rio_YouTubeAds') }}  
  GROUP BY Date
),

leads_created_metrics AS (
  SELECT
    Date AS Aggregation_Date,
    COUNT(1) AS Monthly_Leads,  
    COUNT(CASE WHEN Marketing_Lead_Status = 'qualified' THEN 1 END) AS Monthly_Qualified_Leads,  
    COUNT(CASE 
            WHEN Contact_lead_status = 'Retained'
            AND FORMAT_DATE('%Y-%m', Retained_Date) = FORMAT_DATE('%Y-%m', Date) 
            THEN 1 
          END) AS In_Period_Retained,
    COUNT(CASE 
            WHEN Contact_lead_status = 'Retained' 
            AND DATE_DIFF(Retained_Date, Date, DAY) BETWEEN 0 AND 59
            THEN 1 
          END) AS Rolling_Window_Retained
  FROM filtered_hubspot_leads
  GROUP BY Date
),

leads_retained_metrics AS (
  SELECT
    Retained_Date AS Aggregation_Date,
    COUNT(1) AS Retained_that_Month
  FROM filtered_hubspot_leads
  WHERE Contact_lead_status = 'Retained'
  GROUP BY Retained_Date
),

base_data AS (
  SELECT
    ad.Aggregation_Date,
    COALESCE(lc.Monthly_Leads, 0) AS Monthly_Leads,
    COALESCE(lc.Monthly_Qualified_Leads, 0) AS Monthly_Qualified_Leads,
    COALESCE(lc.In_Period_Retained, 0) AS In_Period_Retained,
    COALESCE(lc.Rolling_Window_Retained, 0) AS Rolling_Window_Retained,
    COALESCE(lr.Retained_that_Month, 0) AS Retained_that_Month,
    COALESCE(fa.YouTubeAds_Cost, 0) AS YouTubeAds_Cost,
    UNIX_DATE(ad.Aggregation_Date) AS aggregation_date_num
  FROM all_dates AS ad
  LEFT JOIN google_ads_aggregated AS fa
    ON ad.Aggregation_Date = fa.Aggregation_Date
  LEFT JOIN leads_created_metrics AS lc
    ON ad.Aggregation_Date = lc.Aggregation_Date
  LEFT JOIN leads_retained_metrics AS lr
    ON ad.Aggregation_Date = lr.Aggregation_Date
),

aggregated_metrics AS (
  SELECT
    *,
    SUM(YouTubeAds_Cost) OVER (
      PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
    ) AS Annual_Ad_Spend,
    SUM(Monthly_Leads) OVER (
      PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
    ) AS Annual_Leads,
    SUM(Monthly_Qualified_Leads) OVER (
      PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
    ) AS Annual_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) OVER (
        PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
      ),
      SUM(Monthly_Qualified_Leads) OVER (
        PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
      )
    ) AS Annual_CPQL,
    SUM(Retained_that_Month) OVER (
      PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
    ) AS Annual_Retained,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) OVER (
        PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
      ),
      SUM(In_Period_Retained) OVER (
        PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) ORDER BY Aggregation_Date
      )
    ) AS Annual_CPA,

    -- Rolling 60-Day Metrics
    SUM(YouTubeAds_Cost) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS Rolling_60_Ad_Spend,
    SUM(Monthly_Leads) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS Rolling_60_Leads,
    SUM(Monthly_Qualified_Leads) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS Rolling_60_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
      ),
      SUM(Monthly_Qualified_Leads) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
      )
    ) AS Rolling_60_CPQL,
    SUM(Retained_that_Month) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS Rolling_60_Retained,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
      ),
      SUM(In_Period_Retained) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
      )
    ) AS Rolling_60_CPA,

    -- Rolling 365-Day Metrics
    SUM(YouTubeAds_Cost) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS Rolling_365_Ad_Spend,
    SUM(Monthly_Leads) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS Rolling_365_Leads,
    SUM(Monthly_Qualified_Leads) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS Rolling_365_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
      ),
      SUM(Monthly_Qualified_Leads) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
      )
    ) AS Rolling_365_CPQL,
    SUM(Retained_that_Month) OVER (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS Rolling_365_Retained,
    SAFE_DIVIDE(
      SUM(YouTubeAds_Cost) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
      ),
      SUM(In_Period_Retained) OVER (
        ORDER BY aggregation_date_num
        RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
      )
    ) AS Rolling_365_CPA
  FROM base_data
)

SELECT 
  Aggregation_Date,
  YouTubeAds_Cost,
  Rolling_60_Ad_Spend,
  Rolling_365_Ad_Spend,
  Monthly_Leads,
  Monthly_Qualified_Leads,
  In_Period_Retained,
  Rolling_Window_Retained,
  Retained_that_Month,
  Annual_Ad_Spend,
  Annual_Leads,
  Annual_Qualified_Leads,
  Annual_CPQL,
  Annual_Retained,
  Annual_CPA,
  Rolling_60_Leads,
  Rolling_60_Qualified_Leads,
  Rolling_60_CPQL,
  Rolling_60_Retained,
  Rolling_60_CPA,
  Rolling_365_Leads,
  Rolling_365_Qualified_Leads,
  Rolling_365_CPQL,
  Rolling_365_Retained,
  Rolling_365_CPA
FROM aggregated_metrics
WHERE Aggregation_Date IS NOT NULL
ORDER BY Aggregation_Date