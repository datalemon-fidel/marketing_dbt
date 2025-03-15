--Hubspot_Leads_AdsAccounts_Consolidated_Table.sql

{{
    config(
        materialized='table'  
    )
}}

WITH filtered_hubspot_leads AS (
  SELECT 
    hl.* EXCEPT (Date, Retained_Date),
    hl.Date AS Created_Date,
    SAFE_CAST(hl.Retained_Date AS DATE) AS Retained_Date
  FROM {{ source('stg', 'LLA_Hubspot_Leads') }} AS hl
  WHERE REGEXP_CONTAINS(LOWER(hl.Source_Traffic), r'facebook|google|youtube|tiktok')
    AND NOT REGEXP_CONTAINS(LOWER(hl.Source_Traffic), r'organic')
),

retained_leads AS (
  SELECT
    Created_Date,
    Retained_Date
  FROM filtered_hubspot_leads
  WHERE Contact_lead_status = 'Retained'
),

date_limits AS (
  SELECT 
    MIN(dt) AS start_date,
    MAX(dt) AS end_date
  FROM (
    SELECT SAFE_CAST(Created_Date AS DATE) AS dt FROM filtered_hubspot_leads
    UNION ALL SELECT SAFE_CAST(Retained_Date AS DATE) FROM filtered_hubspot_leads
    UNION ALL SELECT SAFE_CAST(Date AS DATE) FROM {{ source('stg', 'LLA_FacebookAds') }}
    UNION ALL SELECT SAFE_CAST(Date AS DATE) FROM {{ source('stg', 'LLA_GoogleAds') }}
    UNION ALL SELECT SAFE_CAST(Date AS DATE) FROM {{ source('stg', 'LLA_TikTokAds') }}
    UNION ALL SELECT SAFE_CAST(Date AS DATE) FROM {{ source('stg', 'LLA_YouTubeAds') }}
  )
),

all_dates AS (
  SELECT 
    DATE_ADD(start_date, INTERVAL n DAY) AS Aggregation_Date
  FROM date_limits, 
  UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
),

ads_costs AS (
  SELECT 
    SAFE_CAST(Date AS DATE) AS Aggregation_Date,
    SUM(Total_Cost) AS Total_Ad_Cost
  FROM (
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'LLA_FacebookAds') }}
    UNION ALL
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'LLA_GoogleAds') }}
    UNION ALL
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'LLA_TikTokAds') }}
    UNION ALL
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'LLA_YouTubeAds') }}
  )
  GROUP BY 1
),

leads_created_metrics AS (
  SELECT
    SAFE_CAST(Created_Date AS DATE) AS Aggregation_Date,
    COUNTIF(SAFE_CAST(Jot_Form_Date AS DATE) IS NULL) AS Monthly_Leads,
    COUNTIF(_New__Marketing_Lead_Status = 'Qualified') AS Monthly_Qualified_Leads,
    COUNTIF(
      Contact_lead_status = 'Retained'
      AND FORMAT_DATE('%Y-%m', SAFE_CAST(Retained_Date AS DATE)) = FORMAT_DATE('%Y-%m', SAFE_CAST(Created_Date AS DATE))
    ) AS In_Period_Retained,
    COUNTIF(
      Contact_lead_status = 'Retained' 
      AND DATE_DIFF(SAFE_CAST(Retained_Date AS DATE), SAFE_CAST(Created_Date AS DATE), DAY) BETWEEN -1 AND 61
    ) AS Rolling_Window_Retained
  FROM filtered_hubspot_leads
  GROUP BY 1
),

leads_retained_metrics AS (
  SELECT
    SAFE_CAST(Retained_Date AS DATE) AS Aggregation_Date,
    COUNT(1) AS Retained_that_Month
  FROM filtered_hubspot_leads
  WHERE Contact_lead_status = 'Retained'
  GROUP BY 1
),

rolling_retained AS (
  SELECT
    ad.Aggregation_Date,
    COUNTIF(
      rl.Created_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 59 DAY) AND ad.Aggregation_Date
      AND rl.Retained_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 59 DAY) AND ad.Aggregation_Date
    ) AS Rolling_60_Retained,
    COUNTIF(
      rl.Created_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 89 DAY) AND ad.Aggregation_Date
      AND rl.Retained_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 89 DAY) AND ad.Aggregation_Date
    ) AS Rolling_90_Retained,
    COUNTIF(
      rl.Created_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 364 DAY) AND ad.Aggregation_Date
      AND rl.Retained_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 364 DAY) AND ad.Aggregation_Date
    ) AS Rolling_365_Retained
  FROM all_dates ad
  LEFT JOIN retained_leads rl
    ON rl.Retained_Date BETWEEN DATE_SUB(ad.Aggregation_Date, INTERVAL 364 DAY) AND ad.Aggregation_Date
  GROUP BY 1
),

base_data AS (
  SELECT
    ad.Aggregation_Date,
    COALESCE(lc.Monthly_Leads, 0) AS Monthly_Leads,
    COALESCE(lc.Monthly_Qualified_Leads, 0) AS Monthly_Qualified_Leads,
    COALESCE(lc.In_Period_Retained, 0) AS In_Period_Retained,
    COALESCE(lc.Rolling_Window_Retained, 0) AS Rolling_Window_Retained,
    COALESCE(lr.Retained_that_Month, 0) AS Retained_that_Month,
    COALESCE(rr.Rolling_60_Retained, 0) AS Rolling_60_Retained,
    COALESCE(rr.Rolling_90_Retained, 0) AS Rolling_90_Retained,
    COALESCE(rr.Rolling_365_Retained, 0) AS Rolling_365_Retained,
    COALESCE(ac.Total_Ad_Cost, 0) AS Total_Ad_Cost,
    UNIX_DATE(ad.Aggregation_Date) AS aggregation_date_num
  FROM all_dates AS ad
  LEFT JOIN leads_created_metrics AS lc
    ON ad.Aggregation_Date = lc.Aggregation_Date
  LEFT JOIN leads_retained_metrics AS lr
    ON ad.Aggregation_Date = lr.Aggregation_Date
  LEFT JOIN rolling_retained rr
    ON ad.Aggregation_Date = rr.Aggregation_Date
  LEFT JOIN ads_costs AS ac
    ON ad.Aggregation_Date = ac.Aggregation_Date
),

window_calculations AS (
  SELECT
    *,
    SUM(Total_Ad_Cost) OVER annual AS Annual_Ad_Spend,
    SUM(Monthly_Leads) OVER annual AS Annual_Leads,
    SUM(Monthly_Qualified_Leads) OVER annual AS Annual_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER annual,
      SUM(Monthly_Qualified_Leads) OVER annual
    ) AS Annual_CPQL,
    SUM(Retained_that_Month) OVER annual AS Annual_Retained,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER annual,
      SUM(In_Period_Retained) OVER annual
    ) AS Annual_CPA,

    SUM(Total_Ad_Cost) OVER rolling_60d AS Rolling_60_Ad_Spend,
    SUM(Monthly_Leads) OVER rolling_60d AS Rolling_60_Leads,
    SUM(Monthly_Qualified_Leads) OVER rolling_60d AS Rolling_60_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_60d,
      SUM(Monthly_Qualified_Leads) OVER rolling_60d
    ) AS Rolling_60_CPQL,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_60d,
      SUM(In_Period_Retained) OVER rolling_60d
    ) AS Rolling_60_CPA,

    SUM(Total_Ad_Cost) OVER rolling_90d AS Rolling_90_Ad_Spend,
    SUM(Monthly_Leads) OVER rolling_90d AS Rolling_90_Leads,
    SUM(Monthly_Qualified_Leads) OVER rolling_90d AS Rolling_90_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_90d,
      SUM(Monthly_Qualified_Leads) OVER rolling_90d
    ) AS Rolling_90_CPQL,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_90d,
      SUM(In_Period_Retained) OVER rolling_90d
    ) AS Rolling_90_CPA,

    SUM(Total_Ad_Cost) OVER rolling_365d AS Rolling_365_Ad_Spend,
    SUM(Monthly_Leads) OVER rolling_365d AS Rolling_365_Leads,
    SUM(Monthly_Qualified_Leads) OVER rolling_365d AS Rolling_365_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_365d,
      SUM(Monthly_Qualified_Leads) OVER rolling_365d
    ) AS Rolling_365_CPQL,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_365d,
      SUM(In_Period_Retained) OVER rolling_365d
    ) AS Rolling_365_CPA
  FROM base_data
  WINDOW
    annual AS (
      PARTITION BY EXTRACT(YEAR FROM Aggregation_Date) 
      ORDER BY Aggregation_Date
    ),
    rolling_60d AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
    ),
    rolling_90d AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ),
    rolling_365d AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
    )
)

SELECT 
  Aggregation_Date,
  Total_Ad_Cost,
  Annual_Ad_Spend,
  Annual_Leads,
  Annual_Qualified_Leads,
  Annual_CPQL,
  Annual_Retained,
  Annual_CPA,
  Rolling_60_Ad_Spend,
  Rolling_60_Leads,
  Rolling_60_Qualified_Leads,
  Rolling_60_CPQL,
  Rolling_60_CPA,
  Rolling_90_Ad_Spend,
  Rolling_90_Leads,
  Rolling_90_Qualified_Leads,
  Rolling_90_CPQL,
  Rolling_90_CPA,
  Rolling_365_Ad_Spend,
  Rolling_365_Leads,
  Rolling_365_Qualified_Leads,
  Rolling_365_CPQL,
  Rolling_365_CPA,
  Monthly_Leads,
  Monthly_Qualified_Leads,
  In_Period_Retained,
  Rolling_Window_Retained,
  Retained_that_Month,
  Rolling_60_Retained,
  Rolling_90_Retained,
  Rolling_365_Retained
FROM window_calculations
WHERE Aggregation_Date IS NOT NULL
ORDER BY Aggregation_Date