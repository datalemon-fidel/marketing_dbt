-- Rio_Hubspot_Leads_AdsAccounts_Consolidated_Table.sql

{{
    config(
        materialized='table'  
    )
}}

WITH filtered_hubspot_leads AS (
  SELECT 
    hl.* EXCEPT (Date, Retained_Date),
    hl.Date AS Created_Date,
    hl.Retained_Date
  FROM {{ source('stg', 'Rio_Hubspot_Leads') }} AS hl
  WHERE REGEXP_CONTAINS(LOWER(hl.Source_Traffic), r'facebook|google|youtube')
    AND NOT REGEXP_CONTAINS(LOWER(hl.Source_Traffic), r'organic')
    AND REGEXP_CONTAINS(LOWER(hl.Case_Profile), r'employment')
),

date_limits AS (
  SELECT 
    MIN(dt) AS start_date,
    MAX(dt) AS end_date
  FROM (
    SELECT Created_Date AS dt FROM filtered_hubspot_leads
    UNION ALL SELECT Retained_Date FROM filtered_hubspot_leads
    UNION ALL SELECT Date FROM {{ source('stg', 'Rio_FacebookAds') }}
    UNION ALL SELECT Date FROM {{ source('stg', 'Rio_GoogleAds') }}
    UNION ALL SELECT Date FROM {{ source('stg', 'Rio_YouTubeAds') }}
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
    Date AS Aggregation_Date,
    SUM(Total_Cost) AS Total_Ad_Cost
  FROM (
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'Rio_FacebookAds') }}
    UNION ALL
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'Rio_GoogleAds') }}
    UNION ALL
    SELECT Date, Total_Cost 
    FROM {{ source('stg', 'Rio_YouTubeAds') }}
  )
  GROUP BY 1
),

leads_created_metrics AS (
  SELECT
    Created_Date AS Aggregation_Date,
    COUNT(1) AS Monthly_Leads,
    COUNTIF(Marketing_Lead_Status = 'qualified') AS Monthly_Qualified_Leads,
    COUNTIF(
      Contact_lead_status = 'Retained'
      AND FORMAT_DATE('%Y-%m', Retained_Date) = FORMAT_DATE('%Y-%m', Created_Date)
    ) AS In_Period_Retained,
    COUNTIF(
      Contact_lead_status = 'Retained' 
      AND DATE_DIFF(Retained_Date, Created_Date, DAY) BETWEEN -1 AND 61
    ) AS Rolling_Window_Retained
  FROM filtered_hubspot_leads
  GROUP BY 1
),

leads_retained_metrics AS (
  SELECT
    Retained_Date AS Aggregation_Date,
    COUNT(1) AS Retained_that_Month
  FROM filtered_hubspot_leads
  WHERE Contact_lead_status = 'Retained'
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
    COALESCE(ac.Total_Ad_Cost, 0) AS Total_Ad_Cost,
    UNIX_DATE(ad.Aggregation_Date) AS aggregation_date_num
  FROM all_dates AS ad
  LEFT JOIN leads_created_metrics AS lc USING (Aggregation_Date)
  LEFT JOIN leads_retained_metrics AS lr USING (Aggregation_Date)
  LEFT JOIN ads_costs AS ac USING (Aggregation_Date)
),

window_calculations AS (
  SELECT
    *,
    -- Annual calculations
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

    -- 60-day rolling calculations
    SUM(Total_Ad_Cost) OVER rolling_60d AS Rolling_60_Ad_Spend,
    SUM(Monthly_Leads) OVER rolling_60d AS Rolling_60_Leads,
    SUM(Monthly_Qualified_Leads) OVER rolling_60d AS Rolling_60_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_60d,
      SUM(Monthly_Qualified_Leads) OVER rolling_60d
    ) AS Rolling_60_CPQL,
    SUM(Retained_that_Month) OVER rolling_60d_retained AS Rolling_60_Retained,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_60d,
      SUM(In_Period_Retained) OVER rolling_60d
    ) AS Rolling_60_CPA,

    -- 90-day rolling calculations
    SUM(Total_Ad_Cost) OVER rolling_90d AS Rolling_90_Ad_Spend,
    SUM(Monthly_Leads) OVER rolling_90d AS Rolling_90_Leads,
    SUM(Monthly_Qualified_Leads) OVER rolling_90d AS Rolling_90_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_90d,
      SUM(Monthly_Qualified_Leads) OVER rolling_90d
    ) AS Rolling_90_CPQL,
    SUM(Retained_that_Month) OVER rolling_90d_retained AS Rolling_90_Retained,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_90d,
      SUM(In_Period_Retained) OVER rolling_90d
    ) AS Rolling_90_CPA,

    -- 365-day rolling calculations
    SUM(Total_Ad_Cost) OVER rolling_365d AS Rolling_365_Ad_Spend,
    SUM(Monthly_Leads) OVER rolling_365d AS Rolling_365_Leads,
    SUM(Monthly_Qualified_Leads) OVER rolling_365d AS Rolling_365_Qualified_Leads,
    SAFE_DIVIDE(
      SUM(Total_Ad_Cost) OVER rolling_365d,
      SUM(Monthly_Qualified_Leads) OVER rolling_365d
    ) AS Rolling_365_CPQL,
    SUM(Retained_that_Month) OVER rolling_365d_retained AS Rolling_365_Retained,
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
    rolling_60d_retained AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 60 PRECEDING AND 1 FOLLOWING
    ),
    rolling_90d AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ),
    rolling_90d_retained AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 90 PRECEDING AND 1 FOLLOWING
    ),
    rolling_365d AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 364 PRECEDING AND CURRENT ROW
    ),
    rolling_365d_retained AS (
      ORDER BY aggregation_date_num
      RANGE BETWEEN 365 PRECEDING AND 1 FOLLOWING
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
  Rolling_60_Retained,
  Rolling_60_CPA,
  Rolling_90_Ad_Spend,
  Rolling_90_Leads,
  Rolling_90_Qualified_Leads,
  Rolling_90_CPQL,
  Rolling_90_Retained,
  Rolling_90_CPA,
  Rolling_365_Ad_Spend,
  Rolling_365_Leads,
  Rolling_365_Qualified_Leads,
  Rolling_365_CPQL,
  Rolling_365_Retained,
  Rolling_365_CPA,
  Monthly_Leads,
  Monthly_Qualified_Leads,
  In_Period_Retained,
  Rolling_Window_Retained,
  Retained_that_Month
FROM window_calculations
WHERE Aggregation_Date IS NOT NULL
ORDER BY Aggregation_Date