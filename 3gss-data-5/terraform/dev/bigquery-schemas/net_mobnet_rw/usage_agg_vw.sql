with cdm as (
	select
	trim(b.subscriber_num) as msisdn,
    b.agg_mth,
	b.subscriber_status,
	b.current_brand,
	b.business_type,
	b.billing_base,
	b.ACTIVE_30D_PAYG,
	b.ACTIVE_90D_PAYG,
	b.account_num,
  b.account_type,
  b.contract_start_date,
	b.contract_end_date,
  b.arpu,  
	case when b.current_brand = 'BT' then ov.curr_product_code else dpr.product_code end as product_code,
	case when b.current_brand = 'BT' then ov.product_name else dpr.product_description end as product_description,
	case when b.current_brand = 'BT' then ov.product_group else dpr.product_group end as product_group,

-- 	IDENTIFY the PROTOCOL supported by the Tariff
	
	case 	when dpr.protocol = '5G' then '5G'
            when dpr.base_type_level1 = 'PAYG SWITCH' then '3G'
            when dpr.base_type_level1 like 'EE 3G%' then '3G'
            when dpr.product_code in ('X18PYG035','X18PYG036') then '3G'
            when b.current_brand = 'T-Mobile' then '3G'
            when(	dpr.PRODUCT_DESCRIPTION like ('%5G') 
                or 	dpr.PRODUCT_DESCRIPTION like ('%5G %')
                or 	dpr.PRODUCT_DESCRIPTION like ('%5GEE%') 
                or  dpr.product_rollup like '%5G%' ) then '5G'
            when(	(dpr.product_code like 'X2%' or dpr.product_code like 'XB2%') and billing_base = 'PAYM' and dpr.product_group = 'Handset') then '5G'
            when(	(dpr.product_code like 'X2%' or dpr.product_code like 'XB2%') and billing_base = 'PAYM' and dpr.product_group like 'SIMO%' and 
                    (	upper(dpr.product_description) like 'SMART%SIM%' or
                        upper(dpr.product_description) like '5G SIM%' or
                        upper(dpr.product_description) like 'FULL WORKS%' or
                        upper(dpr.product_description) like '%APPLE%') )		then '5G'		 
            when UPPER(substr(dpr.PRODUCT_DESCRIPTION,1,8)) in ('EE 3G PL','MBB 3G P','EE 3G MB') then '3G'
            when UPPER(dpr.PRODUCT_DESCRIPTION) in ('EE SPECIAL RATES (3G)','EE STANDARD RATES (3G)') then '3G' 
            when b.current_brand = 'EE' then '4G'
            when b.current_brand = 'BT' then mobile_data_speed
            else 'Other' end as tar_p2,

-- 	base_type augmented by Watch, Wifi and FWA type tariffs	

  CASE
    WHEN UPPER(SUBSTR(dpr.product_description, 3, 5)) IN ('EE HR', 'EE HO', 'EE RO', 'ME BR') THEN 'FWA'
    WHEN UPPER(dpr.product_description) LIKE '%WIFI%' THEN 'Wifi'
    WHEN UPPER(dpr.product_code) LIKE '%WEA%' THEN 'Watch'
    WHEN UPPER(dpr.product_code) LIKE '%WATCH%' THEN 'Watch'
    WHEN UPPER(dpr.product_description) LIKE '%WEA%' THEN 'Watch'
    WHEN UPPER(dpr.product_description) LIKE '%WATCH%' THEN 'Watch'
    when b.current_brand = 'BT' THEN ov.mobile_service_type
  ELSE
  dpr.base_type
END
  AS base_type,

RANK() OVER (PARTITION BY b.agg_mth, b.subscriber_num ORDER BY contract_start_date) AS r
FROM
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.bi_ccm_agg_mth` AS b

LEFT JOIN
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.ref_cdm_product_excal` dpr
ON
  trim(b.price_plan) = trim(dpr.product_code)
  AND PARSE_DATE(
    '%Y%m',agg_mth) BETWEEN effective_date
  AND expiration_date

  left join
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.ref_cdm_product_ov` ov
ON
  trim(b.price_plan) = trim(ov.curr_product_code)
  AND PARSE_DATE(
    '%Y%m',agg_mth) BETWEEN ov.start_date
  AND ov.end_date


  WHERE
  cast(agg_mth as int64) BETWEEN CAST(FORMAT_DATE("%Y%m",date_sub(current_date(), interval 13 month)) AS INT64) AND CAST (FORMAT_DATE("%Y%m",current_date()) AS INT64)
  AND subscriber_status IN ('A',
    'S')
  -- AND ((billing_base = 'PAYG'
  --     -- AND ACTIVE_90D_PAYG = 1)
  --   OR billing_base <> 'PAYG')
  AND business_type = 'Consumer'
),


msisdns_tech AS (
  SELECT
    DISTINCT trim(b.msisdn) as msisdn,
    b.month_id,
    b.month_date,
    
  CASE
    WHEN dcd.technology LIKE '2G%' THEN '2G'
    WHEN dcd.technology LIKE '3G%' THEN '3G'
    WHEN dcd.cell LIKE '_1' THEN '3G'
    WHEN dcd.cell LIKE '_2' THEN '3G'
    WHEN dcd.technology = 'FEMTO' THEN '3G'
    WHEN dcd.technology = '4G' THEN '4G'
    WHEN dcd.global_cell_id LIKE '304%' THEN '4G'
    WHEN dcd.global_cell_id LIKE '305%' THEN '5G'
    WHEN dcd.technology = 'WIFI' THEN 'WIFI'
    WHEN dcd.technology = '-1'
  AND SUBSTR(dcd.global_cell_id,1,3) IN ('300',
    '301',
    '330') THEN '2G'
    WHEN dcd.technology = '-1' AND dcd.global_cell_id LIKE '302%' THEN '3G'
  ELSE
  'Other'
END
  AS interp_tech,

  SUM(b.home_mo_count + b.work_mo_count) AS mo_calls,
  SUM(b.home_mt_count + b.work_mt_count) AS mt_calls,
  SUM(b.home_mo_duration_secs + b.work_mo_duration_secs)/60.0 AS mo_mins,
  SUM(b.home_mt_duration_secs + b.work_mt_duration_secs)/60.0 AS mt_mins,
  SUM(b.home_mo_sms_count + b.work_mo_sms_count) AS mo_sms,
  SUM(b.home_mt_sms_count + b.work_mt_sms_count) AS mt_sms,
  SUM(b.home_data_in_volume_kb + b.work_data_in_volume_kb)/1024.0 AS dl_mb,
  SUM(b.home_data_out_volume_kb + b.work_data_out_volume_kb)/1024.0 AS ul_mb
FROM
  `cdm`

left JOIN
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.t_agg_mth_subscriber_usage` b
ON
  trim(b.msisdn) = trim(cdm.msisdn)
 and b.month_date = parse_date('%Y%m%d',concat(cdm.agg_mth,'01'))

LEFT JOIN
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.dim_cell_data` dcd
ON
  b.first_cell_data_id = dcd.cell_data_id
WHERE
   b.month_date BETWEEN date_sub(current_date(), interval 13 month)  AND current_date()
  AND cdm.r = 1
GROUP BY
  1,
  2,3,4
 ),
 
--  select 'best' imsi from cdr i.e. drop invalid codes below
 imsi as (
select
month_id,
month_date,
msisdn,
imsi
from 
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.t_agg_mth_subscriber_usage` 

where trim(imsi) not in ('0','234300000000000') 
group by 1,2,3,4
)

SELECT
    case
		  when C.MSISDN not like '0%' then concat('0', C.MSISDN)
		  else C.MSISDN
	end as MSISDN,
    I.IMSI,
    C.AGG_MTH,
    C.SUBSCRIBER_STATUS,
    case when C.current_brand = 'EE' and tar_p2 = '3G' then 'OUK EE 3G' else C.current_brand end as BRAND,
    C.BILLING_BASE,
    C.BUSINESS_TYPE,
    C.TAR_P2 AS TAR_PROTOCOL,
    C.BASE_TYPE,
    C.ACTIVE_30D_PAYG,
    C.ACTIVE_90D_PAYG,
    C.ACCOUNT_TYPE,
    C.ACCOUNT_NUM,
    C.PRODUCT_CODE,
    C.PRODUCT_DESCRIPTION,
    C.PRODUCT_GROUP,
    C.ARPU,
    C.CONTRACT_START_DATE,
    C.CONTRACT_END_DATE,    

    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.MO_CALLS
      ELSE 0
    END) AS MO_CALLS_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.MO_CALLS
      ELSE 0
    END) AS MO_CALLS_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.MO_CALLS
      ELSE 0
    END) AS MO_CALLS_4G,
    sum(CASE
      WHEN T.INTERP_TECH = 'WIFI' THEN T.MO_CALLS
      ELSE 0
    END) AS MO_CALLS_WI,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.MO_CALLS
      ELSE 0
    END) AS MO_CALLS_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.MT_CALLS
      ELSE 0
    END) AS MT_CALLS_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.MT_CALLS
      ELSE 0
    END) AS MT_CALLS_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.MT_CALLS
      ELSE 0
    END) AS MT_CALLS_4G,
    sum(CASE
      WHEN T.INTERP_TECH = 'WIFI' THEN T.MT_CALLS
      ELSE 0
    END) AS MT_CALLS_WI,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.MT_CALLS
      ELSE 0
    END) AS MT_CALLS_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.MO_MINS
      ELSE 0
    END) AS MO_MINS_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.MO_MINS
      ELSE 0
    END) AS MO_MINS_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.MO_MINS
      ELSE 0
    END) AS MO_MINS_4G,
    sum(CASE
      WHEN T.INTERP_TECH = 'WIFI' THEN T.MO_MINS
      ELSE 0
    END) AS MO_MINS_WI,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.MO_MINS
      ELSE 0
    END) AS MO_MINS_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.MT_MINS
      ELSE 0
    END) AS MT_MINS_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.MT_MINS
      ELSE 0
    END) AS MT_MINS_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.MT_MINS
      ELSE 0
    END) AS MT_MINS_4G,
    sum(CASE
      WHEN T.INTERP_TECH = 'WIFI' THEN T.MT_MINS
      ELSE 0
    END) AS MT_MINS_WI,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.MT_MINS
      ELSE 0
    END) AS MT_MINS_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.MO_SMS
      ELSE 0
    END) AS MO_SMS_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.MO_SMS
      ELSE 0
    END) AS MO_SMS_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.MO_SMS
      ELSE 0
    END) AS MO_SMS_4G,
    sum(CASE
      WHEN T.INTERP_TECH = 'WIFI' THEN T.MO_SMS
      ELSE 0
    END) AS MO_SMS_WI,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.MO_SMS
      ELSE 0
    END) AS MO_SMS_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.MT_SMS
      ELSE 0
    END) AS MT_SMS_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.MT_SMS
      ELSE 0
    END) AS MT_SMS_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.MT_SMS
      ELSE 0
    END) AS MT_SMS_4G,
    sum(CASE
      WHEN T.INTERP_TECH = 'WIFI' THEN T.MT_SMS
      ELSE 0
    END) AS MT_SMS_WI,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.MT_SMS
      ELSE 0
    END) AS MT_SMS_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.DL_MB
      ELSE 0
    END) AS DL_MB_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.DL_MB
      ELSE 0
    END) AS DL_MB_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.DL_MB
      ELSE 0
    END) AS DL_MB_4G,
    sum(CASE
      WHEN T.INTERP_TECH = '5G' THEN T.DL_MB
      ELSE 0
    END) AS DL_MB_5G,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.DL_MB
      ELSE 0
    END) AS DL_MB_OTHER,
    sum(CASE
      WHEN T.INTERP_TECH = '2G' THEN T.UL_MB
      ELSE 0
    END) AS UL_MB_2G,
    sum(CASE
      WHEN T.INTERP_TECH = '3G' THEN T.UL_MB
      ELSE 0
    END) AS UL_MB_3G,
    sum(CASE
      WHEN T.INTERP_TECH = '4G' THEN T.UL_MB
      ELSE 0
    END) AS UL_MB_4G,
    sum(CASE
      WHEN T.INTERP_TECH = '5G' THEN T.UL_MB
      ELSE 0
    END) AS UL_MB_5G,
    sum(CASE
      WHEN T.INTERP_TECH = 'Other' THEN T.UL_MB
      ELSE 0
    END) AS UL_MB_OTHER
  FROM CDM AS C
  LEFT OUTER JOIN MSISDNS_TECH AS T ON TRIM(C.MSISDN) = TRIM(T.MSISDN)
  and parse_date('%Y%m%d',concat(C.agg_mth,'01')) = T.month_date
  left outer join imsi AS I ON TRIM(I.MSISDN) = TRIM(T.MSISDN)
  and I.month_date = T.month_date
  WHERE C.R = 1
  and C.base_type <>'MIFI'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,14,15,16,17,18,19


