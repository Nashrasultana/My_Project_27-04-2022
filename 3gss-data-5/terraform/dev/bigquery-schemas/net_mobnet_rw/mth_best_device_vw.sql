WITH
  msisdn_devs AS (
  SELECT
    t.month_id,
    t.msisdn,
    SUBSTR(t.imei,1,8) AS imei8,
    ddr.tac,
    ddr.protocol,
    ddr.device_type,
    ddr.model,
    ddr.manufacturer,
    ddr.marketing_name,
    ddr.volte_flag,
    ddr.vowifi_flag,

RANK() OVER (PARTITION BY t.msisdn ORDER BY protocol DESC, SUM(t.HOME_DATA_DURATION_SECS + t.WORK_DATA_DURATION_SECS) DESC, SUM(t.home_mo_count + t.home_mt_count + t.work_mo_count + t.work_mt_count) DESC, SUM(t.home_mo_sms_count + t.home_mt_sms_count + t.work_mo_sms_count + t.work_mt_sms_count) DESC, tac DESC) AS r

FROM
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.t_agg_mth_subscriber_usage` AS t

LEFT JOIN
  `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.ref_cdm_device_excal` ddr
ON
  SUBSTR(imei,1,8) = ddr.tac
WHERE
  month_date = date_add(date_trunc(current_date(), month), interval -1 month) 
  
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11
)
----------------------------------------
SELECT DISTINCT
    MSISDN_DEVS.MONTH_ID,
    MSISDN_DEVS.MSISDN,
    MSISDN_DEVS.TAC,
    MSISDN_DEVS.PROTOCOL,
    MSISDN_DEVS.DEVICE_TYPE,
    MSISDN_DEVS.MODEL,
    MSISDN_DEVS.MANUFACTURER,
    MSISDN_DEVS.MARKETING_NAME,
    MSISDN_DEVS.VOLTE_FLAG,
    MSISDN_DEVS.VOWIFI_FLAG
  FROM
    `MSISDN_DEVS`
  WHERE MSISDN_DEVS.R = 1

