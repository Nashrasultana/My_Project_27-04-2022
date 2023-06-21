with msisdn_devs as (
    select
    t.msisdn,
    substr(t.imei,1,8) as imei8,
    ddr.tac,
    ddr.protocol,
    ddr.device_type,
    ddr.model,
    ddr.manufacturer,
    ddr.marketing_name,
    ddr.volte_flag,
    ddr.vowifi_flag,
    
    max(date_of_call) as last_date,
       
    rank() over (partition by t.msisdn order by max(date_of_call) desc, protocol desc, sum(t.HOME_DATA_DURATION_SECS + t.WORK_DATA_DURATION_SECS) desc, sum(t.HOME_MO_COUNT + t.WORK_MO_COUNT + t.HOME_MT_COUNT + t.WORK_MT_COUNT) desc, sum(t.HOME_MO_SMS_COUNT + t.WORK_MO_SMS_COUNT + t.HOME_MT_SMS_COUNT + t.WORK_MT_SMS_COUNT) desc, tac desc) as r_date
    
    from        `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.t_agg_dly_subscriber_usage` t
    left join   `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.ref_cdm_device_excal` ddr
            on  substr(imei,1,8) = ddr.tac
            
    WHERE call_date between date_add(date_trunc(current_date(), month), interval -1 month) and
	LAST_DAY(date_add(date_trunc(current_date(), month), interval -1 month)) 

    group by 1,2,3,4,5,6,7,8,9,10
      
)

select

msisdn,
imei8 as tac,
protocol,
device_type,
model,
manufacturer,
marketing_name,
volte_flag,
vowifi_flag,
last_date

from msisdn_devs
where r_date = 1

