source('config_env.R')
source("../function_library.R")
manual_db_pick = c(3)                         
configure_temp = manual_pick(manual_db_pick)
########
start_date = '2019-10-01'
end_date = '2019-11-01'

e_com_d = runQuery(paste0("select dc_id, dc_name from developers_categories
                        where dc_name rlike 'e-commerce'"))
e_com_a = runQuery(paste0("select ac_id, ac_name from applications_categories
                          where ac_name rlike 'e-commerce'"))
a_ecom_live = runQuery(paste0("select a.d_id, a.a_id, ac.ac_name, dc.dc_name
                              from applications a
                              left join applications_to_categories atc on a.a_id = atc.a_id
                              left join applications_categories ac on atc.ac_id = ac.ac_id
left join developers d on a.d_id = d.d_id
left join developers_categories dc on d.dc_id = dc.dc_id
                              where (ac.ac_id in (",paste0(e_com_a$ac_id,collapse = ","),")
or d.dc_id in (",paste0(e_com_d$dc_id,collapse = ","),"))
                              and a.a_live = 1"))
a_ecom_psda = runQuery(paste0("select distinct a_id from ps_clicks_daily_aggregated
                              where a_id in (",paste0(a_ecom_live$a_id,collapse = ","),")"))

clicks = runQuery(paste0("select psc.cl_id, psc.a_id, from_unixtime(psc.cl_date_clicked) timecl,
psc.cl_date_clicked as timecl_timestamp,
                         del.del_id, del.del_type, del.del_details,
from_unixtime(del.del_estimated_delivery_datetime) del_estimated_delivery_datetime,
#from_unixtime(del.del_estimated_update_datetime) as del_estimated_update_datetime,
                         ds.ds_id, ds.del_status as ds_del_status, 
from_unixtime(ds.del_datetime) as del_datetime,
                         ds.del_datetime as del_datetime_timestamp
                         from delivery del
                         left join delivery_statuses ds on del.del_id = ds.del_id
left join ps_clicks psc on del.cl_id = psc.cl_id
                         where psc.cl_date_clicked >= unix_timestamp('",start_date,"')
                         and psc.cl_date_clicked < unix_timestamp('",end_date,"')
                         and psc.a_id in (",paste0(a_ecom_psda$a_id,collapse = ","),")
                         and cl_tracked = 1
                         and del.del_type = 'physical'"))

data = clicks
#### Removed bug cases where 1 status is created twice
data = data %>% group_by(cl_id,ds_del_status) %>% top_n(1,ds_id)
# speed = clicks %>% group_by(cl_id,a_id,timecl,del_id,del_type,del_details,
#                           del_estimated_delivery_datetime,del_estimated_update_datetime,
#                           rleid = data.table::rleid(del_id,ds_del_status)
#                           ) %>% summarise(ds_id = max(ds_id),
#                                           del_datetime = max(del_datetime),
#                                           del_datetime_timestamp = max(del_datetime_timestamp)
#                           ) %>% ungroup() %>% arrange(rleid) %>% select(-rleid)

data = data %>% group_by(cl_id,a_id,timecl,timecl_timestamp,del_id,del_type,del_details,
                          del_estimated_delivery_datetime
        ) %>% mutate(next_ds_del_status = lead(ds_del_status,order_by = del_id),
                     next_del_datetime = lead(del_datetime,order_by = del_id),
                     next_del_datetime_timestamp = lead(del_datetime_timestamp,order_by = del_id))
data[is.na(data)] = 0
speed = data %>% filter(next_ds_del_status != 0)
#### change status time
speed$cst = speed$next_del_datetime_timestamp - speed$del_datetime_timestamp
#### remove cases when delivering is created before order_placed for about 10 seconds
speed = speed %>% mutate(remove = ifelse(ds_del_status=='delivering'&next_ds_del_status=='order_placed',1,0)
                         ) %>% filter(remove == 0)
#### from order_placed to delivering
speed1 = speed %>% filter(ds_del_status == 'order_placed' & next_ds_del_status == 'delivering')
speed1 = speed1 %>% group_by(a_id) %>% summarise(total_cst1 = sum(cst),
                                                 average_cst1 = round(sum(cst)/uniqueN(cl_id)))
#### from delivering to delivered
speed2 = speed %>% filter(ds_del_status == 'delivering' & next_ds_del_status == 'delivered')

speed2 = speed2 %>% group_by(a_id) %>% summarise(total_cst2 = sum(cst),
                                               average_cst2 = round(sum(cst)/uniqueN(cl_id)))

#### count orders in its latest step
total = data %>% group_by(a_id) %>% summarise(orders = uniqueN(cl_id),
                                              order_placed = uniqueN(cl_id[ds_del_status=='order_placed'&next_ds_del_status==0]),
                                              delivering = uniqueN(cl_id[ds_del_status == 'delivering'&next_ds_del_status==0]),
                                              delivered = uniqueN(cl_id[ds_del_status=='delivered'&next_ds_del_status==0])) 

total = left_join(total,speed1,'a_id') %>% left_join(.,speed2,'a_id')

#### difference btw actual delivered time and click made (average delivering time)
delivered = data %>% filter(ds_del_status == 'delivered')
delivered$diff = delivered$del_datetime_timestamp - delivered$timecl_timestamp
delivered = delivered %>% group_by(a_id) %>% summarise(diff = sum(diff),
                                                       delivered_order = uniqueN(cl_id))
delivered$adt = round(delivered$diff/delivered$delivered_order)

total = left_join(total,delivered %>% select(a_id,adt),'a_id')


info = runQuery(paste0("select a.d_id, d.d_company, a.a_id, a.a_name
                       from applications a
                       left join developers d on a.d_id = d.d_id
                       where a.a_id in (",paste0(total$a_id,collapse = ","),")"))
total = left_join(total,info,'a_id')
total = total[,c(11,12,1,13,2:5,7,9,10)]

total$average_cst1 = seconds_to_period(total$average_cst1)
total$average_cst1 = gsub("M"," min",total$average_cst1)
total$average_cst1 = gsub(" 0S","",total$average_cst1)
total$average_cst1 = gsub("S"," sec",total$average_cst1)
total$average_cst2 = seconds_to_period(total$average_cst2)
total$average_cst2 = gsub("M"," min",total$average_cst2)
total$average_cst2 = gsub(" 0S","",total$average_cst2)
total$average_cst2 = gsub("S"," sec",total$average_cst2)
total$adt = seconds_to_period(total$adt)
total$adt = gsub("M","min",total$adt)
total$adt = gsub("S","sec",total$adt)
total[is.na(total)] = "_"

colnames(total) = c("MID","Merchant","PID","Project",
                    "Order","Order Placed","Delivering","Delivered",
                    "Average time from Order Placed to Delivereing",
                    "Average time from Delivering to Delivered",
                    "Average time from Paid to Delivered")


# reverse = data %>% mutate(reverse = ifelse(ds_del_status=='delivering' & next_ds_del_status=='order_placed',1,0)
#                           ) %>% filter(reverse == 1)
# reverse = clicks %>% filter(del_id %in% reverse$del_id)
# reverse = left_join(reverse,info,'a_id')
# write_clip(reverse)
# duplicate = anti_join(clicks,data)
# duplicate = clicks %>% filter(del_id %in% duplicate$del_id)
# library(clipr)
# write_clip(duplicate)
# 
# info = runQuery(paste0("select a.d_id, d.d_company, a.a_id, a.a_name
#                        from applications a
#                        left join developers d on a.d_id = d.d_id
#                        where a.a_id in (",paste0(second$a_id,collapse = ","),")"))
# duplicate = left_join(duplicate,info,'a_id')
# 
# second = clicks %>% filter(del_id == 2851812)
# second = left_join(second,info,'a_id')
# write_clip(second)
