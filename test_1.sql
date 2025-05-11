select * from platinum.order_master_bi 
where order_date= current_Date - interval '1' day
and test=0 and verified=1