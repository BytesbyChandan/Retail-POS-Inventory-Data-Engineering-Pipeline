create or replace database threetype;
use threetype;
create schema threetypefile;
use schema threetypefile;

create or replace stage threetstage
url = 
credentials = (
    aws_key_id = 
    aws_secret_key = 
);

desc stage threetstage;
list @threetstage;

-- JSON FILE
create or replace table bronze_json(raw variant);

create or replace file format json_ff
type = 'json',
strip_outer_array = true;

create or replace pipe json_pipe
auto_ingest = true
as 
copy into bronze_json 
from @threetstage/json/iot_events_batch_01.json
file_format = (format_name = json_ff);

desc pipe json_pipe;

copy into bronze_json
from @threetstage/json/iot_events_batch_01.json
file_format = (format_name = json_ff);

select * from bronze_json;

select  
    f.value:alert_type as Alert_type,
    f.value:severity as Severity,
    f.value:triggered_at as Trggered_at,
    raw:device_id as Device_id,
    raw:event_id as Event_id,
    raw:event_type as Event_type,
    raw:metadata:battery_pct as Battery_pct,
    raw:metadata:firmware as Firmware,
    raw:metadata:signal_rssi as SignalRssI
from bronze_json,
lateral flatten(input => raw:alerts) as f;

create or replace table silver_json (
    alert_type varchar(100),
    severity varchar(50),
    triggered_at timestamp_ntz,
    device_id varchar(100),
    event_id varchar(100),
    event_type varchar(100),
    battery_pct number(10,2),
    firmware varchar(100),
    signal_rssi number (10,2)
);

insert into silver_json
select 
    initcap(trim(f.value:alert_type)) as Alert_type,
    lower(trim(f.value:severity)) as Severity,
    try_to_timestamp_ntz(f.value:triggered_at::varchar) as Trggered_at,
    trim(raw:device_id) as Device_id,
    upper(trim(raw:event_id)) as Event_id,
    initcap(trim(raw:event_type)) as Event_type,
    raw:metadata:battery_pct as Battery_pct,
    trim(raw:metadata:firmware) as Firmware,
    raw:metadata:signal_rssi as SignalRssI
from bronze_json,
lateral flatten(input => raw:alerts) as f;

select * from silver_json;

create or replace stream json_stream
on table bronze_json;

create or replace task json_task 
warehouse = 'COMPUTE_WH'
when system$stream_has_data('json_stream')
as
merge into silver_json as sj
using (
select 
    initcap(trim(f.value:alert_type)) as Alert_type,
    lower(trim(f.value:severity)) as Severity,
    try_to_timestamp_ntz(f.value:triggered_at::varchar) as Triggered_at,
    trim(raw:device_id) as Device_id,
    upper(trim(raw:event_id)) as Event_id,
    initcap(trim(raw:event_type)) as Event_type,
    raw:metadata:battery_pct as Battery_pct,
    trim(raw:metadata:firmware) as Firmware,
    raw:metadata:signal_rssi as SignalRssI,
    METADATA$ACTION AS action,
    METADATA$ISUPDATE AS isupdate
from json_stream,
lateral flatten(input => raw:alerts) as f
) as js
on sj.device_id = js.device_id and sj.event_id = js.event_id and sj.alert_type = js.alert_type
when matched 
   and js.action = 'DELETE'
   and js.isupdate = FALSE
then delete 

when matched
    and js.action = 'INSERT'
    and js.isupdate = TRUE
then update set 
        sj.event_type = js.Event_type,
        sj.severity = js.Severity,
        sj.triggered_at = js.Triggered_at,
        sj.battery_pct = js.Battery_pct,
        sj.firmware = js.Firmware,
        sj.signal_rssi = js.SignalRssi
when not matched 
    and js.action = 'INSERT'
    and js.isupdate = FALSE
then insert (
    alert_type,
    severity,
    triggered_at,
    device_id,
    event_id,
    event_type,
    battery_pct,
    firmware,
    signal_rssi
)
values (
    js.Alert_type,
    js.Severity,
    js.Triggered_at,
    js.Device_id,
    js.Event_id,
    js.Event_type,
    js.Battery_pct,
    js.Firmware,
    js.SignalRssi
);


insert into silver_json select 
    initcap(trim(f.value:alert_type)) as Alert_type,
    lower(trim(f.value:severity)) as Severity,
    try_to_timestamp_ntz(f.value:triggered_at::varchar) as Triggered_at,
    trim(raw:device_id) as Device_id,
    upper(trim(raw:event_id)) as Event_id,
    initcap(trim(raw:event_type)) as Event_type,
    raw:metadata:battery_pct as Battery_pct,
    trim(raw:metadata:firmware) as Firmware,
    raw:metadata:signal_rssi as SignalRssI,
from json_stream,
lateral flatten(input => raw:alerts) as f
 where metadata$action='INSERT' 
 and metadata$isupdate='FALSE';

 
alter task json_task resume;
desc task json_task;
alter task json_task suspend;

select * from silver_json;

copy into @threetstage/json/silver_json
from (
      select object_construct(*)
      from silver_json
      )
file_format = (format_name = 'json_ff');

 







-- PARQUET FILE
create or replace table bronze_parquet(raw variant);

create or replace file format parquet_ff
type = 'parquet';


create or replace pipe parquet_pipe
auto_ingest = true
as
copy into bronze_parquet
from @threetstage/parquet/erp_inventory.parquet
file_format = (format_name = parquet_ff);

desc pipe parquet_pipe;

copy into bronze_parquet
from @threetstage/parquet/erp_inventory.parquet
file_format = (format_name = json_ff);

select * from bronze_parquet;

select 
    raw:category as Category,
    raw:last_received_date as LastReceivedDate,
    raw:max_stock_level as MaxStockLevel,
    raw:product_sku as Product_sku,
    raw:quantity_on_hand as Quantity_on_hand,
    raw:record_level as RecordLevel,
    raw:snapshot_date as SnapShot_Date
from bronze_parquet;

create or replace table silver_parquet(
    category varchar(100),
    last_received_date date,
    max_stock_level number,
    product_sku varchar(50),
    quantity_on_hand number,
    record_level int,
    snapshot_date date     
);

insert into silver_parquet
select 
    raw:category as Category,
    raw:last_received_date as LastReceivedDate,
    raw:max_stock_level as MaxStockLevel,
    raw:product_sku as Product_sku,
    raw:quantity_on_hand as Quantity_on_hand,
    raw:record_level as RecordLevel,
    raw:snapshot_date as SnapShot_Date
from bronze_parquet;

select * from silver_parquet;

create or replace stream parquet_stream
on table bronze_parquet;

select 
    raw:category as Category,
    raw:last_received_date as LastReceivedDate,
    raw:max_stock_level as MaxStockLevel,
    raw:product_sku as Product_sku,
    raw:quantity_on_hand as Quantity_on_hand,
    raw:record_level as RecordLevel,
    raw:snapshot_date as SnapShot_Date,
    metadata$action,
    metadata$isupdate,
    metadata$row_id
from parquet_stream;

create or replace task parquet_task
warehouse = 'COMPUTE_WH'
when system$stream_has_data('parquet_stream')
as
merge into silver_parquet as sp
using (
    select 
    raw:category as Category,
    raw:last_received_date as LastReceivedDate,
    raw:max_stock_level as MaxStockLevel,
    raw:product_sku as Product_sku,
    raw:quantity_on_hand as Quantity_on_hand,
    raw:record_level as RecordLevel,
    raw:snapshot_date as SnapShot_Date,
    metadata$action as action,
    metadata$isupdate as isupdate,
    metadata$row_id
from parquet_stream
) as ps
on sp.Product_sku = ps.Product_sku
when matched 
   and ps.action = 'DELETE'
   and ps.isupdate = FALSE
then delete 

when matched
    and ps.action = 'INSERT'
    and ps.isupdate = TRUE
then update set 
    sp.Category = ps.Category,
    sp.Last_Received_Date = ps.LastReceivedDate,
    sp.Max_Stock_Level = ps.MaxStockLevel,
    sp.Quantity_on_hand = ps.Quantity_on_hand,
    sp.Record_Level = ps.RecordLevel,
    sp.SnapShot_Date = ps.SnapShot_Date

when not matched 
    and ps.action = 'INSERT'
    and ps.isupdate = FALSE
then insert (
    Category,
    last_received_date,
    max_stock_level,
    product_sku,
    quantity_on_hand,
    record_level,
    snapshot_date
)
values (
    ps.Category,
    ps.LastReceivedDate,
    ps.MaxStockLevel,
    ps.Product_sku,
    ps.Quantity_on_hand,
    ps.RecordLevel,
    ps.SnapShot_Date
);


alter task parquet_task resume;
desc task parquet_task;
alter task parquet_task suspend;

copy into @threetstage/parquet/silver_parquet
from (
      select object_construct(*)
      from silver_parquet
      )
file_format = (format_name = 'parquet_ff');





-- CSV FILE
select  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
from @threetstage/csv/pos_batch_jan_apr.csv;

create or replace file format csv_ff
type='csv', 
skip_header=1;

create or replace table bronze_csv(
    transaction_id varchar(50),
    store_id varchar(20),
    store_name varchar(100),
    store_city varchar(100),
    store_region varchar(100),
    cashier_id varchar(20),
    customer_id varchar(20),
    transaction_date date,
    transaction_time time,
    product_sku varchar(40),
    product_name varchar(200),
    category varchar(100),
    subcategory varchar(100),
    quantity int,
    unit_price number(12,2),
    discount_pct number(5,2),
    total_amount number(12,2),
    payment_method varchar(50),
    loyalty_points number(10,2)
);

create or replace pipe csv_pipe
auto_ingest = true
as
copy into bronze_csv
from @threetstage/csv/pos_batch_jan_apr.csv
file_format = (format_name = csv_ff);

desc pipe csv_pipe;

copy into bronze_csv
from @threetstage/csv/pos_batch_jan_apr.csv
file_format = (format_name = csv_ff);

select * from bronze_csv;

create or replace table silver_csv(
    transaction_id varchar(50),
    store_id varchar(20),
    store_name varchar(100),
    store_city varchar(100),
    store_region varchar(100),
    cashier_id varchar(20),
    customer_id varchar(20),
    transaction_date date,
    transaction_time time,
    product_sku varchar(40),
    product_name varchar(200),
    category varchar(100),
    subcategory varchar(100),
    quantity int,
    unit_price number(12,2),
    discount_pct number(5,2),
    total_amount number(12,2),
    payment_method varchar(50),
    loyalty_points number(10,2)
);


insert into silver_csv 
select 
    upper(trim(transaction_id)) as Transaction_id,
    trim(store_id) as Store_id,
    initcap(trim(store_name)) as Store_Name,
    initcap(trim(store_city)) as Store_City,
    upper(trim(Store_region)) as Store_region,
    trim(cashier_id) as Cashier_Id,
    trim(customer_id) as Customer_id,
    transaction_date as Transaction_Date,
    transaction_time as Transaction_Time,
    upper(trim(product_sku)) as Product_SKU,
    initcap(trim(product_name)) as Product_Name,
    initcap(trim(category)) as Category,
    initcap(trim(subcategory)) as SubCategory,
    abs(try_to_number(quantity)) as Quantity,
    unit_price as Unit_Price,
    discount_pct as Discount_PCT,
    total_amount as Total_amount,
    payment_method as Payment_Method,
    loyalty_points as Loyalty_Points
from bronze_csv;


select * from silver_csv;

create or replace stream csv_stream
on table bronze_csv;

select * from csv_stream;

create or replace task csv_task 
warehouse = 'COMPUTE_WH'
when system$stream_has_data('csv_stream')
as
merge into silver_csv as sc
using csv_stream as cs
on 
sc.store_id = cs.store_id 
and 
sc.cashier_id = cs.cashier_id
and 
sc.customer_id = cs.customer_id
and
sc.transaction_id = cs.transaction_id


when matched 
    and metadata$action = 'DELETE'
    and metadata$isupdate = 'FALSE'
then delete

when matched
    and metadata$action = 'INSERT'
    and metadata$isupdate = 'TRUE'
then update set
    sc.store_name = cs.store_name,
    sc.store_city = cs.store_city,
    sc.store_region = cs.store_region,
    sc.transaction_date = cs.transaction_date,
    sc.transaction_time = cs.transaction_time,
    sc.product_sku = cs.product_sku,
    sc.product_name = cs.product_name,
    sc.category = cs.category,
    sc.subcategory = cs.subcategory,
    sc.quantity = cs.quantity,
    sc.unit_price = cs.unit_price,
    sc.discount_pct = cs.discount_pct,
    sc.total_amount = cs.total_amount,
    sc.payment_method = cs.payment_method,
    sc.loyalty_points = cs.loyalty_points
    
when not matched
    and cs.metadata$action = 'INSERT'
    and cs.metadata$isupdate = 'FALSE'
then insert(
    transaction_id,
    store_id,
    store_name,
    store_city,
    store_region,
    cashier_id,
    customer_id,
    transaction_date,
    transaction_time,
    product_sku,
    product_name,
    category,
    subcategory,
    quantity,
    unit_price,
    discount_pct,
    total_amount,
    payment_method,
    loyalty_points 
) values(
    cs.transaction_id,
    cs.store_id,
    cs.store_name,
    cs.store_city,
    cs.store_region,
    cs.cashier_id,
    cs.customer_id,
    cs.transaction_date,
    cs.transaction_time,
    cs.product_sku,
    cs.product_name,
    cs.category,
    cs.subcategory,
    cs.quantity,
    cs.unit_price,
    cs.discount_pct,
    cs.total_amount,
    cs.payment_method,
    cs.loyalty_points
);
    

alter task csv_task resume;
desc task csv_task;
alter task csv_task suspend;

copy into @threetstage/csv/silver_csv
from silver_csv
file_format = (format_name = 'csv_ff');

list @threetstage/csv;
select  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
from @threetstage/csv/pos_transactions.csv;

create or replace external table external_csv (
    transaction_id VARCHAR(50) AS (VALUE:c1::VARCHAR),
    store_id VARCHAR(20) AS (VALUE:c2::VARCHAR),
    store_name VARCHAR(100) AS (VALUE:c3::VARCHAR),
    store_city VARCHAR(100) AS (VALUE:c4::VARCHAR),
    store_region VARCHAR(100) AS (VALUE:c5::VARCHAR),
    cashier_id VARCHAR(20) AS (VALUE:c6::VARCHAR),
    customer_id VARCHAR(20) AS (VALUE:c7::VARCHAR),
    transaction_date DATE AS (VALUE:c8::DATE),
    transaction_time TIME AS (VALUE:c9::TIME),
    product_sku VARCHAR(40) AS (VALUE:c10::VARCHAR),
    product_name VARCHAR(200) AS (VALUE:c11::VARCHAR),
    category VARCHAR(100) AS (VALUE:c12::VARCHAR),
    subcategory VARCHAR(100) AS (VALUE:c13::VARCHAR),
    quantity INT AS (VALUE:c14::INT),
    unit_price NUMBER(12,2) AS (VALUE:c15::NUMBER(12,2)),
    discount_pct NUMBER(5,2) AS (VALUE:c16::NUMBER(5,2)),
    total_amount NUMBER(12,2) AS (VALUE:c17::NUMBER(12,2)),
    payment_method VARCHAR(50) AS (VALUE:c18::VARCHAR),
    loyalty_points NUMBER(10,2) AS (VALUE:c19::NUMBER(10,2))
)
LOCATION = @threetstage/csv/
FILE_FORMAT = (FORMAT_NAME = 'csv_ff');

select * from external_csv;

create or replace stream external_csv_stream
on external table external_csv
insert_only=true;

