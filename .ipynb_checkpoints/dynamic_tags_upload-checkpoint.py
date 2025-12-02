import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
import calendar
import json
from datetime import date, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from common import setup_environment_2
import importlib
import warnings
import time
import boto3
import requests
import json
import os
import base64
from botocore.exceptions import ClientError
warnings.filterwarnings("ignore")
importlib.reload(setup_environment_2)
setup_environment_2.initialize_env()
import gspread     
from airflow import DAG
from airflow.operators.python import PythonOperator



def query_snowflake(query, columns=[]):
    import os
    import snowflake.connector
    import numpy as np
    import pandas as pd
    con = snowflake.connector.connect(
        user =  os.environ["SNOWFLAKE_USERNAME"],
        account= os.environ["SNOWFLAKE_ACCOUNT"],
        password= os.environ["SNOWFLAKE_PASSWORD"],
        database =os.environ["SNOWFLAKE_DATABASE"]
    )
    try:
        cur = con.cursor()
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute(query)
        if len(columns) == 0:
            out = pd.DataFrame(np.array(cur.fetchall()))
        else:
            out = pd.DataFrame(np.array(cur.fetchall()),columns=columns)
        return out
    except Exception as e:
        print("Error: ", e)
    finally:
        cur.close()
        con.close() 


def run_dynamic_tags_upload():
    query = '''
SHOW PARAMETERS LIKE 'TIMEZONE'
'''
    x  = query_snowflake(query)
    _zone_to_use = x[1].values[0]

    query = '''

select distinct retailer_id,sales_order_status_id
from (
select so.retailer_id, so.sales_order_status_id,so.created_at as order_date,max(so.created_at)over(partition by so.retailer_id) as last_date
from sales_orders so 
where so.created_at::date >= current_date - 30 
qualify last_date = order_date
)
'''
    retailer_status  = query_snowflake(query, columns = ['retailer_id','sales_order_status_id'])
    retailer_status.columns = retailer_status.columns.str.lower()
    for col in retailer_status.columns:
        retailer_status[col] = pd.to_numeric(retailer_status[col], errors='ignore')      

    query = '''
with base as (
select c.id as cohort_id,dt.dynamic_tag_id,taggable_id as retailer_id
from cohorts c 
join  dynamic_taggables dt on dt.dynamic_tag_id = c.dynamic_tag_id
where c.id in (700,701,702,703,704,1124,1125,1126,1123)
),
mapping as (
select * 
from (
values
('Cairo',700,2807),
('Giza',701,2808),
('Alexandria',702,2809),
('Delta East',704,2811),
('Delta West',703,2812),
('Upper Egypt',1123,2810),
('Upper Egypt',1124,2810),
('Upper Egypt',1125,2810),
('Upper Egypt',1126,2810)
)x(region,cohort_id,tag_id)

),
final_data as (
select cohort_id,dynamic_tag_id,retailer_id,sum(percent*cntrb) as final_perc
from (
select *,
PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY qty) over(partition by product_id,dynamic_tag_id) as perc_80,
case when qty>perc_80 then least((qty/perc_80)*80,100) else greatest((qty/perc_80)*80,40) end as percent,
nmv/sum(nmv)over(partition by retailer_id) as cntrb
from (
select 
base.cohort_id,
base.dynamic_tag_id,
so.retailer_id,
pso.product_id,
sum(purchased_item_count*basic_unit_count) as qty,
sum(pso.total_price) as nmv


FROM product_sales_order pso
JOIN sales_orders so ON so.id = pso.sales_order_id
JOIN products on products.id=pso.product_id
JOIN brands on products.brand_id = brands.id 
JOIN categories ON products.category_id = categories.id
join base on base.retailer_id = so.retailer_id
        

WHERE   True
    AND so.created_at::date between date_trunc('month',current_date - interval '4 months') and date_trunc('month',current_date)
    AND so.sales_order_status_id not in (7,12)
    AND so.channel IN ('telesales','retailer')
    AND pso.purchased_item_count <> 0

GROUP BY ALL
)
)
group by all 
HAVING final_perc > 80
)

select tag_id,retailer_id,dt.name as tag_name
from final_data fd
join mapping m on fd.cohort_id = m.cohort_id
join dynamic_tags dt on dt.id = m.tag_id

'''
    tags_recc_rets  = query_snowflake(query, columns = ['tag_id','retailer_id','tag_name'])
    tags_recc_rets.columns = tags_recc_rets.columns.str.lower()
    for col in tags_recc_rets.columns:
        tags_recc_rets[col] = pd.to_numeric(tags_recc_rets[col], errors='ignore')      

    req_data = tags_recc_rets.merge(retailer_status,on=['retailer_id'],how='left')
    req_data = req_data.fillna(0)
    req_data = req_data.groupby(['tag_id','retailer_id','tag_name'])['sales_order_status_id'].max().reset_index()
    req_data = req_data[req_data['sales_order_status_id'].isin([0,6,9,12])]

    # Exclude retailers that have whole_sale dynamic tags
    query = '''
    select dt.id,dt.name as tag_name,dta.TAGGABLE_ID as retailer_id,dta.created_at
    from DYNAMIC_TAGS dt 
    join dynamic_taggables dta on dt.id = dta.dynamic_tag_id 
    where name like '%whole_sale%'
    and dt.id > 3000
    '''
    whole_sale_retailers = query_snowflake(query, columns=['id', 'tag_name', 'retailer_id', 'created_at'])
    whole_sale_retailers.columns = whole_sale_retailers.columns.str.lower()
    for col in whole_sale_retailers.columns:
        whole_sale_retailers[col] = pd.to_numeric(whole_sale_retailers[col], errors='ignore')
    
    # Get unique retailer_ids to exclude
    excluded_retailer_ids = whole_sale_retailers['retailer_id'].unique()
    print(f"Excluding {len(excluded_retailer_ids)} retailers with whole_sale tags")
    
   