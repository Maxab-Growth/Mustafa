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
    
    # Filter out excluded retailers
    initial_count = len(req_data)
    req_data = req_data[~req_data['retailer_id'].isin(excluded_retailer_ids)]
    excluded_count = initial_count - len(req_data)
    print(f"Excluded {excluded_count} retailer-tag combinations")

    pricing_api_secret = json.loads(get_secret("prod/pricing/api/"))
    _username = pricing_api_secret["egypt_username"]
    _password = pricing_api_secret["egypt_password"]
    secret = pricing_api_secret["egypt_secret"]

    clear_directory('dynamic_tags')
    upload_dynamic_tags(req_data, _username, _password, secret)


def get_secret(secret_name):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            return get_secret_value_response['SecretString']
        else:
            return base64.b64decode(get_secret_value_response['SecretBinary'])


def _unused():
    # Placeholder to avoid linter warnings for imported but unused names in Airflow parse time.
    return calendar, tqdm, ServiceAccountCredentials, gspread


def get_access_token(url, client_id, client_secret, username, password):
    """
    get_access_token function takes three parameters and returns a session token
    to connect to MaxAB APIs

    :param url: production MaxAB token URL
    :param client_id: client ID
    :param client_secret: client sercret
    :return: session token
    """
    response = requests.post(
        url,
        data={"grant_type": "password",
              "username": username,
              "password": password},
        auth=(client_id, client_secret),
    )
    return response.json()["access_token"]



import glob

def clear_directory(directory):
    """Delete all files in directory but keep the directory"""
    files = glob.glob(os.path.join(directory, '*'))
    for f in files:
        if os.path.isfile(f):
            os.remove(f)
            print(f"Deleted: {f}")



import os
import time
import base64
import requests
import pandas as pd

def upload_dynamic_tags(req_data, username, password, secret):
    """Upload dynamic tags to API"""
    os.makedirs('dynamic_tags', exist_ok=True)
    
    # Get unique tags
    unique_tags = req_data[['tag_id', 'tag_name']].drop_duplicates()
    
    print(f"Found {len(unique_tags)} unique tags to process\n")
    
    success_count = 0
    fail_count = 0
    total_retailers = 0
    
    for idx, (tag_id, tag_name) in enumerate(unique_tags.itertuples(index=False), 1):
        # Convert to Python native types
        tag_id = int(tag_id)
        tag_name = str(tag_name)
        
        print(f"[{idx}/{len(unique_tags)}] Processing tag {tag_id}: {tag_name}")
        
        # Get data for this tag
        tag_data = req_data[req_data['tag_id'] == tag_id]
        to_upload = tag_data[['retailer_id']].drop_duplicates()
        
        print(f"  - Retailers: {len(to_upload)}")
        total_retailers += len(to_upload)
        
        # Save to Excel
        file_path = f'dynamic_tags/tag_{tag_id}_list.xlsx'
        to_upload.to_excel(file_path, index=False, sheet_name='Sheet1')
        print(f"  ✓ Saved: {file_path}")
        
        # Read as binary
        with open(file_path, 'rb') as f:
            file_base64 = base64.b64encode(f.read()).decode('utf-8')
        
        # Get token
        try:
            token = get_access_token(
                'https://sso.maxab.info/auth/realms/maxab/protocol/openid-connect/token',
                'main-system-externals',
                secret,
                username,
                password
            )
        except Exception as e:
            print(f"  ✗ Failed to get token: {e}\n")
            fail_count += 1
            continue
        
        # Upload to API
        url = f"https://api.maxab.info/commerce/api/admins/v1/internal-dynamic-tags/{tag_id}"
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        
        payload = {
            "basic_info": {
                "id": tag_id,
                "type": 1,
                "method": 2,
                "name": tag_name
            },
            "file": file_base64,
            "file_extension": "xlsx"
        }
        
        try:
            response = requests.put(url, headers=headers, json=payload)
            
            if response.status_code in [200, 201, 204]:
                print(f"  ✓ Upload successful: {response.status_code}")
                success_count += 1
            else:
                print(f"  ✗ Upload failed: {response.status_code}")
                print(f"    Error: {response.text}")
                fail_count += 1
        except Exception as e:
            print(f"  ✗ Request failed: {e}")
            fail_count += 1
        
        print()
        time.sleep(2)  # Rate limiting
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"{'='*60}")

    # Send a small Slack summary (uses SLACK_TOKEN if available)
    try:
        def send_text_slack(channel, text):
            try:
                import slack
            except Exception:
                return
            slack_token = os.environ.get("SLACK_TOKEN")
            if not slack_token:
                return
            client = slack.WebClient(token=slack_token)
            try:
                client.chat_postMessage(channel=channel, text=text)
            except Exception:
                pass

        total_tags = len(unique_tags)
        msg = (
            f":label: Dynamic tags upload\n"
            f"• Tags: {total_tags}\n"
            f"• Retailers total: {total_retailers}\n"
            f"• Success: {success_count} | Failed: {fail_count}"
        )
        send_text_slack(channel='pushed_prices', text=msg)
    except Exception:
        pass

default_args = {
    'owner': 'seif', #stakeholder: Mostafa waleed
    
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 5),
    'catchup': False,
}

with DAG(
    dag_id='dynamic_tags_upload',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # 18:00 UTC daily
    catchup=False,
    tags=[ 'dynamic_tags'],
) as dag:
    run_dynamic_tags_upload_task = PythonOperator(
        task_id='run_dynamic_tags_upload',
        python_callable=run_dynamic_tags_upload,
    )