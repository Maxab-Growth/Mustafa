# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:   
# https://aws.amazon.com/developers/getting-started/python/

import os
import boto3
import base64
from botocore.exceptions import ClientError
import json
from requests import get
from pathlib import Path

def imports():
    import os
    import pandas as pd
    import numpy as np
    import psycopg2

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
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
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

def initialize_env():
    db_secret = json.loads(get_secret("rds/mainsystem/redash"))
    dwh_reader_secret = json.loads(get_secret("prod/db/datawarehouse/metabase"))
    dwh_writer_secret = json.loads(get_secret("prod/db/datawarehouse/sagemaker"))
    snowflake_secret = json.loads(get_secret("Snowflake-sagemaker"))

    # maxab_data_aws_key = json.loads(get_secret("prod/MaxabDataAWSKey"))
    
    renv_path = str(Path.home())+"/.Renviron"
    print(renv_path)
    renv = open(renv_path, "w")
    os.environ["DB_HOST"] = db_secret["host"]
    renv.write("DB_HOST="+db_secret["host"]+"\n")
    os.environ["DB_NAME"] = db_secret["dbname"]
    renv.write("DB_NAME="+db_secret["dbname"]+"\n")
    os.environ["DB_USER_NAME"] = db_secret["username"]
    renv.write("DB_USER_NAME="+db_secret["username"]+"\n")
    os.environ["DB_PASSWORD"] = db_secret["password"]
    renv.write("DB_PASSWORD="+db_secret["password"]+"\n")
    
    os.environ["DWH_READER_HOST"] = dwh_reader_secret["host"]
    os.environ["DWH_READER_NAME"] = dwh_reader_secret["dbname"]
    os.environ["DWH_READER_USER_NAME"] = dwh_reader_secret["username"]
    os.environ["DWH_READER_PASSWORD"] = dwh_reader_secret["password"]
    
    os.environ["DWH_WRITER_HOST"] = dwh_writer_secret["host"]
    os.environ["DWH_WRITER_NAME"] = dwh_writer_secret["dbname"]
    os.environ["DWH_WRITER_USER_NAME"] = dwh_writer_secret["username"]
    os.environ["DWH_WRITER_PASSWORD"] = dwh_writer_secret["password"]
    
    os.environ["SNOWFLAKE_USERNAME"] = snowflake_secret["username"]
    os.environ["SNOWFLAKE_SERVICE_USERNAME"] = snowflake_secret["service_username"]
    os.environ["SNOWFLAKE_PASSWORD"] = snowflake_secret["password"]
    os.environ["SNOWFLAKE_ACCOUNT"] = snowflake_secret["account"]
    os.environ["SNOWFLAKE_DATABASE"] = snowflake_secret["database"]
    os.environ["SNOWFLAKE_ROLE"] = snowflake_secret["role"]
    os.environ["sagemaker_key_bucket"] = snowflake_secret["private_key_bucket"]
    os.environ["sagemaker_key_path"] = snowflake_secret["private_key_path"]
    boto3.client('s3').download_file(os.environ["sagemaker_key_bucket"], 
                                         os.environ["sagemaker_key_path"], '/tmp/sagemaker_service.p8')
    # os.environ["MaxAB_Data_AWS_Key"] = maxab_data_aws_key["maxab_data_aws_key_id"]
    # os.environ["MaxAB_Data_AWS_Secret"] = maxab_data_aws_key["maxab_data_aws_key_secret"]
    
    renv.close()
    
    os.environ["INSTANCE_IP"] = get('https://api.ipify.org').text
    
    json_path = str(Path.home())+"/service_account_key.json"
    print(json_path)
    bigquery_key = get_secret("prod/bigquery/sagemaker")
    f = open(json_path, "w")
    f.write(bigquery_key)
    f.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    
    slack_secret = json.loads(get_secret("prod/slack/reports"))
    os.environ["SLACK_TOKEN"] = slack_secret["token"]

    


# Define Functions to read and write to aws s3 maxab-shared    

def maxab_shared_download_file(file,destination):
    import boto3
    import botocore
    from botocore.config import Config
    import os
    BUCKET_NAME = 'maxab-shared' # replace with your bucket name
    KEY =  file # replace with your object key
    s3 = boto3.resource('s3')
    try:
        my_config = Config(region_name = 'us-east-1')
        s3_client = boto3.client(
                              's3',
                              aws_access_key_id=os.environ["MaxAB_Data_AWS_Key"],
                              aws_secret_access_key=os.environ["MaxAB_Data_AWS_Secret"],
                              config = my_config)
    except Exception as e:
        print(e)
    try:
        s3_client.download_file(BUCKET_NAME, KEY, destination)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
            
def upload_file(file_name, bucket, object_name=None):
    import logging
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    my_config = Config(
        region_name = 'us-east-1')
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client(
                          's3',
                          aws_access_key_id=os.environ["MaxAB_Data_AWS_Key"],
                          aws_secret_access_key=os.environ["MaxAB_Data_AWS_Secret"],
                          config = my_config)
    response = s3_client.upload_file(file_name, bucket, object_name)
    return True
def maxab_shared_upload_file(file,destination):
    BUCKET_NAME = 'maxab-shared' # replace with your bucket name
    upload_file(file, BUCKET_NAME, destination)
    print('done')
    
#################################################################
# Define a function to get data from PostgreSQL DataWarehouse    
def dwh_pg_query(query,columns = []):
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        conn = psycopg2.connect(host=os.environ["DWH_READER_HOST"], database=os.environ["DWH_READER_NAME"], user=os.environ["DWH_READER_USER_NAME"], password=os.environ["DWH_READER_PASSWORD"])
        cur = conn.cursor()
        cur.execute(query)
        if len(columns) == 0:
            out = pd.DataFrame(np.array(cur.fetchall()))
        else:
            out = pd.DataFrame(np.array(cur.fetchall()),columns=columns)
        cur.close()
        conn.close()
        return out
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
        
# Define a function to get data from PostgreSQL Main DB    
def pg_query(query,columns = []):
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(query)
        if len(columns) == 0:
            out = pd.DataFrame(np.array(cur.fetchall()))
        else:
            out = pd.DataFrame(np.array(cur.fetchall()),columns=columns)
        cur.close()
        conn.close()
        return out
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
# Define a function to get data from Bigquery (app events)       
def bq_query(query):
    from google.cloud import bigquery
    import pandas as pd
    import os
    import numpy as np
    client = bigquery.Client()
    query_job = client.query(query)
    out = pd.DataFrame(query_job.to_dataframe())
    return out
# Define a function to get LR between dates 
def get_lr(date1,date2):
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        command_string = '''
        SELECT performance.lost_revenue.created_at::date,
               performance.lost_revenue.wh_id,
               performance.lost_revenue.product_id,
               concat(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku,
               brands.name_ar AS brand,
               categories.name_ar AS cat,
               sections.name_en,
               brands.id,
               categories.id,
               performance.lost_revenue.retailer_id,
               sum(performance.lost_revenue.lost_revenue) AS lost_revenue,
               count(DISTINCT performance.lost_revenue.retailer_id) AS oos_retailers
        FROM performance.lost_revenue
        JOIN products ON products.id = performance.lost_revenue.product_id
        JOIN brands ON brands.id = products.brand_id
        JOIN categories ON categories.id = products.category_id
        JOIN product_units ON product_units.id = products.unit_id
        left JOIN sections on sections.id = categories.section_id
        where performance.lost_revenue.created_at::Date between '{}' and '{}'
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5,
                 6,
                 7,
                 8,9,10'''.format(date1,date2)
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        LR = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['created_at','wh_id','product_id','sku','brand','cat','section','brand_id','cat_id','retailer_id','lr','oos_retailers'])
        LR.created_at = pd.to_datetime(LR.created_at)
        LR.wh_id = pd.to_numeric(LR.wh_id)
        LR.product_id = pd.to_numeric(LR.product_id)
        LR.lr = pd.to_numeric(LR.lr)
        LR.wh_id = pd.to_numeric(LR.wh_id)
        LR.cat_id = pd.to_numeric(LR.cat_id)
        LR.lr = pd.to_numeric(LR.lr)
        cur.close()
        conn.close()
        return LR
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
        
# Define a function to get retailers data
def get_retailers_data():
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        command_string = '''
        SELECT 
            retailers.id,
            retailer_polygon.district_id,
            case when regions.name_en in ('Delta West','Delta East') then 'Delta'
                 else regions.name_en end as region,
            cities.name_en as city,
            districts.name_ar as district,
            performance.segments.segment
        
        from retailers
        left join performance.segments on performance.segments.retailer_id = retailers.id
        left join retailer_polygon on retailer_polygon.retailer_id = retailers.id
        left join districts on districts.id = retailer_polygon.district_id
        left join cities on cities.id = districts.city_id
        left join states on states.id = cities.state_id
        left join regions on regions.id = states.region_id
        '''
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        r = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['retailer_id', 'district_id','region','city','district','segment'])
        r.retailer_id = pd.to_numeric(r.retailer_id)
        r.district_id = pd.to_numeric(r.district_id)
        r.loc[r.district_id.isna(),'region'] = None
        cur.close()
        conn.close()
        return r
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
# define a function to get so 
def get_so(date1,date2):
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        command_string = '''
        SELECT sales_orders.created_at::Date,
               coalesce(sales_orders.parent_sales_order_id,0) as parent_id,
               sales_orders.id,
               sales_orders.retailer_id,
               sales_orders.warehouse_id,
               sales_orders.district_id,
               sales_orders.maxab_offer_id,
               sales_orders.order_price,
               sales_orders.discount
        FROM sales_orders
        JOIN retailers on retailers.id = sales_orders.retailer_id
        JOIN districts on districts.id = sales_orders.district_id
        WHERE sales_orders.sales_order_status_id <= 6
          AND sales_orders.created_at::Date BETWEEN '{}'::date AND '{}'
          AND retailers.is_market_type_private = False
          AND sales_orders.order_price > 0
        '''.format(date1,date2)
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        pso = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['created_at', 'parent_id','id','retailer_id','wh_id','district_id','offer_id','order_price','discount'])
        pso.created_at = pd.to_datetime(pso.created_at)
        pso.id = pd.to_numeric(pso.id)
        pso.parent_id = pd.to_numeric(pso.parent_id)
        pso.wh_id = pd.to_numeric(pso.wh_id)
        pso.district_id = pd.to_numeric(pso.district_id)
        pso.retailer_id = pd.to_numeric(pso.retailer_id)
        pso.discount = pd.to_numeric(pso.discount)
        pso.order_price = pd.to_numeric(pso.order_price)
        pso.offer_id = pd.to_numeric(pso.offer_id)

        cur.close()
        conn.close()
        return pso
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
# Define a function to get PSO and Margins
def get_pso(date1,date2):
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        command_string = '''
        SELECT sales_orders.created_at::Date,
               coalesce(sales_orders.parent_sales_order_id,sales_orders.id) as id,
               sales_orders.retailer_id,
               sales_orders.warehouse_id,
               sales_orders.district_id,
               products.id,
               concat(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku,
               product_sales_order.packing_unit_id,
               brands.name_ar AS brand,
               brands.id as brand_id,
               categories.name_ar AS categories,
               categories.id AS cat_id,
               product_sales_order.purchased_item_count * product_sales_order.basic_unit_count as units,
               product_sales_order.total_price,
               sales_orders.discount / sales_orders.order_price * product_sales_order.total_price as discount,
               dynamic_bundle_sales_order_id,
               retailers.is_market_type_private,
               sales_orders.maxab_offer_id,
               coalesce(product_sales_order.purchased_item_count * product_sales_order.basic_unit_count * finance.all_cogs.wac4, product_sales_order.total_price) AS cogs
        FROM product_sales_order
        JOIN sales_orders ON sales_orders.id = product_sales_order.sales_order_id
        JOIN products ON products.id = product_sales_order.product_id
        JOIN brands ON brands.id = products.brand_id
        JOIN categories ON categories.id = products.category_id
        JOIN product_units ON product_units.id = products.unit_id
        JOIN retailers on retailers.id = sales_orders.retailer_id
        LEFT JOIN finance.all_cogs ON finance.all_cogs.product_id = product_sales_order.product_id
         AND sales_orders.delivered_at >= finance.all_cogs.from_date
         AND sales_orders.delivered_at < finance.all_cogs.to_date
        JOIN districts on districts.id = sales_orders.district_id
        WHERE sales_orders.sales_order_status_id <= 6
          AND sales_orders.created_at::Date BETWEEN '{}'::date AND '{}'
          AND retailers.is_market_type_private = False
          AND sales_orders.order_price > 0
        '''.format(date1,date2)
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        pso = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['created_at', 'id','retailer_id','wh_id','district_id','product_id','sku','packing_unit','brand','brand_id','cat','cat_id','units','total_price','discount','dynamic_id','is_private','offer_id','cogs'])
        pso.created_at = pd.to_datetime(pso.created_at)
        pso.packing_unit = pd.to_numeric(pso.packing_unit)
        pso.id = pd.to_numeric(pso.id)
        pso.product_id = pd.to_numeric(pso.product_id)
        pso.brand_id = pd.to_numeric(pso.brand_id)
        pso.cat_id = pd.to_numeric(pso.cat_id)
        pso.wh_id = pd.to_numeric(pso.wh_id)
        pso.district_id = pd.to_numeric(pso.district_id)
        pso.retailer_id = pd.to_numeric(pso.retailer_id)
        pso.total_price = pd.to_numeric(pso.total_price)
        pso.discount = pd.to_numeric(pso.discount)
        pso.units = pd.to_numeric(pso.units)
        pso.cogs = pd.to_numeric(pso.cogs)
        pso.offer_id = pd.to_numeric(pso.offer_id)

        cur.close()
        conn.close()
        return pso
    except Exception as e:
        cur.close()
        conn.close()
        print(e)

        
# Degine a function to get simplified PSO

def get_simplified_pso(date1,date2):
    try:
        import psycopg2
        import pandas as pd
        import os
        import numpy as np
        command_string = '''
        SELECT sales_orders.created_at::Date,
               coalesce(sales_orders.parent_sales_order_id,sales_orders.id) as id,
               sales_orders.retailer_id,
               sales_orders.warehouse_id,
               sales_orders.district_id,
               products.id,
               concat(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku,
               product_sales_order.packing_unit_id,
               brands.name_ar AS brand,
               brands.id as brand_id,
               categories.name_ar AS categories,
               categories.id AS cat_id,
               product_sales_order.purchased_item_count * product_sales_order.basic_unit_count as units,
               product_sales_order.total_price
        FROM product_sales_order
        JOIN sales_orders ON sales_orders.id = product_sales_order.sales_order_id
        JOIN products ON products.id = product_sales_order.product_id
        JOIN brands ON brands.id = products.brand_id
        JOIN categories ON categories.id = products.category_id
        JOIN product_units ON product_units.id = products.unit_id
        JOIN retailers on retailers.id = sales_orders.retailer_id
        JOIN districts on districts.id = sales_orders.district_id
        WHERE sales_orders.sales_order_status_id <= 6
          AND sales_orders.created_at::Date BETWEEN '{}'::date AND '{}'
          AND retailers.is_market_type_private = False
          AND sales_orders.order_price > 0
        '''.format(date1,date2)
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        pso = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['created_at', 'id','retailer_id','wh_id','district_id','product_id','sku','packing_unit','brand','brand_id','cat','cat_id','units','total_price'])
        pso.created_at = pd.to_datetime(pso.created_at)
        pso.packing_unit = pd.to_numeric(pso.packing_unit)
        pso.id = pd.to_numeric(pso.id)
        pso.product_id = pd.to_numeric(pso.product_id)
        pso.brand_id = pd.to_numeric(pso.brand_id)
        pso.cat_id = pd.to_numeric(pso.cat_id)
        pso.wh_id = pd.to_numeric(pso.wh_id)
        pso.district_id = pd.to_numeric(pso.district_id)
        pso.retailer_id = pd.to_numeric(pso.retailer_id)
        pso.total_price = pd.to_numeric(pso.total_price)

        cur.close()
        conn.close()
        return pso
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
        

# Define a function to get offers relevant data

def get_offers_data(date1,date2):
    
    import psycopg2
    import pandas as pd
    import os
    import numpy as np
    from datetime import datetime, timedelta, date
    import datetime
    try:
        command_string = '''
        Select retailer_polygon.retailer_id,
               retailer_polygon.district_id,
               regions.name_en as region
        from materialized_views.retailer_polygon retailer_polygon
        join districts on districts.id = retailer_polygon.district_id
        join cities on cities.id = districts.city_id
        join states on states.id = cities.state_id
        join regions on regions.id = states.region_id
        '''.format(date1,date2)
        conn = psycopg2.connect(host=os.environ["DWH_READER_HOST"], database=os.environ["DWH_READER_NAME"], user=os.environ["DWH_READER_USER_NAME"], password=os.environ["DWH_READER_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        r = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['retailer_id', 'district_id','region'])
        r.district_id = pd.to_numeric(r.district_id)
        r.retailer_id = pd.to_numeric(r.retailer_id)

        cur.close()
        conn.close()
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
        
    try:
        command_string = '''
        SELECT sales_orders.created_at::Date,
               sales_orders.id,
               sales_orders.retailer_id,
               sales_orders.warehouse_id,
               sales_orders.district_id,
               case when districts.city_id <= 17 then 'Cairo'
               else 'Delta' end as region,
               products.id,
               concat(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku,
               product_sales_order.packing_unit_id,
               brands.name_ar AS brand,
               brands.id as brand_id,
               categories.name_ar AS categories,
               categories.id AS cat_id,
               product_sales_order.purchased_item_count * product_sales_order.basic_unit_count as units,
               product_sales_order.total_price,
               sales_orders.discount / sales_orders.order_price * product_sales_order.total_price as discount,
               dynamic_bundle_sales_order_id,
               retailers.is_market_type_private,
               sales_orders.maxab_offer_id,
               coalesce(product_sales_order.purchased_item_count * product_sales_order.basic_unit_count * finance.global_cogs_rolling.cost_of_item, product_sales_order.total_price) AS cogs
        FROM product_sales_order
        JOIN sales_orders ON sales_orders.id = product_sales_order.sales_order_id
        JOIN products ON products.id = product_sales_order.product_id
        JOIN brands ON brands.id = products.brand_id
        JOIN categories ON categories.id = products.category_id
        JOIN product_units ON product_units.id = products.unit_id
        JOIN retailers on retailers.id = sales_orders.retailer_id
        LEFT JOIN finance.global_cogs_rolling ON finance.global_cogs_rolling.product_id = product_sales_order.product_id
         AND sales_orders.delivered_at >= finance.global_cogs_rolling.from_date
         AND sales_orders.delivered_at < finance.global_cogs_rolling.to_date
        JOIN districts on districts.id = sales_orders.district_id
        WHERE sales_orders.sales_order_status_id <= 6
          AND sales_orders.created_at::Date BETWEEN '{}'::date - interval '30 days' AND '{}'
          AND retailers.is_market_type_private = False
          --and product_sales_order.purchased_item_count > 0
          AND sales_orders.order_price > 0
        '''.format(date1,date2)
        conn = psycopg2.connect(host=os.environ["DB_HOST"], database=os.environ["DB_NAME"], user=os.environ["DB_USER_NAME"], password=os.environ["DB_PASSWORD"])
        cur = conn.cursor()
        cur.execute(command_string)
        pso = pd.DataFrame(np.array(cur.fetchall()),
                            columns=['created_at', 'id','retailer_id','wh_id','district_id','region','product_id','sku','packing_unit','brand','brand_id','cat','cat_id','units','total_price','discount','dynamic_id','is_private','offer_id','cogs'])
        pso.created_at = pd.to_datetime(pso.created_at)
        pso.packing_unit = pd.to_numeric(pso.packing_unit)
        pso.id = pd.to_numeric(pso.id)
        pso.product_id = pd.to_numeric(pso.product_id)
        pso.brand_id = pd.to_numeric(pso.brand_id)
        pso.cat_id = pd.to_numeric(pso.cat_id)
        pso.wh_id = pd.to_numeric(pso.wh_id)
        pso.district_id = pd.to_numeric(pso.district_id)
        pso.retailer_id = pd.to_numeric(pso.retailer_id)
        pso.total_price = pd.to_numeric(pso.total_price)
        pso.discount = pd.to_numeric(pso.discount)
        pso.units = pd.to_numeric(pso.units)
        pso.cogs = pd.to_numeric(pso.cogs)
        pso.offer_id = pd.to_numeric(pso.offer_id)

        cur.close()
        conn.close()
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
    
    query= '''
    select 
            area.ids as areas,
            offers.*,
            ts.ticket_size_minimum_value,
            channel.channels
    from
    (
    select
    offer.white_list_retailer_ids,
    offer.id,
    offer.active_from,
    offer.active_to,
    offer.legacy_id,
    offer.description,
    offer.name,
    offer.rule_id,

    STRING_AGG(cast(all_of_brands_rule_item.type_id as text),', ') as all_brands,
    STRING_AGG(cast(all_of_brands_rule_item.minimum_gmv as text),', ') as all_brands_minimum_gmv,
    STRING_AGG(cast(all_of_brands_rule_item.minimum_skus as text),', ') as all_brands_minimum_skus,

    STRING_AGG(cast(one_of_brands_rule_item.type_id as text),', ') as one_brands,
    STRING_AGG(cast(one_of_brands_rule_item.minimum_gmv as text),', ') as one_brands_minimum_gmv,
    STRING_AGG(cast(one_of_brands_rule_item.minimum_skus as text),', ') as one_brands_minimum_skus,
    --
    STRING_AGG(cast(all_of_categories_rule_item.type_id as text),', ') as all_categories,
    STRING_AGG(cast(all_of_categories_rule_item.minimum_gmv as text),', ') as all_categories_minimum_gmv,
    STRING_AGG(cast(all_of_categories_rule_item.minimum_skus as text),', ') as all_categories_minimum_skus,

    STRING_AGG(cast(one_of_categories_rule_item.type_id as text),', ') as one_categories,
    STRING_AGG(cast(one_of_categories_rule_item.minimum_gmv as text),', ') as one_categories_minimum_gmv,
    STRING_AGG(cast(one_of_categories_rule_item.minimum_skus as text),', ') as one_categories_minimum_skus,
    --
    STRING_AGG(cast(all_of_skus_rule_item.type_id as text),', ') as all_skus,
    STRING_AGG(cast(all_of_skus_rule_item.minimum_gmv as text),', ') as all_skus_minimum_gmv,

    STRING_AGG(cast(one_of_skus_rule_item.type_id as text),', ') as one_skus,
    STRING_AGG(cast(one_of_skus_rule_item.minimum_gmv as text),', ') as one_skus_minimum_gmv

    from offers.offer as offer
    left join offers.rule as rule on rule.parent_rule_id = offer.rule_id
    and rule.dtype in ('AllOfSkusRule',
                        'OneOfBrandsRule',
                        'OneOfCategoriesRule',
                        'OneOfSkusRule',
                        'AllOfBrandsRule',
                        'AllOfCategoriesRule')
    left join offers.all_of_brands_rule_item as all_of_brands_rule_item on all_of_brands_rule_item.all_of_brands_rule_id = rule.id
    left join offers.one_of_brands_rule_item as one_of_brands_rule_item on one_of_brands_rule_item.one_of_brands_rule_id = rule.id

    left join offers.all_of_categories_rule_item as all_of_categories_rule_item on all_of_categories_rule_item.all_of_categories_rule_id = rule.id
    left join offers.one_of_categories_rule_item as one_of_categories_rule_item on one_of_categories_rule_item.one_of_categories_rule_id = rule.id

    left join offers.all_of_skus_rule_item as all_of_skus_rule_item on all_of_skus_rule_item.all_of_skus_rule_id = rule.id
    left join offers.one_of_skus_rule_item as one_of_skus_rule_item on one_of_skus_rule_item.one_of_skus_rule_id = rule.id

    where offer.active_from::date between '{}' and '{}'
          or offer.active_to::date between '{}' and '{}'
          or (offer.active_from::date <= '{}' and offer.active_to >= '{}')
    group by 1,2,3,4,5,6,7,8
    order by offer.id) as offers
    left join 
    (select parent_rule_id,
           ticket_size_minimum_value
    from offers.rule as rule 
    where dtype = 'TicketSizeRule') as ts on ts.parent_rule_id = offers.rule_id
    left join 
    (select parent_rule_id,
           channels
    from offers.rule as rule 
    where dtype = 'ChannelIncludedRule') as channel on channel.parent_rule_id = offers.rule_id
    left join 
    (select parent_rule_id,
           ids
    from offers.rule as rule
    where dtype = 'AreaIncludedRule') as area on area.parent_rule_id = offers.rule_id
    '''.format(date1,date2,date1,date2,date1,date2)    
    try:
        conn = psycopg2.connect(host=os.environ["DWH_READER_HOST"], database=os.environ["DWH_READER_NAME"], user=os.environ["DWH_READER_USER_NAME"], password=os.environ["DWH_READER_PASSWORD"])
        cur = conn.cursor()
        cur.execute(query)
        offers = pd.DataFrame(np.array(cur.fetchall()))

        cur.close()
        conn.close()
    except Exception as c:
        cur.close()
        conn.close()
        print(c)
    offers.columns = ['areas','white_list_retailer_ids','id','active_from','active_to','legacy_id','description','name','rule_id','all_brands','all_brands_minimum_gmv','all_brands_minimum_skus','one_brands','one_brands_minimum_gmv','one_brands_minimum_skus','all_categories','all_categories_minimum_gmv','all_categories_minimum_skus','one_categories','one_categories_minimum_gmv','one_categories_minimum_skus','all_skus','all_skus_minimum_gmv','one_skus','one_skus_minimum_gmv','ticket_size_minimum_value','channels']
    offers.active_from = pd.to_datetime(offers.active_from).dt.tz_localize(None)
    offers.active_to = pd.to_datetime(offers.active_to).dt.tz_localize(None)
    offer_area = offers[['id','active_from','areas']].dropna().reset_index(drop = True)
    offer_area = offer_area.explode('areas')
    
    area_offers_retailers = pd.DataFrame()
    for d in offer_area.active_from.unique():
        t1 = pso.loc[(pso.created_at.dt.date >= (pd.Timestamp(d) - timedelta(days = 30))) & (pso.created_at.dt.date < pd.Timestamp(d))][['retailer_id','district_id']].drop_duplicates()
        t2 = offer_area.loc[offer_area.active_from == d]
        t2.areas = pd.to_numeric(t2.areas)
        rows = pd.merge(t2.rename(columns = {'areas':'district_id'}),t1,on = ['district_id']).reset_index(drop = True)
        rows.drop(columns = ['active_from'],inplace = True)
        area_offers_retailers = area_offers_retailers.append(rows)
    try:
        area_offers_retailers = pd.merge(area_offers_retailers,r[['retailer_id','region']],on = 'retailer_id')
    except:
        area_offers_retailers = pd.DataFrame(columns = ['id','retailer_id','region'])
        print('no area offers in selected period')
    offer_wl = offers[['id','active_from','white_list_retailer_ids']].dropna().reset_index(drop = True)
    offer_wl = offer_wl.explode('white_list_retailer_ids')
    offer_wl.white_list_retailer_ids = pd.to_numeric(offer_wl.white_list_retailer_ids)

    wl_offers_retailers = offer_wl[['id','white_list_retailer_ids']].rename(columns = {'white_list_retailer_ids':'retailer_id'})
    
    wl_offers_retailers = pd.merge(wl_offers_retailers,r[['retailer_id','region']],on = 'retailer_id')
    
    offers_all_brands = offers[['id','all_brands']].dropna()
    offers_all_brands.all_brands = offers_all_brands['all_brands'].apply(lambda x: x.split(','))
    offers_all_brands = offers_all_brands.explode('all_brands')
    offers_all_brands.all_brands = pd.to_numeric(offers_all_brands.all_brands)

    x1 = offers[['all_brands_minimum_gmv']].dropna()
    x1['all_brands_minimum_gmv'] = x1['all_brands_minimum_gmv'].apply(lambda x: x.split(','))
    x1 = x1.explode('all_brands_minimum_gmv')
    x1.all_brands_minimum_gmv = pd.to_numeric(x1.all_brands_minimum_gmv)

    x2 = offers[['all_brands_minimum_skus']].dropna()
    x2['all_brands_minimum_skus'] = x2['all_brands_minimum_skus'].apply(lambda x: x.split(','))
    x2 = x2.explode('all_brands_minimum_skus')
    x2.all_brands_minimum_skus = pd.to_numeric(x2.all_brands_minimum_skus)

    try:
        offers_all_brands = offers_all_brands.reset_index(drop = True).join(x1.reset_index(drop = True)).join(x2.reset_index(drop = True))
    except:
        offers_all_brands[['all_brands_minimum_gmv','all_brands_minimum_skus']] = np.nan
        
    offers_one_brands = offers[['id','one_brands']].dropna()
    offers_one_brands.one_brands = offers_one_brands['one_brands'].apply(lambda x: x.split(','))
    offers_one_brands = offers_one_brands.explode('one_brands')
    offers_one_brands.one_brands = pd.to_numeric(offers_one_brands.one_brands)

    x1 = offers[['one_brands_minimum_gmv']].dropna()
    x1['one_brands_minimum_gmv'] = x1['one_brands_minimum_gmv'].apply(lambda x: x.split(','))
    x1 = x1.explode('one_brands_minimum_gmv')
    x1.one_brands_minimum_gmv = pd.to_numeric(x1.one_brands_minimum_gmv)

    x2 = offers[['one_brands_minimum_skus']].dropna()
    x2['one_brands_minimum_skus'] = x2['one_brands_minimum_skus'].apply(lambda x: x.split(','))
    x2 = x2.explode('one_brands_minimum_skus')
    x2.one_brands_minimum_skus = pd.to_numeric(x2.one_brands_minimum_skus)

    try:
        offers_one_brands = offers_one_brands.reset_index(drop = True).join(x1.reset_index(drop = True)).join(x2.reset_index(drop = True))
    except:
        offers_one_brands[['one_brands_minimum_gmv','one_brands_minimum_skus']] = np.nan
        
    offers_all_cats = offers[['id','all_categories']].dropna()
    offers_all_cats.all_categories = offers_all_cats['all_categories'].apply(lambda x: x.split(','))
    offers_all_cats = offers_all_cats.explode('all_categories')
    offers_all_cats.all_categories = pd.to_numeric(offers_all_cats.all_categories)

    x1 = offers[['all_categories_minimum_gmv']].dropna()
    x1['all_categories_minimum_gmv'] = x1['all_categories_minimum_gmv'].apply(lambda x: x.split(','))
    x1 = x1.explode('all_categories_minimum_gmv')
    x1.all_categories_minimum_gmv = pd.to_numeric(x1.all_categories_minimum_gmv)

    x2 = offers[['all_categories_minimum_skus']].dropna()
    x2['all_categories_minimum_skus'] = x2['all_categories_minimum_skus'].apply(lambda x: x.split(','))
    x2 = x2.explode('all_categories_minimum_skus')
    x2.all_categories_minimum_skus = pd.to_numeric(x2.all_categories_minimum_skus)

    try:
        offers_all_cats = offers_all_cats.reset_index(drop = True).join(x1.reset_index(drop = True)).join(x2.reset_index(drop = True))
    except:
        offers_all_cats[['all_categories_minimum_gmv','all_categories_minimum_skus']] = np.nan
        
    offers_one_cats = offers[['id','one_categories']].dropna()
    offers_one_cats.one_categories = offers_one_cats['one_categories'].apply(lambda x: x.split(','))
    offers_one_cats = offers_one_cats.explode('one_categories')
    offers_one_cats.one_categories = pd.to_numeric(offers_one_cats.one_categories)

    x1 = offers[['one_categories_minimum_gmv']].dropna()
    x1['one_categories_minimum_gmv'] = x1['one_categories_minimum_gmv'].apply(lambda x: x.split(','))
    x1 = x1.explode('one_categories_minimum_gmv')
    x1.one_categories_minimum_gmv = pd.to_numeric(x1.one_categories_minimum_gmv)

    x2 = offers[['one_categories_minimum_skus']].dropna()
    x2['one_categories_minimum_skus'] = x2['one_categories_minimum_skus'].apply(lambda x: x.split(','))
    x2 = x2.explode('one_categories_minimum_skus')
    x2.one_categories_minimum_skus = pd.to_numeric(x2.one_categories_minimum_skus)

    try:
        offers_one_cats = offers_one_cats.reset_index(drop = True).join(x1.reset_index(drop = True)).join(x2.reset_index(drop = True))
    except:
        offers_one_cats[['one_categories_minimum_gmv','one_categories_minimum_skus']] = np.nan
        
    offers_all_skus = offers[['id','all_skus']].dropna()
    offers_all_skus.all_skus = offers_all_skus['all_skus'].apply(lambda x: x.split(','))
    offers_all_skus = offers_all_skus.explode('all_skus')
    offers_all_skus.all_skus = pd.to_numeric(offers_all_skus.all_skus)

    x1 = offers[['all_skus_minimum_gmv']].dropna()
    x1['all_skus_minimum_gmv'] = x1['all_skus_minimum_gmv'].apply(lambda x: x.split(','))
    x1 = x1.explode('all_skus_minimum_gmv')
    x1.all_skus_minimum_gmv = pd.to_numeric(x1.all_skus_minimum_gmv)

    try:
        offers_all_skus = offers_all_skus.reset_index(drop = True).join(x1.reset_index(drop = True))
    except:
        offers_all_skus[['all_skus_minimum_gmv']] = np.nan
        
    offers_one_skus = offers[['id','one_skus']].dropna()
    offers_one_skus.one_skus = offers_one_skus['one_skus'].apply(lambda x: x.split(','))
    offers_one_skus = offers_one_skus.explode('one_skus')
    offers_one_skus.one_skus = pd.to_numeric(offers_one_skus.one_skus)

    x1 = offers[['one_skus_minimum_gmv']].dropna()
    x1['one_skus_minimum_gmv'] = x1['one_skus_minimum_gmv'].apply(lambda x: x.split(','))
    x1 = x1.explode('one_skus_minimum_gmv')
    x1.one_skus_minimum_gmv = pd.to_numeric(x1.one_skus_minimum_gmv)

    try:
        offers_one_skus = offers_one_skus.reset_index(drop = True).join(x1.reset_index(drop = True))
    except:
        offers_one_skus[['all_one_minimum_gmv']] = np.nan
        
    offers_ts = offers[['id','ticket_size_minimum_value']].dropna().reset_index(drop = True)
    
    summary = offers[['id','active_from','active_to','description','name','channels']].copy()
    summary['wl'] = np.where(summary.id.isin(wl_offers_retailers.id),1,0)
    summary['area_wl'] = np.where(summary.id.isin(area_offers_retailers.id),1,0)
    summary['ts'] = np.where(summary.id.isin(offers_ts.id),1,0)

    summary['one_skus'] = np.where(summary.id.isin(offers_one_skus.id),1,0)
    summary['all_skus'] = np.where(summary.id.isin(offers_all_skus.id),1,0)
    summary['one_brands'] = np.where(summary.id.isin(offers_one_brands.id),1,0)
    summary['all_brands'] = np.where(summary.id.isin(offers_all_brands.id),1,0)
    summary['one_cats'] = np.where(summary.id.isin(offers_one_cats.id),1,0)
    summary['all_cats'] = np.where(summary.id.isin(offers_all_cats.id),1,0)
    
    return area_offers_retailers,wl_offers_retailers,offers_one_skus,offers_all_skus,offers_one_brands,offers_all_brands,offers_one_cats,offers_all_cats,offers_ts,summary

# Define a function to send text message on slack
def send_text_slack(channel,text):
    import slack
    import os
    client = slack.WebClient(token=os.environ["SLACK_TOKEN"])
    try:
        response = client.chat_postMessage(
        channel=channel,
        text=text
      )
        print('Message Sent')
    except SlackApiError as e:
      # You will get a SlackApiError if "ok" is False
      assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        
        
# Define a function to send an excel file on slack

def send_file_slack(dfs,sheet_names,file_name,channel,comment):
    import slack
    import os
    import pandas as pd
    try:
        writer = pd.ExcelWriter(file_name, engine = 'xlsxwriter')
        for c,i in enumerate(dfs):
            i.to_excel(writer, sheet_name = sheet_names[c])
        writer.save()

        slack_client = slack.WebClient(token=os.environ["SLACK_TOKEN"])

        slack_response = slack_client.files_upload(    
            file=file_name,
            initial_comment=comment,
            channels=channel
        )

        assert slack_response['ok']
        os.remove(file_name)
        print('File Sent')
    except Exception as e:
        print('error')
        print(e)

        
def connect_athena():
    from pyathena import connect
    import pandas as pd
    conn = connect(s3_staging_dir='s3://athena-results-for-events-data',
               region_name='us-east-1')
    return conn

