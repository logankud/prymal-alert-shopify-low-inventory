import boto3 
import botocore
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
import loguru
from loguru import logger
import os
import pandas as pd
import numpy as np
from datetime import timedelta


AWS_ACCESS_KEY=os.environ['AWS_ACCESS_KEY']
AWS_ACCESS_SECRET=os.environ['AWS_ACCESS_SECRET']


# ---------------------------------------
# FUNCTIONS
# ---------------------------------------

# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY,
                                 aws_secret_access_key=AWS_ACCESS_SECRET)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] for r in row['Data']] for row in data_rows])



        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions





# ========================================================================
# Execute Code
# ========================================================================


yesterday_date = pd.to_datetime(pd.to_datetime('today') - timedelta(1)).strftime('%Y-%m-%d')

DATABASE = 'prymal-analytics'
REGION = 'us-east-1'

# Construct query to pull data by product
# ----

QUERY = f"""SELECT *
            FROM shopify_inventory_report 
            
            WHERE partition_date = DATE('{yesterday_date}')
            AND days_of_stock_onhand <= 30
            """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
result_df.columns = ['sku','sku_name','forecast','lower_bound','upper_bound','inventory_on_hand','days_of_stock_onhand','partition_date']

# Set data type
result_df['days_of_stock_onhand'] = result_df['days_of_stock_onhand'].astype(int)


logger.info('Constructing alert details string')

# Costruct email alert
# -----------

alert_details = ''
for r in range(len(result_df)):

    product = result_df.loc[r,'sku_name']
    stock_days = result_df.loc[r,'days_of_stock_onhand']


    if stock_days == 0:

        alert_details += f'{product} : Out of stock \n'

    else:

        alert_details += f'{product} : ~{stock_days} days of stock on hand \n'


# Initialize a boto3 client for SNS
sns_client = boto3.client('sns', region_name='us-east-1')  

# Specify the SNS topic ARN
topic_arn = 'arn:aws:sns:us-east-1:925570149811:prymal_alerts'  

# Email subject and message
subject = "Shopify Low Stock Alert"

message = f'The following core flavors are low stock (less than 30 days of inventory on hand): \n \n {alert_details}'

logger.info('Sending SNS alert')

# Publish a message to the SNS topic
response = sns_client.publish(
    TopicArn=topic_arn,
    Message=message,
    Subject=subject
)

logger.info(f'SNS Response: {response}')