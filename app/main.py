from fastapi import FastAPI
from dotenv import dotenv_values
import boto3
import botocore

from typing import List

from pydantic import Field
from model import Glue_job
from model import SuccessResponse, ErrorResponse, ExceptionResponse

config = dotenv_values(".env")
ACCESS_ID = config.get("aws_access_key_id")
ACCESS_KEY = config.get("aws_secret_access_key")
REGION = config.get("aws_region")


client = boto3.client('glue', aws_access_key_id=ACCESS_ID,
                      aws_secret_access_key=ACCESS_KEY, region_name=REGION)

app = FastAPI(openapi_url="/job/openapi.json",docs_url="/job/docs")

@app.post("/job/create_job", response_model=SuccessResponse)
async def create_job(glue:Glue_job):
    """
    This endpoint creates a Glue job.
    """
    try:
        response = client.create_job(
            Name=glue.Name,
            Role=glue.Role,
            Command=dict(glue.Command)
           )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'IdempotentParameterMismatchException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ConcurrentModificationException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()

@app.get("/job/get_jobs")
async def get_jobs():
    """
    This endpoint returns the all Glue jobs.
    """
    try:
        response = client.get_jobs(
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'],data=response['Jobs'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.get("/job/get_job/{Name}")
async def get_job(Name: str): 
    """
    This endpoint return the specific Glue job.
    """
    try:
        response = client.get_job(
            JobName=Name
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response['Job'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.delete("/job/delete_job/{Name}")
async def delete_job(Name: str): 
    """
    This endpoint delete a Glue job.
    """
    try:
        response = client.delete_job(
            JobName=Name
        )
        print('re', response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()

@app.put("/job/update_job")
async def update_job(glue:Glue_job):
    """
    This endpoint updates a Glue job.
    """
    try:
        response = client.update_job(
            JobName=glue.Name,
            JobUpdate={
                'Role':glue.Role,
                'Command':dict(glue.Command),
                "ExecutionProperty": {
                    "MaxConcurrentRuns": 1
                },
                "WorkerType": "G.1X",
                "NumberOfWorkers": 10,
                "ExecutionClass": "STANDARD",
                "DefaultArguments": {
                    "--enable-metrics": "true",
                    "--enable-spark-ui": "true",
                    "--spark-event-logs-path": "s3://aws-glue-assets-300289082521-us-east-1/sparkHistoryLogs/",
                    "--enable-job-insights": "false",
                    "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "--enable-glue-datacatalog": "true",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--datalake-formats": "delta",
                    "--job-language": "python",
                    "--TempDir": "s3://aws-glue-assets-300289082521-us-east-1/temporary/"
                },
            }
            
           )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ConcurrentModificationException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()

@app.get("/job/start_job_run/{JobName}")
async def start_job_run(JobName: str): 
    """
    This endpoint start the specific Glue job.
    """
    try:
        response = client.start_job_run(
            JobName=JobName
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ConcurrentRunsExceededException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.get("/job/get_job_run/{JobName}/{RunId}")
async def get_job_run(JobName: str, RunId: str= Field(min_length=1, max_length=255)): 
    """
    This endpoint return the specific Glue job running status.
    """
    try:
        response = client.get_job_run(
            JobName=JobName,
            RunId=RunId
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response['JobRun'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.get("/job/get_job_runs/{JobName}")
async def get_job_runs(JobName: str): 
    """
    This endpoint return the specific Glue job runs history.
    """
    try:
        response = client.get_job_runs(
            JobName=JobName
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()
