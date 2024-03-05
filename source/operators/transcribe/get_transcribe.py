# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import urllib3
import json
from botocore import config
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from MediaInsightsEngineLambdaHelper import MediaInsightsOperationHelper
from MediaInsightsEngineLambdaHelper import MasExecutionError
from MediaInsightsEngineLambdaHelper import DataPlane
import codecs

patch_all()

region = os.environ['AWS_REGION']

mie_config = json.loads(os.environ['botoConfig'])
config = config.Config(**mie_config)
transcribe = boto3.client("transcribe", config=config)

dynamodb_resource = boto3.resource('dynamodb')


def check_ddb_status(job_id, ddbtable_name):
    # Retrieve the status from DynamoDB
    ddbtable = dynamodb_resource.Table(ddbtable_name)
    try:
        response = ddbtable.get_item(
            Key={
                'job_id': job_id
            }
        )
        # Check if item exists and has result field
        if 'Item' in response and 'result' in response['Item']:
            return response['Item']['result']
        else:
            return 'error'
    except ClientError as e:
        print(f'Error in retrieving status from DynamoDB: {str(e)}')
        return 'error'


def get_output_txtkey_from_ddb(job_id, ddbtable_name):
    """
    Retrieves the output_txtkey from the DynamoDB table and returns the key.
    """
    ddbtable = dynamodb_resource.Table(ddbtable_name)
    try:
        response = ddbtable.get_item(
            Key={
                'job_id': job_id
            }
        )
        # Check if item exists and has output_txtkey field
        if 'Item' in response and 'output_txtkey' in response['Item']:
            return response['Item']['output_txtkey']
        else:
            return 'error'
    except Exception as e:
        print(f'Error in retrieving output_txtkey from DynamoDB: {str(e)}')
        return 'error'


def read_text_from_file(file_key, bucket_name):
    """
    Read and return the content of the text file from S3 bucket. The text is in Arabic.
    """
    s3 = boto3.client('s3')
    try:
        # Getting the file object from S3
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        # Reading the file content in bytes
        file_content = file_obj['Body'].read()
        # Decoding the bytes to string using utf-8 encoding for Arabic text
        return codecs.decode(file_content, 'utf-8')
    except Exception as e:
        print(f'Error in reading the file from S3: {str(e)}')
        return 'error'


def get_output_jsonkey_from_ddb(job_id, ddbtable_name):
    """
    Retrieves the output_jsonkey from the DynamoDB table and returns the key.
    """
    dynamodb_resource = boto3.resource('dynamodb')
    ddbtable = dynamodb_resource.Table(ddbtable_name)

    try:
        response = ddbtable.get_item(
            Key={
                'job_id': job_id
            }
        )
        # Check if item exists and has output_jsonkey field
        if 'Item' in response and 'output_jsonkey' in response['Item']:
            return response['Item']['output_jsonkey']
        else:
            return 'error'
    except Exception as e:
        print(f'Error in retrieving output_jsonkey from DynamoDB: {str(e)}')
        return 'error'


def read_json_from_file(file_key, bucket_name):
    """
    Read and return the content of the json file from S3 bucket.
    """
    s3 = boto3.client('s3')
    try:
        # Getting the file object from S3
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        # Reading the file content in bytes
        file_content = file_obj['Body'].read()
        # Decoding the bytes to string
        json_content = file_content.decode('utf-8')
        # Convert the JSON string to a Python dictionary
        return json.loads(json_content)
    except Exception as e:
        print(f'Error in reading the JSON file from S3: {str(e)}')
        return 'error'


def transform_whisper_json_output(input_data):
    transformed_output = {
        "jobName": "transcribe-a1564bf1-bd38-4054-b4fd-8e877cfef0d7",
        "accountId": "014661450282",
        "results": {
            "transcripts": [{"transcript": input_data.get("text", "")}],
            "items": []
        }
    }

    for segment in input_data.get("segments", []):
        start_time = str(segment.get("start", 0.0))
        end_time = str(segment.get("end", 0.0))
        text = segment.get("text", "")

        item = {
            "start_time": start_time,
            "end_time": end_time,
            "alternatives": [{"confidence": "0.995", "content": text}],
            "type": "pronunciation"
        }

        transformed_output["results"]["items"].append(item)

    return transformed_output


def lambda_handler(event, context):
    print("We got this event:\n", event)
    operator_object = MediaInsightsOperationHelper(event)

    job_id = event['MetaData']['TranscribeJobId']
    asset_id = event['MetaData']['AssetId']
    workflow_id = event['MetaData']['WorkflowExecutionId']

    try:
        if "Audio" in event["Input"]["Media"]:
            wbucket = event["Input"]["Media"]["Audio"]["S3Bucket"]
            wkey = event["Input"]["Media"]["Audio"]["S3Key"]
        elif "Video" in event["Input"]["Media"]:
            wbucket = event["Input"]["Media"]["Video"]["S3Bucket"]
            wkey = event["Input"]["Media"]["Video"]["S3Key"]

    except Exception:
        operator_object.update_workflow_status("Error")
        operator_object.add_workflow_metadata(TranscribeError="No valid inputs")
        raise MasExecutionError(operator_object.return_output_object())

    if "whisper" in event['MetaData']['TranscribeJobId']:
        print('whisper in TranscribeJobId')
        # prefix = wkey.rsplit('/', 1)[0]
        # file_name = f'{job_id}.json'

        ddbtable_name = os.environ['DynamodbTableName']
        if ddbtable_name:
            ddb_status = check_ddb_status(
                job_id,
                ddbtable_name
            )
            if ddb_status == 'complete':
                print("ddb_status == 'complete'")
                operator_object.update_workflow_status("Complete")
                dataplane = DataPlane()
                # Get the output keys from DynamoDB
                output_txtkey = get_output_txtkey_from_ddb(job_id, ddbtable_name)
                output_jsonkey = get_output_jsonkey_from_ddb(job_id, ddbtable_name)

                # If the output_jsonkey was successfully retrieved, read the JSON content from S3
                if (output_jsonkey != 'error') and (output_txtkey != 'error'):
                    print("(output_jsonkey != 'error') and (output_txtkey != 'error')")
                    json_content = read_json_from_file(output_jsonkey, wbucket)

                    json_content_processed = transform_whisper_json_output(json_content)

                    json_content_processed["TextTranscriptUri"] = {"S3Bucket": wbucket, "S3Key": output_txtkey}

                    transcript_storage_path = dataplane.generate_media_storage_path(asset_id, workflow_id)

                    print('json_content_processed')
                    print(json_content_processed)

                    metadata_upload = dataplane.store_asset_metadata(
                        asset_id,
                        operator_object.name,
                        workflow_id,
                        json_content_processed
                    )

                    print('dataplane.retrieve_asset_metadata')
                    print(dataplane.retrieve_asset_metadata(
                        asset_id=asset_id
                        # operator=operator_object.name
                    ))

                    print('metadata_upload')
                    print(metadata_upload)
                    operator_object.add_media_object('Text', metadata_upload['Bucket'], metadata_upload['Key'])
                    operator_object.add_workflow_metadata(TranscribeJobId=job_id)
                    operator_object.update_workflow_status("Complete")
                    print("operator_object.return_output_object()")
                    print(operator_object.return_output_object())
                    return operator_object.return_output_object()
                else:
                    operator_object.update_workflow_status("Error")
                    operator_object.add_workflow_metadata(TranscribeError="Whisper Error {e}".format(e=e))
                    raise MasExecutionError(operator_object.return_output_object())
            elif ddb_status == 'started':
                operator_object.update_workflow_status("Executing")
                operator_object.add_workflow_metadata(
                    TranscribeJobId=job_id,
                    AssetId=asset_id,
                    WorkflowExecutionId=workflow_id)
                return operator_object.return_output_object()
            else:
                operator_object.update_workflow_status("Error")
                operator_object.add_workflow_metadata(TranscribeError="Whisper Error {e}".format(e=e))
                raise MasExecutionError(operator_object.return_output_object())

    else:
        # If Transcribe wasn't run due to silent audio, then we're done
        if "Mediainfo_num_audio_tracks" in event["Input"]["MetaData"] and event["Input"]["MetaData"][
            "Mediainfo_num_audio_tracks"] == "0":
            operator_object.update_workflow_status("Complete")
            return operator_object.return_output_object()
        try:
            job_id = operator_object.metadata["TranscribeJobId"]
            workflow_id = operator_object.workflow_execution_id
            asset_id = operator_object.asset_id
        except KeyError as e:
            operator_object.update_workflow_status("Error")
            operator_object.add_workflow_metadata(TranscribeError="Missing a required metadata key {e}".format(e=e))
            raise MasExecutionError(operator_object.return_output_object())
        try:
            response = transcribe.get_transcription_job(
                TranscriptionJobName=job_id
            )
            print(response)
        except Exception as e:
            operator_object.update_workflow_status("Error")
            operator_object.add_workflow_metadata(TranscribeError=str(e), TranscribeJobId=job_id)
            raise MasExecutionError(operator_object.return_output_object())
        else:
            if response["TranscriptionJob"]["TranscriptionJobStatus"] == "IN_PROGRESS":
                operator_object.update_workflow_status("Executing")
                operator_object.add_workflow_metadata(TranscribeJobId=job_id, AssetId=asset_id,
                                                      WorkflowExecutionId=workflow_id)
                return operator_object.return_output_object()
            elif response["TranscriptionJob"]["TranscriptionJobStatus"] == "FAILED":
                operator_object.update_workflow_status("Error")
                operator_object.add_workflow_metadata(TranscribeJobId=job_id,
                                                      TranscribeError=str(
                                                          response["TranscriptionJob"]["FailureReason"]))
                raise MasExecutionError(operator_object.return_output_object())
            elif response["TranscriptionJob"]["TranscriptionJobStatus"] == "COMPLETED":
                transcribe_uri = response["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
                http = urllib3.PoolManager()
                transcription = http.request('GET', transcribe_uri)
                transcription_data = transcription.data.decode("utf-8")
                transcription_json = json.loads(transcription_data)
                print(transcription_json)

                text_only_transcript = ''

                for transcripts in transcription_json["results"]["transcripts"]:
                    transcript = transcripts["transcript"]
                    text_only_transcript = text_only_transcript.join(transcript)

                print(text_only_transcript)

                dataplane = DataPlane()
                s3 = boto3.client('s3')

                transcript_storage_path = dataplane.generate_media_storage_path(asset_id, workflow_id)

                key = transcript_storage_path['S3Key'] + "transcript.txt"
                bucket = transcript_storage_path['S3Bucket']

                s3.put_object(Bucket=bucket, Key=key, Body=text_only_transcript)

                transcription_json["TextTranscriptUri"] = {"S3Bucket": bucket, "S3Key": key}

                metadata_upload = dataplane.store_asset_metadata(
                    asset_id,
                    operator_object.name, workflow_id,
                    transcription_json
                )
                print('metadata_upload')
                print(metadata_upload)

                if "Status" not in metadata_upload:
                    operator_object.add_workflow_metadata(
                        TranscribeError="Unable to upload metadata for asset: {asset}".format(asset=asset_id),
                        TranscribeJobId=job_id)
                    operator_object.update_workflow_status("Error")
                    raise MasExecutionError(operator_object.return_output_object())
                else:
                    if metadata_upload['Status'] == 'Success':
                        operator_object.add_media_object('Text', metadata_upload['Bucket'], metadata_upload['Key'])
                        operator_object.add_workflow_metadata(TranscribeJobId=job_id)
                        operator_object.update_workflow_status("Complete")
                        return operator_object.return_output_object()
                    else:
                        operator_object.add_workflow_metadata(
                            TranscribeError="Unable to upload metadata for asset: {asset}".format(asset=asset_id),
                            TranscribeJobId=job_id)
                        operator_object.update_workflow_status("Error")
                        raise MasExecutionError(operator_object.return_output_object())
            else:
                operator_object.update_workflow_status("Error")
                operator_object.add_workflow_metadata(TranscribeError="Unable to determine status")
                raise MasExecutionError(operator_object.return_output_object())
