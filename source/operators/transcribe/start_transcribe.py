# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import boto3
import json
from botocore import config
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from MediaInsightsEngineLambdaHelper import MediaInsightsOperationHelper
from MediaInsightsEngineLambdaHelper import MasExecutionError
from MediaInsightsEngineLambdaHelper import OutputHelper
from botocore.exceptions import BotoCoreError

patch_all()

region = os.environ['AWS_REGION']

mie_config = json.loads(os.environ['botoConfig'])
config = config.Config(**mie_config)
transcribe = boto3.client("transcribe", config=config)

# TODO: More advanced exception handling, e.g. using boto clienterrors and narrowing exception scopes


def create_payload(s3_bucket, s3_key, job_id, language, ddbtable):
    payload = {
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "job_id": job_id,
        "language": language,
        "ddbtable": ddbtable
    }
    return payload


def get_asynch_input_key(s3_key):
    key = s3_key.rsplit('/', 1)[0] + '/asynch_input_file.json'
    return key


def save_to_s3(s3_client, payload, s3_bucket, asynch_input_key):
    try:
        s3_client.put_object(
            Body=json.dumps(payload),
            Bucket=s3_bucket,
            Key=asynch_input_key
        )
        print("File Saved.")
        return 200
    except BotoCoreError as e:
        print(f"Error saving JSON to S3: {e}")
        return e

def invoke_endpoint(endpoint_name, s3_bucket, asynch_input_key, job_id):
    try:
        sagemaker_runtime = boto3.client('sagemaker-runtime')
        response = sagemaker_runtime.invoke_endpoint_async(
            EndpointName=endpoint_name,
            InputLocation=f's3://{s3_bucket}/{asynch_input_key}',
            ContentType='application/json',
            InferenceId=job_id,
        )
        print(f"response: {response}")

        # Create the output_response variable
        output_response = None

        if response["ResponseMetadata"]["HTTPStatusCode"] == 202:
            output_response = {
                "TranscriptionJob": {
                    "TranscriptionJobStatus": "IN_PROGRESS",
                    "JobId": job_id,
                    "OutputLocation": response["OutputLocation"]
                }
            }
        else:
            raise Exception("Unhandled response status code")

        return output_response

    except BotoCoreError as e:
        print(f"Error invoking endpoint: {e}")
        raise Exception("Error invoking endpoint")


def lambda_handler(event, context):
    print("We got this event:\n", event)
    valid_types = ["mp3", "mp4", "wav", "flac"]
    optional_settings = {}
    operator_object = MediaInsightsOperationHelper(event)
    print("operator_object.return_output_object()")
    print(operator_object.return_output_object())
    workflow_id = str(event["WorkflowExecutionId"])
    asset_id = event['AssetId']

    try:
        if "ProxyEncode" in event["Input"]["Media"]:
            bucket = event["Input"]["Media"]["ProxyEncode"]["S3Bucket"]
            key = event["Input"]["Media"]["ProxyEncode"]["S3Key"]
        elif "Video" in event["Input"]["Media"]:
            bucket = event["Input"]["Media"]["Video"]["S3Bucket"]
            key = event["Input"]["Media"]["Video"]["S3Key"]
        elif "Audio" in event["Input"]["Media"]:
            bucket = event["Input"]["Media"]["Audio"]["S3Bucket"]
            key = event["Input"]["Media"]["Audio"]["S3Key"]
        file_type = key.split('.')[-1]
    except Exception:
        operator_object.update_workflow_status("Error")
        operator_object.add_workflow_metadata(TranscribeError="No valid inputs")
        raise MasExecutionError(operator_object.return_output_object())

    if file_type not in valid_types:
        operator_object.update_workflow_status("Error")
        operator_object.add_workflow_metadata(TranscribeError="Not a valid file type")
        raise MasExecutionError(operator_object.return_output_object())
    try:
        custom_vocab = operator_object.configuration["VocabularyName"]
        optional_settings["VocabularyName"] = custom_vocab
    except KeyError:
        # No custom vocab
        pass
    try:
        language_code = operator_object.configuration["TranscribeLanguage"]
    except KeyError:
        operator_object.update_workflow_status("Error")
        operator_object.add_workflow_metadata(TranscribeError="No language code defined")
        raise MasExecutionError(operator_object.return_output_object())

    media_file = 'https://s3.' + region + '.amazonaws.com/' + bucket + '/' + key

    # If mediainfo data is available then use it to avoid transcribing silent videos.
    if "Mediainfo_num_audio_tracks" in event["Input"]["MetaData"]:
        num_audio_tracks = event["Input"]["MetaData"]["Mediainfo_num_audio_tracks"]
        # Check to see if audio tracks were detected by mediainfo
        if num_audio_tracks == "0":
            # If there is no input audio then we're done.
            operator_object.update_workflow_status("Complete")
            return operator_object.return_output_object()
    try:
        if (language_code == 'ar-AE') or (language_code == 'ar-SA') or (language_code == 'tr-TR'):
        # if (language_code == 'NO ar-AE') or (language_code == 'NO ar-SA') or (language_code == 'NO tr-TR'):
            job_id = "whisper" + "-" + workflow_id
            whisper_sg_endpt = os.environ['WhisperSageMakerEndpoint']
            ddbtable = os.environ['DynamodbTableName']
            if "Audio" in event["Input"]["Media"]:
                wbucket = event["Input"]["Media"]["Audio"]["S3Bucket"]
                wkey = event["Input"]["Media"]["Audio"]["S3Key"]
            elif "Video" in event["Input"]["Media"]:
                wbucket = event["Input"]["Media"]["Video"]["S3Bucket"]
                wkey = event["Input"]["Media"]["Video"]["S3Key"]

            print("endpoint name: " + str(whisper_sg_endpt))
            print("bucket: " + wbucket)
            print("key: " + wkey)
            Key_path, _ = os.path.split(wkey)
            # Append the new filename to the path
            output_file_name = os.path.join(Key_path, 'TranscribeVideo.json')

            if (language_code == 'ar-AE') or (language_code == 'ar-SA'):
                language='ar'
            elif (language_code == 'tr-TR'):
                language='tr'
            else:
                language='auto'

            print("Started Whisper Invoke")
            payload = create_payload(wbucket, wkey, job_id, language, ddbtable)
            print("Payload Created! " + str(payload))
            asynch_input_key = get_asynch_input_key(wkey)
            print("Payload asynch_input_key: " + str(asynch_input_key))
            
            s3_boto3Client = boto3.client('s3')
            
            if save_to_s3(s3_boto3Client, payload, wbucket, asynch_input_key) == 200:
                try:
                    response = invoke_endpoint(
                        whisper_sg_endpt,
                        wbucket,
                        asynch_input_key,
                        job_id
                    )
                    operator_object.update_workflow_status("Executing")
                    operator_object.add_workflow_metadata(
                        TranscribeJobId=job_id,
                        AssetId=asset_id,
                        WorkflowExecutionId=workflow_id
                    )
                    output_response = operator_object.return_output_object()

                except Exception as e:
                    operator_object.update_workflow_status("Error")
                    operator_object.add_workflow_metadata(transcribe_error=str(e))
                    raise MasExecutionError(operator_object.return_output_object())

            else:
                operator_object.update_workflow_status("Error")
                operator_object.add_workflow_metadata(transcribe_error=str(e))
                raise MasExecutionError(operator_object.return_output_object())

        else:
            job_id = "transcribe" + "-" + workflow_id
            response = transcribe.start_transcription_job(
                TranscriptionJobName=job_id,
                LanguageCode=language_code,
                Media={
                    "MediaFileUri": media_file
                },
                MediaFormat=file_type,
                Settings=optional_settings
            )

            print('response')
            print(response)

    except Exception as e:
        operator_object.update_workflow_status("Error")
        operator_object.add_workflow_metadata(transcribe_error=str(e))
        raise MasExecutionError(operator_object.return_output_object())

    else:
        # create the output_response variable
        output_response = None

        if response["TranscriptionJob"]["TranscriptionJobStatus"] == "IN_PROGRESS":
            operator_object.update_workflow_status("Executing")
            operator_object.add_workflow_metadata(TranscribeJobId=job_id, AssetId=asset_id,
                                                  WorkflowExecutionId=workflow_id)
            output_response = operator_object.return_output_object()
        elif response["TranscriptionJob"]["TranscriptionJobStatus"] == "FAILED":
            operator_object.update_workflow_status("Error")
            operator_object.add_workflow_metadata(TranscribeJobId=job_id,
                                                  TranscribeError=str(response["TranscriptionJob"]["FailureReason"]))
            output_response = operator_object.return_output_object()
            raise MasExecutionError(output_response)
        elif response["TranscriptionJob"]["TranscriptionJobStatus"] == "COMPLETE":
            operator_object.update_workflow_status("Executing")
            operator_object.add_workflow_metadata(TranscribeJobId=job_id, AssetId=asset_id,
                                                  WorkflowExecutionId=workflow_id)
            output_response = operator_object.return_output_object()
        else:
            operator_object.update_workflow_status("Error")
            operator_object.add_workflow_metadata(TranscribeJobId=job_id,
                                                  TranscribeError="Unhandled error for this job: {job_id}".format(
                                                      job_id=job_id))
            output_response = operator_object.return_output_object()
            raise MasExecutionError(output_response)

        # return output_response at the end
        print("output_response")
        print(output_response)
        return output_response
