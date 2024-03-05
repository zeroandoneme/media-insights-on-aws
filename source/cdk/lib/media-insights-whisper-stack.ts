import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam'
import * as sagemaker from 'aws-cdk-lib/aws-sagemaker'
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as kms from 'aws-cdk-lib/aws-kms';

export interface OpenaiWhisperDeploymentStackProps extends cdk.StackProps {
  imageUri: string;
  instanceType: string;
  initialInstanceCount: number;
  DataplaneBucketName: string;
}

export class OpenaiWhisperDeploymentStack extends cdk.NestedStack {
  readonly modelBucket: s3.Bucket;
  readonly cfnEndpoint: sagemaker.CfnEndpoint;
  readonly dynamoTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: OpenaiWhisperDeploymentStackProps) {
    super(scope, id, props);

    const { imageUri, instanceType, initialInstanceCount } = props;
    
    //Creating IAM role first as its required in many constructs.
    const sgRole = new iam.Role(this, 'sgRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
    });
    sgRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sagemaker:*'],
      resources: ['*'],
    }));
    sgRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetAuthorizationToken",
      ],
      resources: ['*'],
    }));
    sgRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "kms:GenerateDataKey",
        "kms:Decrypt"
      ],
      resources: ['*'],
    }));
    sgRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ],
      resources: ['*'],
    }));

    const dataplaneBucket = s3.Bucket.fromBucketName(this, 'MyImportedBucket', props.DataplaneBucketName);
    dataplaneBucket.grantReadWrite(sgRole)
      
    const whisperLogBucket = new s3.Bucket(this, 'WhisperLogBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      enforceSSL: true
    });

    this.modelBucket = new s3.Bucket(this, 'WhisperASRModelBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      serverAccessLogsBucket: whisperLogBucket,
      enforceSSL: true
    });

    this.modelBucket.grantReadWrite(sgRole);

    this.dynamoTable = new dynamodb.Table(this, 'JobResultsTable', {
      partitionKey: { name: 'job_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
      tableName: 'OpenAIWhisperLogsDynamodbTable'
    });

    const jobResultsLambdaRole = new iam.Role(this, 'JobResultsLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const jobResultsLambdaCode = `import os
import boto3
import json
from botocore.exceptions import ClientError


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.getenv('TABLE_NAME'))

def add_or_update_item(jobId, result, eventTime):
    try:
        # Try to get the item from the DynamoDB table
        response = table.get_item(
            Key={
                'job_id': jobId
            }
        )

        # Check if item exists
        if 'Item' in response:
            # If item exists, update it
            table.update_item(
                Key={
                    'job_id': jobId
                },
                UpdateExpression="SET snsresult = :r, eventTime = :e",
                ExpressionAttributeValues={
                    ':r': result,
                    ':e': eventTime
                },
                ReturnValues="UPDATED_NEW"
            )
        else:
            # If item doesn't exist, put a new item
            table.put_item(
                Item={
                    'job_id': jobId,
                    'snsresult': result,
                    'eventTime': eventTime
                }
            )
    except ClientError as e:
        print(e.response['Error']['Message'])

def main(event, context):
    print(f"event {event}")
    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        print(f"message {message}")
        
        jobId = message['inferenceId']
        result = message['invocationStatus']
        eventTime = message['eventTime']
        
    add_or_update_item(jobId, result, eventTime)

    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }`;

    const jobResultsLambda = new lambda.Function(this, 'JobResultsFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      code: lambda.Code.fromInline(jobResultsLambdaCode),
      handler: 'index.main',
      environment: {
        TABLE_NAME: this.dynamoTable.tableName,
      },
      role: jobResultsLambdaRole
    });

    this.dynamoTable.grantReadWriteData(sgRole)
    this.dynamoTable.grantReadWriteData(jobResultsLambda);

    const sagemakerModel = new sagemaker.CfnModel(this, 'MyCfnModel', {
      executionRoleArn: sgRole.roleArn,
      primaryContainer: { image: imageUri }
    });
    sagemakerModel.node.addDependency(sgRole)

    const kmsKey = new kms.Key(this, 'topicKMSKey');

    const successTopic = new sns.Topic(this, 'SuccessTopic', {
      displayName: 'Success Topic',
      masterKey: kmsKey,
    })
    successTopic.grantPublish(sgRole)

    const errorTopic = new sns.Topic(this, 'ErrorTopic', {
      displayName: 'Error Topic',
      masterKey: kmsKey,
    });
    errorTopic.grantPublish(sgRole)

    const cfnEndpointConfig = new sagemaker.CfnEndpointConfig(this, 'MyCfnEndpointConfig', {
      productionVariants: [{
        initialVariantWeight: 1.0,
        modelName: sagemakerModel.attrModelName,
        variantName: 'default',
        initialInstanceCount: initialInstanceCount,
        instanceType: instanceType,
      }],
      asyncInferenceConfig: {
        outputConfig: {
          notificationConfig: {
            errorTopic: errorTopic.topicArn,
            successTopic: successTopic.topicArn,
          },
          s3OutputPath: `s3://${this.modelBucket.bucketName}/output`,
        },
        clientConfig: {
          maxConcurrentInvocationsPerInstance: 5,
        },
      }
    });
    cfnEndpointConfig.addDependency(sagemakerModel)

    this.cfnEndpoint = new sagemaker.CfnEndpoint(this, 'MyCfnEndpoint', {
      endpointConfigName: cfnEndpointConfig.attrEndpointConfigName,
      endpointName: 'OpenAIWhisperEndpoint',
    });
    this.cfnEndpoint.addDependency(cfnEndpointConfig)

    // Attaching event source after defining the endpoint
    jobResultsLambda.addEventSource(new lambdaEventSources.SnsEventSource(successTopic));
    jobResultsLambda.addEventSource(new lambdaEventSources.SnsEventSource(errorTopic));

    new cdk.CfnOutput(this, 'BucketNameOutput', {
      value: this.modelBucket.bucketName,
      description: 'The name of the created S3 bucket',
    });

    new cdk.CfnOutput(this, 'WhisperEndpointNameOutput', {
      value: this.cfnEndpoint.attrEndpointName,
      description: 'The name of the deployed SageMaker endpoint',
    });

    new cdk.CfnOutput(this, 'DynamodbTableName', {
      value: this.dynamoTable.tableName,
      description: 'The name of the created Dynamodb table',
    });
  }
}