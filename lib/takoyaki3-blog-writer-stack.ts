import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as location from 'aws-cdk-lib/aws-location';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sqs from 'aws-cdk-lib/aws-sqs';

export class Takoyaki3BlogWriterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const serviceName = 'takoyaki3-blog-writer';
    const uploadPrefix = 'uploads';

    const uploadsBucket = new s3.Bucket(this, 'UploadsBucket', {
      enforceSSL: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      cors: [
        {
          allowedMethods: [s3.HttpMethods.PUT, s3.HttpMethods.POST, s3.HttpMethods.HEAD],
          allowedOrigins: ['*'],
          allowedHeaders: ['*'],
          maxAge: 3600,
        },
      ],
    });

    const contentBucket = new s3.Bucket(this, 'ContentBucket', {
      enforceSSL: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    const siteBucket = new s3.Bucket(this, 'SiteBucket', {
      enforceSSL: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    const siteDistribution = new cloudfront.Distribution(this, 'SiteDistribution', {
      defaultRootObject: 'index.html',
      defaultBehavior: {
        origin: new origins.S3Origin(siteBucket),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
      },
    });

    const uploadsTable = new dynamodb.Table(this, 'UploadsTable', {
      partitionKey: { name: 'upload_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
    });
    uploadsTable.addGlobalSecondaryIndex({
      indexName: 'byUser',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'created_at', type: dynamodb.AttributeType.STRING },
    });

    const photoMetadataTable = new dynamodb.Table(this, 'PhotoMetadataTable', {
      partitionKey: { name: 'upload_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
    });

    const articlesTable = new dynamodb.Table(this, 'ArticlesTable', {
      partitionKey: { name: 'article_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
    });
    articlesTable.addGlobalSecondaryIndex({
      indexName: 'byUser',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'created_at', type: dynamodb.AttributeType.STRING },
    });

    const generationRunsTable = new dynamodb.Table(this, 'GenerationRunsTable', {
      partitionKey: { name: 'run_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
    });
    generationRunsTable.addGlobalSecondaryIndex({
      indexName: 'byArticle',
      partitionKey: { name: 'article_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'created_at', type: dynamodb.AttributeType.STRING },
    });

    const exifDlq = new sqs.Queue(this, 'ExifDlq', {
      retentionPeriod: cdk.Duration.days(14),
    });
    const exifQueue = new sqs.Queue(this, 'ExifQueue', {
      visibilityTimeout: cdk.Duration.minutes(2),
      deadLetterQueue: { queue: exifDlq, maxReceiveCount: 3 },
    });

    const generationDlq = new sqs.Queue(this, 'GenerationDlq', {
      retentionPeriod: cdk.Duration.days(14),
    });
    const generationQueue = new sqs.Queue(this, 'GenerationQueue', {
      visibilityTimeout: cdk.Duration.minutes(4),
      deadLetterQueue: { queue: generationDlq, maxReceiveCount: 3 },
    });

    const placeIndexName = `${serviceName}-place-index`;
    const placeIndex = new location.CfnPlaceIndex(this, 'PlaceIndex', {
      indexName: placeIndexName,
      dataSource: 'Here',
      description: 'Reverse geocoding for photo uploads',
    });

    const geminiApiKey = new secretsmanager.Secret(this, 'GeminiApiKey', {
      secretName: `${serviceName}/gemini-api-key`,
      description: 'Gemini API key for content generation',
    });

    const lambdaEnv = {
      UPLOADS_BUCKET: uploadsBucket.bucketName,
      CONTENT_BUCKET: contentBucket.bucketName,
      UPLOADS_TABLE: uploadsTable.tableName,
      METADATA_TABLE: photoMetadataTable.tableName,
      ARTICLES_TABLE: articlesTable.tableName,
      GENERATION_RUNS_TABLE: generationRunsTable.tableName,
      EXIF_QUEUE_URL: exifQueue.queueUrl,
      GENERATION_QUEUE_URL: generationQueue.queueUrl,
      PLACE_INDEX_NAME: placeIndexName,
      UPLOAD_PREFIX: uploadPrefix,
      GEMINI_API_KEY_SECRET_ARN: geminiApiKey.secretArn,
    };

    const createUploadFn = new lambda.Function(this, 'CreateUploadFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'create_upload.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'http')),
      timeout: cdk.Duration.seconds(15),
      memorySize: 512,
      environment: lambdaEnv,
    });

    const completeUploadFn = new lambda.Function(this, 'CompleteUploadFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'complete_upload.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'http')),
      timeout: cdk.Duration.seconds(15),
      memorySize: 512,
      environment: lambdaEnv,
    });

    const generateArticleFn = new lambda.Function(this, 'GenerateArticleFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'generate_article.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'http')),
      timeout: cdk.Duration.seconds(20),
      memorySize: 512,
      environment: lambdaEnv,
    });

    const regenerateArticleFn = new lambda.Function(this, 'RegenerateArticleFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'regenerate_article.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'http')),
      timeout: cdk.Duration.seconds(20),
      memorySize: 512,
      environment: lambdaEnv,
    });

    const getArticleFn = new lambda.Function(this, 'GetArticleFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'get_article.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'http')),
      timeout: cdk.Duration.seconds(10),
      memorySize: 512,
      environment: lambdaEnv,
    });

    const exifWorkerFn = new lambda.Function(this, 'ExifWorkerFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'exif_worker.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'workers')),
      timeout: cdk.Duration.minutes(2),
      memorySize: 1024,
      environment: lambdaEnv,
    });

    const generationWorkerFn = new lambda.Function(this, 'GenerationWorkerFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'generation_worker.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'workers')),
      timeout: cdk.Duration.minutes(4),
      memorySize: 1024,
      environment: lambdaEnv,
    });

    uploadsBucket.grantPut(createUploadFn);
    uploadsTable.grantWriteData(createUploadFn);

    uploadsTable.grantWriteData(completeUploadFn);
    exifQueue.grantSendMessages(completeUploadFn);

    articlesTable.grantWriteData(generateArticleFn);
    generationRunsTable.grantWriteData(generateArticleFn);
    generationQueue.grantSendMessages(generateArticleFn);

    articlesTable.grantWriteData(regenerateArticleFn);
    generationRunsTable.grantWriteData(regenerateArticleFn);
    generationQueue.grantSendMessages(regenerateArticleFn);

    articlesTable.grantReadData(getArticleFn);

    uploadsBucket.grantRead(exifWorkerFn);
    uploadsTable.grantWriteData(exifWorkerFn);
    photoMetadataTable.grantWriteData(exifWorkerFn);
    exifQueue.grantConsumeMessages(exifWorkerFn);

    uploadsBucket.grantRead(generationWorkerFn);
    articlesTable.grantReadWriteData(generationWorkerFn);
    generationRunsTable.grantWriteData(generationWorkerFn);
    uploadsTable.grantReadData(generationWorkerFn);
    photoMetadataTable.grantReadData(generationWorkerFn);
    contentBucket.grantPut(generationWorkerFn);
    generationQueue.grantConsumeMessages(generationWorkerFn);
    geminiApiKey.grantRead(generationWorkerFn);

    const placeIndexArn = cdk.Stack.of(this).formatArn({
      service: 'geo',
      resource: 'place-index',
      resourceName: placeIndexName,
    });
    exifWorkerFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['geo:SearchPlaceIndexForPosition'],
        resources: [placeIndexArn],
      })
    );

    exifWorkerFn.addEventSource(
      new lambdaEventSources.SqsEventSource(exifQueue, {
        batchSize: 5,
      })
    );
    generationWorkerFn.addEventSource(
      new lambdaEventSources.SqsEventSource(generationQueue, {
        batchSize: 3,
      })
    );

    const api = new apigateway.RestApi(this, 'BlogWriterApi', {
      restApiName: 'Takoyaki3 Blog Writer API',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ['OPTIONS', 'POST', 'GET'],
        allowHeaders: ['Content-Type', 'Authorization'],
      },
    });

    const v1 = api.root.addResource('v1');
    const uploads = v1.addResource('uploads');
    uploads.addMethod('POST', new apigateway.LambdaIntegration(createUploadFn));
    const uploadById = uploads.addResource('{uploadId}');
    uploadById
      .addResource('complete')
      .addMethod('POST', new apigateway.LambdaIntegration(completeUploadFn));

    const articles = v1.addResource('articles');
    articles.addResource('generate').addMethod('POST', new apigateway.LambdaIntegration(generateArticleFn));
    const articleById = articles.addResource('{articleId}');
    articleById.addMethod('GET', new apigateway.LambdaIntegration(getArticleFn));
    articleById.addResource('regenerate').addMethod('POST', new apigateway.LambdaIntegration(regenerateArticleFn));

    new s3deploy.BucketDeployment(this, 'DeploySite', {
      sources: [
        s3deploy.Source.asset(path.join(__dirname, '..', 'web')),
        s3deploy.Source.data('config.json', JSON.stringify({ apiBaseUrl: api.url })),
      ],
      destinationBucket: siteBucket,
      distribution: siteDistribution,
      distributionPaths: ['/*'],
    });

    new cdk.CfnOutput(this, 'ApiEndpoint', {
      value: api.url,
    });
    new cdk.CfnOutput(this, 'WebsiteUrl', {
      value: `https://${siteDistribution.distributionDomainName}`,
    });
    new cdk.CfnOutput(this, 'UploadsBucketName', {
      value: uploadsBucket.bucketName,
    });
    new cdk.CfnOutput(this, 'ContentBucketName', {
      value: contentBucket.bucketName,
    });
    new cdk.CfnOutput(this, 'ExifQueueUrl', {
      value: exifQueue.queueUrl,
    });
    new cdk.CfnOutput(this, 'GenerationQueueUrl', {
      value: generationQueue.queueUrl,
    });
    new cdk.CfnOutput(this, 'PlaceIndexName', {
      value: placeIndexName,
    });
  }
}
