from aws_cdk import core
from aws_cdk.core import Duration
from aws_cdk import(
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks
    )

class DatabandPipelineStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # S3 Bucket
        data_bucket = s3.Bucket(self, "databand-data-bucket")
        
        # Lambda Layers
        requests_layer = _lambda.LayerVersion(self, "requests", code=_lambda.AssetCode('layers/requests.zip'))
        matplotlib_layer = _lambda.LayerVersion(self, "matplotlib", code=_lambda.AssetCode('layers/matplotlib.zip'))
        pandas_layer = _lambda.LayerVersion(self, "pandas", code=_lambda.AssetCode('layers/pandas.zip'))

        # Lambdas
        fetch_function = _lambda.Function(
            self, "fetch_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_7,
            environment = {
                "URL" : "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
                "DATA_BUCKET" : data_bucket.bucket_name
            },
            layers=[requests_layer],
            memory_size = 512,
            timeout=Duration.minutes(3),
            handler="fetch-lambda.handler",
            code=_lambda.Code.asset("./src")
        )

        enrich_function = _lambda.Function(
            self, "enrich_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_7,
            handler="enrich-lambda.handler",
            environment = {
                "DATA_BUCKET" : data_bucket.bucket_name
            },
            memory_size = 512,
            timeout=Duration.minutes(3),
            code=_lambda.Code.asset("./src")
        )

        generate_monthly_dashboard_function = _lambda.Function(
            self, "generate_monthly_dashboard_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_7,
            handler="generate_monthly_dashboard-lambda.handler",
            environment = {
                "DATA_BUCKET" : data_bucket.bucket_name
            },
            layers=[matplotlib_layer, pandas_layer],
            memory_size = 512,
            timeout=Duration.minutes(3),
            code=_lambda.Code.asset("./src")
        )

        # S3 Permissions
        data_bucket.grant_write(fetch_function)
        data_bucket.grant_read_write(enrich_function)
        data_bucket.grant_read_write(generate_monthly_dashboard_function)
        
        # Step Function

        # Taskes
        fetch_task = sfn_tasks.LambdaInvoke(self, 'fetch', lambda_function=fetch_function, payload_response_only=True)
        enrich_task = sfn_tasks.LambdaInvoke(self, 'enrich', lambda_function=enrich_function, payload_response_only=True)
        generate_monthly_dashboard_task = sfn_tasks.LambdaInvoke(self, 'generate_monthly_dashboard', lambda_function=generate_monthly_dashboard_function, payload_response_only=True)

        # Defenition
        definition = fetch_task\
            .next(enrich_task)\
            .next(generate_monthly_dashboard_task)

        sfn.StateMachine(
            self,
            "Databand-Pipeline-StateMachine",
            definition=definition,
            timeout=Duration.minutes(5)
        )

    # TODO:
    # 1) test this - E2E.



