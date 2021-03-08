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
        # matplotlib_layer = _lambda.LayerVersion(self, "matplotlib", code=_lambda.AssetCode('layers/matplotlib.zip'))
        pandas_layer = _lambda.LayerVersion(self, "matplotlib", code=_lambda.AssetCode('layers/pandas.zip'))

        # Lambdas
        fetch_function = _lambda.Function(
            self, "fetch_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_9,
            environment = {
                "URL" : "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
                "DATA_BUCKET" : data_bucket.bucket_name()
            },
            layers=[requests_layer],
            memory_size = 512,
            timeout=Duration.minutes(3),
            handler="fetch-lambda.handler",
            code=_lambda.Code.asset("./src")
        )

        enrich_function = _lambda.Function(
            self, "enrich_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="enrich-lambda.handler",
            environment = {
                "DATA_BUCKET" : data_bucket.bucket_name()
            },
            memory_size = 512,
            timeout=Duration.minutes(3),
            code=_lambda.Code.asset("./src")
        )

        saver_function = _lambda.Function(
            self, "saver_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="saver-lambda.handler",
            environment = {
                "DATA_BUCKET" : data_bucket.bucket_name()
            },
            memory_size = 512,
            timeout=Duration.minutes(3),
            code=_lambda.Code.asset("./src")
        )

        generate_monthly_dashboard_function = _lambda.Function(
            self, "generate_monthly_dashboard_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="generate_monthly_dashboard-lambda.handler",
            environment = {
                "DATA_BUCKET" : data_bucket.bucket_name()
            },
            layers=[pandas_layer],
            memory_size = 512,
            timeout=Duration.minutes(3),
            code=_lambda.Code.asset("./src")
        )

        # S3 Permissions
        data_bucket.grant_write(saver_function)
        data_bucket.grant_write(generate_monthly_dashboard_function)
        
        # Step Function

        # Taskes
        fetch_task = sfn.Task(self, "Fetch", task=sfn_tasks.LambdaInvoke(self, 'fetch', lambda_function=fetch_function))
        enrich_task = sfn.Task(self, "Enrich", task=sfn_tasks.LambdaInvoke(self, 'enrich', lambda_function=enrich_function))
        save_data_task = sfn.Task(self, "Save Data", task=sfn_tasks.LambdaInvoke(self, 'save_data', lambda_function=saver_function))
        generate_monthly_dashboard_task = sfn.Task(self, "Generate Monthly Dashboard", task=sfn_tasks.LambdaInvoke(self, 'generate_monthly_dashboard', lambda_function=generate_monthly_dashboard_function))

        # Defenition
        definition = fetch_task\
            .next(enrich_task)\
            .next(db_saver_task)\
            .next(generate_monthly_dashboard_task)

        sfn.StateMachine(
            self,
            "Databand-Pipeline-StateMachine",
            definition=definition,
            timeout=Duration.minutes(5)
        )

    # TODO:
    # 1) test this - E2E.



