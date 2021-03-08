#!/usr/bin/env python3

from aws_cdk import core

from databand_pipeline.databand_pipeline_stack import DatabandPipelineStack

env = {'region': 'us-east-1'}

app = core.App()
DatabandPipelineStack(app, "databand-pipeline", env=env)

app.synth()
