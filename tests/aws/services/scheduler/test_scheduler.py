import json

import pytest

from localstack.testing.aws.util import in_default_partition
from localstack.testing.pytest import markers
from localstack.utils.strings import short_uid


@pytest.mark.skipif(
    not in_default_partition(), reason="Test not applicable in non-default partitions"
)
@markers.aws.validated
def test_list_schedules(aws_client):
    # simple smoke test to assert that the provider is available, without creating any schedules
    result = aws_client.scheduler.list_schedules()
    assert isinstance(result.get("Schedules"), list)


@markers.aws.validated
def test_update_schedule(aws_client, sqs_create_queue, sqs_get_queue_arn, create_role):
    name = short_uid()

    queue_arn = sqs_get_queue_arn(sqs_create_queue())
    role_name = f"test-role-{short_uid()}"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "scheduler.amazonaws.com"},
                "Action": "sts:AssumeRole",
            },
        ],
    }

    role_arn = create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))[
        "Role"
    ]["Arn"]

    result2 = aws_client.scheduler.create_schedule(
        Name=name,
        ScheduleExpression="rate(5 minute)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": queue_arn,
            "RoleArn": role_arn,
            # "RoleArn": "arn:aws:iam::354924239642:role/localstack-test",
            "Input": "test",
        },
    )

    result2 = aws_client.scheduler.update_schedule(
        Name=name,
        ScheduleExpression="rate(10 minute)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": queue_arn,
            "RoleArn": role_arn,
            # "RoleArn": "arn:aws:iam::354924239642:role/localstack-test",
            "Input": "test",
        },
    )

    result2 = aws_client.scheduler.get_schedule(
        Name=name,
    )

    assert result2["ScheduleExpression"] == "rate(10 minute)"
