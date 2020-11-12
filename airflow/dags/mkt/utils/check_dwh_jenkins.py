import pendulum


def check_dwh_jenkins_success(response, allow_unstable=False, offset=0, **airflow_context):
    """
    Check that a given DWH Jenkins jobs has finished successfully.

    By default it compares {{ tomorrow_ds }} to Jenkins timestamp, as this corresponds to jobs that
    have run on the same day.

    The indented use case is
        - Daily scheduled Airflow job for example ds = 2020-03-01
        - By Airflow's design this job will run sometime on 2020-03-02
        - A corresponding DWH job will also run sometime on 2020-03-02
        - Using `check_dwh_jenkins` in `CustomHttpSensor` will make sure the Airflow job will halt
          until DWH job has finished.

    Specify `offset` to allow a grace period (in days), e.g
    - offset=0 => Check for Jenkins job on the same day as Airflow job get executed
    - offset=1 => Check for yesterday

    Specify `allow_unstable` to consider Jenkins in UNSTABLE state as successful.

    When passing non-default parameters to CustomHttpSensor use a lambda like so
    ```python
    CustomHttpSensor(
        response_check=lambda r, **ctx: check_dwh_jenkins_success(
            r, allow_unstable=True, offset=1, **ctx
        )
    )
    ```
    """
    tomorrow_ds = airflow_context["tomorrow_ds"]
    response_parsed = response.json()

    return (
        response_parsed["result"] == "SUCCESS"
        or (response_parsed["result"] == "UNSTABLE" and allow_unstable)
    ) and (
        pendulum.parse(tomorrow_ds).subtract(days=offset)
        < pendulum.from_timestamp(response_parsed["timestamp"] / 1000)
    )
