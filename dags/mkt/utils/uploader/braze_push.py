"""
Copied from mkt-crm-feed repo from locaf_feed/local_feed_push.py
Added wrapper for this to work in airflow plus removed unnecesary glue related functions
"""
import os
import json
import time
from pathlib import PosixPath
from urllib.parse import urljoin

import logging
import tempfile
import datetime

import threading
import requests

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from tenacity import retry, stop_after_attempt, wait_fixed


logger = logging.getLogger("braze_push")
BRAZE_API_ROOT = "https://rest.iad-01.braze.com/"
POOL_SIZE = 10


class BrazeChunker:
    def __init__(self, iterator, data_per_call_count, attributes_max_size):
        """Wraps profile diff iterator to chunk it into list of dicts that honor Braze limits

        :param iterator: Iterator of profile diffs expected as dicts
        :param data_per_call_count: maximum amount of object changes in single payload
        :param attributes_max_size: maximum amount of attributes in single object change
        """
        self.data_per_call_count = data_per_call_count
        self.attributes_max_size = attributes_max_size
        self.iterator = iterator
        self._lock = threading.Lock()
        self._payload_iterator = self._create_payload_iterator()

    def _create_payload_iterator(self):
        """Yields payloads to send to Braze track user endpoint.

        Data is read from iterator provided during instance init. A single payload to braze
        can have only up to `data_per_call_count` object changes in it and each object change
        can have only up to `attributes_max_size` attributes in it. This generator is taking
        both of these limits into account.
        """
        chunk = []
        attributes_list = None
        external_id = None

        # we need space for external_id and a call flag "_update_existing_only"
        attr_count = self.attributes_max_size - 2

        while True:
            # While there are still attributes to be processed from the last json, don't read
            # next json.
            # This implements splitting many attributes into multiple calls as Braze
            # API expects a maximum of 75 attributes per call at the time of this comment.
            if not attributes_list:
                try:
                    jsonl_line = next(self.iterator)
                except StopIteration:
                    # no more data to read. leftovers should be yielded and method should raise
                    # StopIteration exception afterwards
                    if chunk:
                        yield chunk
                    raise
                else:
                    # there is more data in the iterator to split
                    line_attributes = json.loads(jsonl_line)
                    external_id = line_attributes.pop("external_id")
                    attributes_list = list(line_attributes.items())

            # split off desired number of attributes
            current_data = dict(attributes_list[:attr_count])
            attributes_list = attributes_list[attr_count:]

            current_data.update(
                {"external_id": external_id, "_update_existing_only": True}
            )
            chunk.append(current_data)

            if len(chunk) == self.data_per_call_count:
                yield chunk
                chunk = []

    def pop(self):
        """Pops next payload to Braze suitable for sending. This method is thread safe.

        :return: list[dict]
        """
        with self._lock:
            return next(self._payload_iterator)


class BrazeRateLimitError(Exception):
    pass


class BrazeConnection:
    def __init__(self, app_group_token, track_user_endpoint="users/track"):
        self.app_group_token = app_group_token
        self.track_user_endpoint = urljoin(BRAZE_API_ROOT, track_user_endpoint)

    def wait_time_reset(self, time_to_reset):
        """
        API Limit is reached when # of request sent is = 0, wait until the time to reset the
        requests counter
        :param time_to_reset: r.headers['X-RateLimit-Reset'], time at which the current rate
        limit window resets in UTC epoch seconds
        """
        now = datetime.datetime.utcnow()
        epoch = datetime.datetime.utcfromtimestamp(0)
        # In seconds. We add 1 extra minute to account for clock differences
        wait_time = int(int(time_to_reset) - (now - epoch).total_seconds()) + 61

        logger.info("Waiting {} seconds for new credits...".format(wait_time))
        time.sleep(wait_time)
        logger.info("Waiting period ended. Continuing upload.")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def track_user_request(self, data_content, data_type, req):
        """
        Send the updated data to Appboy
        :param data_content: List(Dict) -- a list of the user data to upload. Each user should be
            a single entry in the list and the entry is a dictionary of key-value pairs
        :param data_type: String -- either 'attributes' or 'events'; the type of data to be
            uploaded
        :param req: Session request
        :return: Int -- the number of users uploaded successfully
        """
        data = {"app_group_id": self.app_group_token, data_type: data_content}

        logger.debug("Uploading %s data rows", len(data_content))

        num_users_uploaded = 0
        try:
            r = req.post(self.track_user_endpoint, json=data)
        except Exception as e:
            logger.info("Upload failed because of the following error: {}".format(e))
            raise

        logger.debug(
            "Received this answer from Braze (status code = {}):\n{}".format(
                r.status_code, r.text
            )
        )
        r_json = r.json()

        # If the response returns X-RateLimit-Remaining = 0, we must stop until the period is
        # reset, and then try again
        if (
            "X-RateLimit-Remaining" in r.headers
            and r.headers["X-RateLimit-Remaining"] == "0"
        ):
            msg = "Braze API limit reached ({} calls per hour)".format(
                r.headers["X-RateLimit-Limit"]
            )
            logger.info(msg)
            self.wait_time_reset(r.headers["X-RateLimit-Reset"])
            raise BrazeRateLimitError(msg)

        if "errors" in r_json and len(r_json["errors"]) > 0:
            logger.info(
                "There were errors in this batch. HTTP status = {}. Message = {}".format(
                    r.status_code, r_json.get("message", "")
                )
            )

            error_msgs = r_json["errors"]

            is_full_batch_error = False

            for msg in error_msgs:
                if "index" in msg:  # The error is for a particular row of the data
                    logger.info("Upload error: {}".format(msg))
                    logger.info(
                        "Caused by the following row: {}".format(
                            data_content[msg["index"]]
                        )
                    )
                else:  # The error is for the whole upload
                    logger.warning("Error affecting the whole batch: {}".format(msg))
                    is_full_batch_error = True
                    if (
                        "type" in msg
                        and msg["type"] == u"Invalid data point credit key"
                    ):
                        logger.error(
                            "'Invalid data point credit key' error. Maybe you don't "
                            "need to specify a credit key; try setting it to None"
                        )

            if is_full_batch_error:
                error_num = len(data_content)
            else:
                error_num = len(error_msgs)

            num_users_uploaded = len(data_content) - error_num

            msg = "{} rows failed upload ({} where uploaded successfully)".format(
                error_num, num_users_uploaded
            )
            logger.info(msg)

            if is_full_batch_error:
                raise Exception(msg)

        if str(r.status_code)[0] != "2":
            msg = "Upload failed: Status code={}. Response={}".format(
                r.status_code, r.text
            )
            logger.warning(msg)
            raise Exception(msg)

        if "message" not in r_json:
            msg = "The response from Appboy does not contain a 'message' key"
            logger.warning(msg)
            raise Exception(msg)

        if r_json["message"] == "queued":
            # Request queued for maintenance. Server returns 202 response. We assume it will be
            # successful, but log this anyway
            logger.warning("Upload queued by Appboy. Assuming success and continuing")
            num_users_uploaded = len(data_content)

        # Note that the API response can be "success" while there are errors
        if r_json["message"] == "success":
            num_users_uploaded = len(data_content)

        return num_users_uploaded

    def track_user(self, data_content, data_type, req):
        return self.track_user_request(data_content, data_type, req)

    def upload_user_data(self, data_queue, data_type):
        """
        Create threads to send requests in parallel.
        :param data_calls: List((String, List(Dict))) -- List of blocks of data: String is the
        app_group_id, and List contains dictionaries of users
        :param data_type: String -- either 'attributes' or 'events'; type of data to be uploaded
        :return: Int -- the number of users successfully uploaded
        """
        start = datetime.datetime.now()
        update_lock = threading.Lock()

        element_count = 0

        def work():
            current_thread = threading.current_thread()
            thread_start = datetime.datetime.utcnow()
            logger.debug("Thread {} started".format(current_thread.name))

            req = requests.Session()

            while True:
                # timeout to kill the thread when it takes more then 4 hours to finish
                time_diff = datetime.datetime.utcnow() - thread_start
                timeout = 60 * 60 * 4
                if timeout < time_diff.total_seconds():
                    logger.critical(
                        "Thread %s took more than %d seconds to finish. Stopping!",
                        current_thread,
                        timeout,
                    )
                    break

                try:
                    upload_data = data_queue.pop()
                except StopIteration:
                    break

                try:
                    result = self.track_user(upload_data, data_type, req)
                except Exception:
                    logger.exception("Upload failed after 3 retries")
                    continue

                # For counting how many attributes were updated
                with update_lock:
                    nonlocal element_count
                    element_count += result

            logger.debug("Thread {} finished".format(thread.name))

        logger.info("Starting {} threads to send requests".format(POOL_SIZE))
        work_threads = []
        for i in range(POOL_SIZE):
            thread = threading.Thread(target=work)
            thread.start()
            work_threads.append(thread)

        for thread in work_threads:
            thread.join()

        logger.info("All requests in the queue have been processed")

        delta = (datetime.datetime.now() - start).total_seconds()
        logger.info("Loaded {} elements in {} seconds".format(element_count, delta))

        return element_count


def s3_file_read(s3_service, bucket_name, s3_diff_processing_path):
    # Diff files are chunked and follow this naming convention:
    # {platform}/local/{bucket}/diff/{platform}_{entity}_diff.jsonl.part001
    bucket = s3_service.Bucket(bucket_name)
    diff_files = bucket.objects.filter(Prefix=s3_diff_processing_path)
    for diff_file in diff_files:
        if diff_file.key.endswith("/"):
            continue
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = PosixPath(diff_file.key)
            download_path = os.path.join(tmp_dir, path.name)
            object_resource = s3_service.Object(bucket_name, diff_file.key)
            object_resource.download_file(download_path)

            with open(download_path) as fp:
                yield from fp


def push_the_diff(app_group_token, s3_service, bucket, s3_path):
    fp = s3_file_read(s3_service, bucket, s3_path)
    chunker = BrazeChunker(fp, 50, 75)
    braze_uploader = BrazeConnection(app_group_token)
    braze_uploader.upload_user_data(chunker, "attributes")


def push_jsonl_to_braze(
    braze_conn_id, s3_conn_id, bucket_name, jsonl_path,
):
    braze_conn = BaseHook.get_connection(braze_conn_id)
    braze_access_token = braze_conn.password
    s3 = AwsHook(aws_conn_id=s3_conn_id).get_resource_type("s3")

    push_the_diff(braze_access_token, s3, bucket_name, jsonl_path)
