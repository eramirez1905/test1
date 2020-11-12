import json
import os
import unittest

from freezegun import freeze_time
from jira import Issue

from atlassian.jira_json_cleansing import JiraJsonCleansing


class JiraJsonCleansingTest(unittest.TestCase):
    maxDiff = None

    def test_prepare(self):
        fixture_path = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures'
        freezer = freeze_time("2019-09-09 05:05:05.000000")
        freezer.start()

        with open(f'{fixture_path}/test_issue.json', 'r') as file:
            input_raw = json.loads(file.read())

        test_issue = Issue(None, None, raw=input_raw)
        test_issue._options = {'server': 'jira_server'}

        jira_json_cleanser = JiraJsonCleansing('2020-03-31 05:00:00')

        clean_output = jira_json_cleanser.prepare(test_issue)

        with open(f'{fixture_path}/test_result.json', 'r') as file:
            expected_result = file.read()

        self.assertDictEqual(json.loads(expected_result), json.loads(clean_output))
        freezer.stop()
