import unittest

from freshdesk.freshdesk_json_cleansing import FreshDeskJsonCleansing


class FreshDeskCsvJsonCleansingTest(unittest.TestCase):

    def test_json_standardisation(self):
        expected_result = "{'Internal_Agent': None, 'Internal_Group': None, 'Freshchat_Conversation_ID': None, " \
                          "'Freshchat_Group': None, 'Freshchat_Agent': None, 'Freshchat_Rider_ID': None, " \
                          "'Freshchat_Contact_Reason': None, 'Facebook_Profile_ID': None, 'Ciudad_de_Repartos': None, " \
                          "'Work_phone': None, 'Mobile_phone': None, 'instance': 'asia', '_ingested_at': '2020-04-28 " \
                          "13:48:38.946205'}"

        fresh_desk_json_cleansing = FreshDeskJsonCleansing("asia", "2020-04-28 13:48:38.946205")
        output = str(fresh_desk_json_cleansing.standardised_rows)
        self.assertEqual(expected_result, output)
