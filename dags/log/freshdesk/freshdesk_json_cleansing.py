import csv
import json
import re
import tempfile
from datetime import datetime


class FreshDeskJsonCleansing:

    def __init__(self,
                 region,
                 ingested_at):
        self.region = region
        self.ingested_at = ingested_at
        self.default_rows = {
            'instance': self.region,
            '_ingested_at': self.ingested_at
        }
        self.optional_rows = {
            'asia': {
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Rider_ID': None,
                'Freshchat_Contact_Reason': None,
                'Facebook_Profile_ID': None,
                'Ciudad_de_Repartos': None,
                'Work_phone': None,
                'Mobile_phone': None
            },
            'at': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None,
                'Work_phone': None,
                'Mobile_phone': None
            },
            'canada': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None,
                'Work_phone': None,
                'Mobile_phone': None
            },
            'cee': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None,
                'Work_phone': None,
                'Mobile_phone': None
            },
            'eu': {
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None,
                'Work_phone': None,
                'Mobile_phone': None
            },
            'sa': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Country': None,
                'City_': None,
                'Type_27': None,
                'Work_phone': None,
                'Mobile_phone': None
            },
            'talabat': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None
            },
            'cyprus': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None
            },
            'ksa': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None
            },
            'balkan': {
                'Association_Type': None,
                'Internal_Agent': None,
                'Internal_Group': None,
                'Freshchat_Group': None,
                'Freshchat_Agent': None,
                'Freshchat_Contact_Reason': None,
                'Freshchat_Conversation_ID': None,
                'Freshchat_Rider_ID': None,
                'Ciudad_de_Repartos': None,
                'Country': None,
                'City_': None,
                'Type_27': None
            }
        }
        self.standardised_rows = {**self.optional_rows[self.region], **self.default_rows}

    def csv_to_json(self, source):
        default_rows = self.standardised_rows
        cleansed_row = {}
        json_list = []
        created_date = "created_date"
        last_updated_time = "last_updated_time"
        created_at = "created_at"
        updated_at = "updated_at"
        created_time = "created_time"
        time_fmt = "%Y-%m-%d %H:%M:%S"

        with open(source) as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                for key, value in row.items():
                    transformed_key = re.sub('[^a-zA-Z0-9\n]', '_', key)
                    cleansed_row[transformed_key] = value
                    if transformed_key.lower() == created_time:
                        cleansed_row[created_date] = datetime.strptime(value, time_fmt).strftime('%Y-%m-%d')
                        cleansed_row[created_at] = value
                    if transformed_key.lower() == last_updated_time:
                        cleansed_row[updated_at] = value
                json_list.append(
                    json.dumps(dict(cleansed_row, **default_rows))
                )

        return '\n'.join(json_list)

    def prepare_json(self, response):
        """
           Method for convert csv to json and
           cleanse the csv header from special
           characters.
        """
        with tempfile.NamedTemporaryFile(delete=True, mode='a+') as temp_csv_file:
            with open(temp_csv_file.name, 'wb') as file:
                for chunk in response:
                    file.write(chunk)

            json_string = self.csv_to_json(temp_csv_file.name)
        return json_string
