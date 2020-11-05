import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from airflow.models import BaseOperator

from datahub.hooks.bigquery_hook import BigQueryHook


class VariableImportancesStaffingOperator(BaseOperator):
    template_ext = ['.sql']

    def __init__(self, bigquery_conn_id='bigquery_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hook = None
        self.bigquery_conn_id = bigquery_conn_id
        self.target_table = 'rl.variable_importances_staffing'

    @property
    def hook(self):
        if self._hook is None:
            self._hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                use_legacy_sql=False,
            )
        return self._hook

    def execute(self, context):
        sql = self.render_template('side_tables/side_staffing_weights.sql', context)
        df = self.hook.get_pandas_df(sql, dialect='standard')

        df = self.remove_margins(df)

        # Remove NANs
        df.dropna(inplace=True)

        features, labels = self.define_labels_and_features(df)

        feature_list, features = self.split_names_and_values(df, features)

        # Training and Testing Sets
        # Split the data into training and testing sets
        train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size=0.25,
                                                                                    random_state=42)

        # Train Model
        # Instantiate (configure) model with 1000 decision trees
        rf = RandomForestRegressor(n_estimators=1000, random_state=42)
        # Train the model on training data
        rf.fit(train_features, train_labels)

        # Variable Importances
        var_imp = self.extract_and_convert_feature_importances(feature_list, rf)
        # Save importances as bigquery table
        var_imp.to_gbq(self.target_table, self.hook.project_id, if_exists='replace')

    def extract_and_convert_feature_importances(self, feature_list, rf):
        # Get numerical feature importances
        importances = list(rf.feature_importances_)
        # List of tuples with variable and importance
        feature_importances = [(feature, round(importance, 2)) for feature, importance in
                               zip(feature_list, importances)]
        # Convert feature_importances to dataframe
        var_imp = pd.DataFrame(feature_importances, columns=['feature', 'weight'])
        return var_imp

    def split_names_and_values(self, df, features):
        # Saving feature names for later use
        feature_list = list(features.columns)
        # Convert to numpy array
        features = np.array(features)
        return feature_list, features

    def define_labels_and_features(self, df):
        # Features and Targets and Convert Data to Arrays
        # Labels are the values we want to predict
        labels = np.array(df['utr'])
        # Remove the labels from the features
        features = df[[str(x) for x in df.columns if x not in ('country_code', 'city_id', 'report_date_local', 'utr')]]
        return features, labels

    def remove_margins(self, df):
        # Remove marginal 1% or 2,5% of series, depending on variable distribution
        df = df[df.perc_rider_demand_fulfilled < df.perc_rider_demand_fulfilled.quantile(0.99)]
        df = df[df.perc_manual_staffing < df.perc_manual_staffing.quantile(0.99)]
        df = df[df.utr <= 5]
        return df
