import numpy as np
import pandas as pd
from airflow.models import BaseOperator

from datahub.hooks.bigquery_hook import BigQueryHook


class ScorecardScoreAssignmentOperator(BaseOperator):
    template_ext = ['.sql']
    template_fields = ('target_table', 'sql')

    ui_color = '#ffed31'
    ui_fgcolor = '#000'

    def __init__(self, target_table, sql, bigquery_conn_id='bigquery_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hook = None
        self.bigquery_conn_id = bigquery_conn_id
        self.target_table = target_table
        self.sql = sql

    @property
    def hook(self):
        if self._hook is None:
            self._hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                use_legacy_sql=False,
            )
        return self._hook

    def execute(self, context):
        df = self.hook.get_pandas_df(self.sql, dialect='standard')

        df.dropna(inplace=True)

        bins = self.get_quantiles(df)

        bins_asc, bins_desc = self.divide_dataframe(bins)

        # Sort values to descending in bins_desc
        bins_desc.sort_values(by=bins_desc.iloc[:, 0:1].columns[0], ascending=False, inplace=True)

        # In bins_desc, replace maximum values with 500 to create bottom interval where score = 0
        bins_desc.iloc[0, :] = 500

        ascending_variables, descending_variables = self.pivot_and_rename(bins_asc, bins_desc)

        self.get_end_intervals(ascending_variables, descending_variables)

        # Replace NAN with 0 in descending_variables
        descending_variables.fillna(0, inplace=True)

        self.replace_negative_and_null_values(ascending_variables)

        # Add indicator in each dataset that values are desc or asc
        ascending_variables['indicator'] = 'asc'
        descending_variables['indicator'] = 'desc'

        # Concatenate two dataframes
        intervals = pd.concat([descending_variables, ascending_variables], axis=0, ignore_index=True)

        # Pull weights from BigQuery table
        sql = self.render_template('pandas/weights.sql', context)
        weights = self.hook.get_pandas_df(sql, dialect='standard')
        weights.reset_index(drop=True, inplace=True)

        # Add weights to intervals
        weights_to_intervals = self.merge_weights_into_intervals(intervals, weights)

        self.add_score_range_to_variable(intervals, weights_to_intervals)

        weights = pd.merge(weights_to_intervals, self.segments_data_frame, left_on=['variable'], right_on=['variable'])

        self.log.info(weights['variable'].nunique())

        weights.to_gbq(self.target_table, self.hook.project_id, if_exists='replace')

    @staticmethod
    def add_score_range_to_variable(intervals, weights_to_intervals):
        scores = list(range(11)) * intervals.variable.nunique()
        weights_to_intervals['score'] = scores
        return weights_to_intervals

    @staticmethod
    def merge_weights_into_intervals(intervals, weights):
        weights_to_intervals = pd.merge(intervals, weights, left_on=['variable'], right_on=['variable'])
        return weights_to_intervals

    @staticmethod
    def replace_negative_and_null_values(ascending_variables):
        # Deal with negative values in ascending_variables by replacing them with 0
        for x in range(len(ascending_variables['interval_end'])):
            if ascending_variables.interval_end[x] < 0:
                ascending_variables.interval_end[x] = 0
        # In ascending_variables, replace NAN with 500
        for x, y in zip(ascending_variables.interval_end, range(len(ascending_variables.interval_end))):
            if np.isnan(x):
                ascending_variables.iloc[y:y + 1, 2:3] = 500
        return ascending_variables

    @staticmethod
    def get_end_intervals(ascending_variables, descending_variables):
        # Perform the equivalent of a "LEAD" function in SQL on interval_start to create interval_end
        descending_variables['interval_start'] = round(descending_variables['interval_start'], 3)
        descending_variables['interval_end'] = round(
            descending_variables.groupby('variable')['interval_start'].shift(-1), 3) + 0.001
        ascending_variables['interval_start'] = round(ascending_variables['interval_start'], 3)
        ascending_variables['interval_end'] = round(
            ascending_variables.groupby('variable')['interval_start'].shift(-1), 3) - 0.001
        return ascending_variables, descending_variables

    @staticmethod
    def pivot_and_rename(bins_asc, bins_desc):
        # Melt dataframe and rename 'value' into 'interval_start'
        descending_variables = bins_desc.melt()
        descending_variables.rename(columns={'value': 'interval_start'}, inplace=True)
        ascending_variables = bins_asc.melt()
        ascending_variables.rename(columns={'value': 'interval_start'}, inplace=True)
        return ascending_variables, descending_variables

    @staticmethod
    def divide_dataframe(bins):
        columns_sorted_descending = [
            'perc_reactions_over_2',
            'perc_no_show_shifts',
            'perc_vendor_time_over_7',
            'perc_customer_time_over_5',
            'acceptance_issues',
            'avg_issue_solving_time',
            'perc_manually_touched_orders',
            'estimated_prep_duration',
            'cancellation_rate',
            'pickup_distance',
            'dropoff_distance',
            'perc_manual_staffing'
        ]
        columns_sorted_ascending = [
            'perc_full_work_shifts',
            'reliability_rate',
            'vendor_density',
            'order_density',
            'dropoff_accuracy',
            'pickup_accuracy',
            'perc_rider_demand_fulfilled'
        ]
        # Create two dataframes out of those two lists
        bins_desc = bins[[x for x in columns_sorted_descending]]
        bins_asc = bins[[x for x in columns_sorted_ascending]]
        return bins_asc, bins_desc

    @staticmethod
    def get_quantiles(df):
        # Return percentile values
        bins = df.quantile([0, 0.2, 0.35, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95])
        bins.reset_index(level=0, inplace=True)
        # Drop index column
        bins = bins.drop('index', axis=1)
        return bins

    @property
    def segments_data_frame(self):
        return pd.DataFrame(
            {
                'segment': [
                    'rider_compliance',
                    'rider_compliance',
                    'rider_compliance',
                    'rider_compliance',
                    'rider_compliance',
                    'rider_compliance',
                    'dispatching',
                    'dispatching',
                    'vendor_compliance',
                    'vendor_compliance',
                    'vendor_compliance',
                    'infrastructure',
                    'infrastructure',
                    'infrastructure',
                    'infrastructure',
                    'infrastructure',
                    'infrastructure',
                    'staffing',
                    'staffing',
                ],
                'variable': [
                    'perc_reactions_over_2',
                    'perc_no_show_shifts',
                    'perc_full_work_shifts',
                    'perc_vendor_time_over_7',
                    'perc_customer_time_over_5',
                    'acceptance_issues',
                    'avg_issue_solving_time',
                    'perc_manually_touched_orders',
                    'estimated_prep_duration',
                    'cancellation_rate',
                    'reliability_rate',
                    'pickup_distance',
                    'dropoff_distance',
                    'vendor_density',
                    'order_density',
                    'dropoff_accuracy',
                    'pickup_accuracy',
                    'perc_rider_demand_fulfilled',
                    'perc_manual_staffing',
                ]
            }
        )
