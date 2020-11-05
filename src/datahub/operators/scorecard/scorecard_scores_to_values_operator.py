import pandas as pd
from airflow.models import BaseOperator

from datahub.hooks.bigquery_hook import BigQueryHook


class ScorecardScoresToValuesOperator(BaseOperator):
    template_ext = ['.sql']
    template_fields = ('target_table', 'sql', 'sql_score')

    ui_color = '#91baf5'
    ui_fgcolor = '#000'

    def __init__(self, target_table, sql, sql_score, bigquery_conn_id='bigquery_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hook = None
        self.bigquery_conn_id = bigquery_conn_id
        self.target_table = target_table
        self.sql = sql
        self.sql_score = sql_score

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
        scores = self.hook.get_pandas_df(self.sql_score, dialect='standard')

        df = self.pivot_dataframe(df)

        merged_scores = self.merge_scores_into_dataframe(df, scores)

        scores_to_values = self.identify_and_keep_correct_scores(merged_scores)

        scores_to_values.to_gbq(self.target_table, self.hook.project_id, if_exists='replace')

        self.log.info('All done')

    @staticmethod
    def identify_and_keep_correct_scores(merged_scores):
        # Identify the right score in each row
        trues = list()
        for value, start, end, indicator, score in zip(merged_scores.value, merged_scores.interval_start,
                                                       merged_scores.interval_end, merged_scores.indicator,
                                                       merged_scores.score):
            if indicator == 'desc' and start >= value >= end:
                trues.append('true')
            elif indicator == 'desc' and score == 10 and value <= end:
                trues.append('true')
            elif indicator == 'asc' and start <= value <= end:
                trues.append('true')
            elif indicator == 'asc' and score == 10 and value >= end:
                trues.append('true')
            else:
                trues.append('false')
        # Add list of booleans to merged_scores
        merged_scores['keep'] = trues
        # Keep only rows where kpi value is within the specified interval
        scores_to_values = merged_scores.query('keep == "true"')
        # Get rid of 'keep' column
        scores_to_values.drop(['keep'], axis=1, inplace=True)
        return scores_to_values

    @staticmethod
    def merge_scores_into_dataframe(df, scores):
        # Join scores to df
        merged_scores = pd.merge(df, scores, left_on=['variable'], right_on=['variable'])
        return merged_scores

    @staticmethod
    def pivot_dataframe(df):
        # Create list of identifier variables and of columns to pivot, to use in the melt function
        index_list = [str(x) for x in list(df.columns[:4])]
        variable_list = [str(x) for x in list(df.columns[4:])]
        # Pivot df
        df = pd.melt(df, id_vars=index_list, value_vars=variable_list)
        return df
