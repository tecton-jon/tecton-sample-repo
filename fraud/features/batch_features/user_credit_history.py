from tecton import batch_feature_view, Aggregation, materialization_context
from fraud.entities import user
from fraud.data_sources.credit_scores import credit_scores_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[credit_scores_batch],
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1),
    aggregations=[Aggregation(column='credit_score', function='min', time_window=timedelta(days=30)),
                  Aggregation(column='credit_score', function='max', time_window=timedelta(days=30)),
                  Aggregation(column='credit_score', function='mean', time_window=timedelta(days=30)),
                  Aggregation(column='credit_score', function='min', time_window=timedelta(days=180)),
                  Aggregation(column='credit_score', function='max', time_window=timedelta(days=180)),
                  Aggregation(column='credit_score', function='mean', time_window=timedelta(days=180))],
    online=True,
    offline=True,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2023, 1, 1)
)
def user_credit_history(credit_scores, context=materialization_context()):
    return f'''
        SELECT
            user_id,
            credit_score,
            timestamp
        FROM
            {credit_scores}
        '''