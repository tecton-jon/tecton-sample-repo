from tecton import batch_feature_view, FilteredSource
from fraud.entities import user
from fraud.data_sources.credit_score import credit_scores_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(credit_scores_batch, start_time_offset=timedelta(days=-29))],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
)
def user_has_great_credit(credit_scores):
    return f'''
        SELECT
            user_id,
            IF (credit_score > 740, 1, 0) as user_has_great_credit,
            timestamp
        FROM
            {credit_scores}
        '''