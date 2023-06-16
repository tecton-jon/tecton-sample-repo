from tecton import batch_feature_view
from fraud.entities import user
from fraud.data_sources.credit_scores import credit_scores_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[credit_scores_batch],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
)
def user_has_good_credit(credit_scores):
    return f'''
        SELECT
            user_id,
            IF (credit_score > 670, 1, 0) as user_has_good_credit,
            timestamp
        FROM
            {credit_scores}
        '''