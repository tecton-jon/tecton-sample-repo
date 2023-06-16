from tecton import batch_feature_view
from fraud.entities import user
from fraud.data_sources.credit_scores import credit_scores_batch
from datetime import datetime, timedelta

@batch_feature_view(
    # explicit name of the feature view, which will override the default of using the function name below
    name='user_credit_categories',
    sources=[credit_scores_batch],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
)
# function name is implicitly going to be used as feature view
def user_credit_categories(credit_scores):
    # define our 3 feature names based on different credit score tiering logic
    return f'''
        SELECT
            user_id,
            IF (credit_score < 500, 1, 0) as cat_user_has_bad_credit,
            IF (credit_score > 670, 1, 0) as cat_user_has_good_credit,
            IF (credit_score > 740, 1, 0) as cat_user_has_great_credit,
            timestamp
        FROM
            {credit_scores}
        '''