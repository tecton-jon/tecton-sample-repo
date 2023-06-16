from tecton import batch_feature_view, transformation, const, FilteredSource, Aggregation
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from fraud.transformations.is_evening_transform import is_evening_transform
from fraud.transformations.is_weekend_transform import is_weekend_transform
from datetime import datetime, timedelta

@transformation(mode='spark_sql')
def weekend_evening_transaction_transform(input_df):
    return f'''
        SELECT
            user_id,
            case when is_weekend = 1 and is_evening = 1 then 1 else 0 end as weekend_evening_transform,
            timestamp
        FROM
            {input_df}
        '''

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='pipeline',
    aggregation_interval=timedelta(days=1),
    aggregations=[Aggregation(column='weekend_evening_transform', function='sum', time_window=timedelta(days=30)),
                  Aggregation(column='weekend_evening_transform', function='sum', time_window=timedelta(days=60))],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='User transaction totals over a series of time windows, updated daily.'
)
def user_weekend_evening_transaction_counts(transactions_batch):
    timestamp_key = const("timestamp")
    return weekend_evening_transaction_transform(
        is_weekend_transform(
            is_evening_transform(transactions_batch, timestamp_key)
            , timestamp_key
        ),
    )