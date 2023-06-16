from tecton import batch_feature_view, transformation, const, FilteredSource, Aggregation
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from fraud.transformations.is_evening_transform import is_evening_transform
from fraud.transformations.is_weekend_transform import is_weekend_transform
from datetime import datetime, timedelta

def is_evening_inline(input_df, timestamp_column):
    from pyspark.sql.functions import hour, col, to_timestamp
    return input_df.withColumn("is_evening", hour(to_timestamp(col(timestamp_column))).isin([0, 1, 2, 3, 4, 5, 6, 23]).cast("int"))

def is_weekend_inline(input_df, timestamp_column):
    from pyspark.sql.functions import dayofweek, col, to_timestamp
    return input_df.withColumn("is_weekend", dayofweek(to_timestamp(col(timestamp_column))).isin([1,7]).cast("int"))

def is_evening_weekend_inline(input_df):
    from pyspark.sql.functions import when, col
    return input_df.withColumn("is_evening_weekend", when((col('is_evening') == 1) & (col('is_weekend') == 1), 1).otherwise(0))

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(days=1),
    aggregations=[Aggregation(column='is_evening_weekend', function='sum', time_window=timedelta(days=30)),
                  Aggregation(column='is_evening_weekend', function='sum', time_window=timedelta(days=60))],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    tags={'release': 'production'},
    owner='jon@tecton.ai',
    description='User transaction totals over a series of time windows, updated daily.'
)
def user_weekend_evening_txn_counts_inline(transactions_batch):
    return is_evening_weekend_inline(is_weekend_inline(is_evening_inline(transactions_batch, "timestamp"), "timestamp")) \
        .select('user_id', 'is_evening_weekend', 'timestamp')