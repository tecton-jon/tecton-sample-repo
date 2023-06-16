from tecton import transformation

@transformation(mode='pyspark')
def is_weekend_transform(input_df, timestamp_column):
    from pyspark.sql.functions import dayofweek, col, to_timestamp
    return input_df.withColumn("is_weekend", dayofweek(to_timestamp(col(timestamp_column))).isin([1,7]).cast("int"))