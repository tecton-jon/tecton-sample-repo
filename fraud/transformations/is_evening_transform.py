from tecton import transformation

# mark a transaction as occurring during the evening if it occurs >= 11pm and before 7am
@transformation(mode='spark_sql')
def is_evening_transform(input_df, timestamp_column):
    return f'''
        SELECT
            *,
            case when hour({timestamp_column}) < 7 or hour({timestamp_column}) >= 23 then 1 else 0 end as is_evening
        FROM
            {input_df}
        '''