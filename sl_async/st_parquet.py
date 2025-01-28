


def write_parquet(df_chunk, path):
    import pyarrow as pa
    from pyarrow import parquet as pq

    for col in df_chunk.select_dtypes(include=['object']).columns:
        df_chunk[col] = df_chunk[col].astype(str)
    table = pa.Table.from_pandas(df_chunk)
    pq_writer = pq.ParquetWriter(path, table.schema, compression='SNAPPY')
    pq_writer.write_table(table)
    pq_writer.close()
