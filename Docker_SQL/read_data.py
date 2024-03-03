import pandas as pd
df = pd.read_parquet('Docker_SQL\output.parquet')


print(pd.io.sql.get_schema(df.head(0), name='crazy_taxi'))