import pandas as pd
import polars as pl
import time
import matplotlib.pyplot as plt

# Reading Data From a CSV File
pandas_time = []
polars_time = []
for i in range(5):
    start = time.time()
    df_pandas = pd.read_csv("/content/train.csv")
    end = time.time()
    pandas_time.append(end-start)

    start = time.time()
    df_polars = pl.read_csv("/content/train.csv")
    end = time.time()
    polars_time.append(end-start)

pandas_mean = sum(pandas_time)/5
polars_mean = sum(polars_time)/5

plt.bar(['Pandas', 'Polars'], [pandas_mean, polars_mean])
plt.title('Reading Data From a CSV File')
plt.xlabel('Library')
plt.ylabel('Time (s)')
plt.show()

# Selecting Columns
pandas_time = []
polars_time = []
for i in range(5):
    start = time.time()
    df_pandas_select = df_pandas[["User_ID", "Gender", "Purchase"]]
    end = time.time()
    pandas_time.append(end-start)

    start = time.time()
    df_polars_select = df_polars.select(["User_ID", "Gender", "Purchase"])
    end = time.time()
    polars_time.append(end-start)

pandas_mean = sum(pandas_time)/5
polars_mean = sum(polars_time)/5

plt.bar(['Pandas', 'Polars'], [pandas_mean, polars_mean])
plt.title('Selecting Columns')
plt.xlabel('Library')
plt.ylabel('Time (s)')
plt.show()

# Filtering Rows
pandas_time = []
polars_time = []
for i in range(5):
    start = time.time()
    df_pandas_filter = df_pandas.loc[df_pandas["Age"] == "0-17"]
    end = time.time()
    pandas_time.append(end-start)

    start = time.time()
    df_polars_filter = df_polars.filter(pl.col("Age") == "0-17")
    end = time.time()
    polars_time.append(end-start)

pandas_mean = sum(pandas_time)/5
polars_mean = sum(polars_time)/5

plt.bar(['Pandas', 'Polars'], [pandas_mean, polars_mean])
plt.title('Filtering Rows')
plt.xlabel('Library')
plt.ylabel('Time (s)')
plt.show()

# Grouping and Aggregating Data
pandas_time = []
polars_time = []
for i in range(5):
    start = time.time()
    df_pandas_groupby = df_pandas.groupby(["Age"]).agg({"Purchase": "mean"})
    end = time.time()
    pandas_time.append(end-start)

    start = time.time()
    df_polars_groupby = df_polars.groupby("Age").agg({"Purchase": pl.mean("Purchase")})
    end = time.time()
    polars_time.append(end-start)

pandas_mean = sum(pandas_time)/5
polars_mean = sum(polars_time)/5

plt.bar(['Pandas', 'Polars'], [pandas_mean, polars_mean])
plt.title('Grouping and Aggregating Data')
plt.xlabel('Library')
plt.ylabel('Time (s)')
plt.show()

# Applying Functions to Data
pandas_time = []
polars_time = []
for i in range(5):
    start = time.time()
    df_pandas_apply = df_pandas["Purchase"].apply(lambda x: x*2)
    end = time.time()
    pandas_time.append(end-start)

    start = time.time()
    df_polars_apply = df_polars.select(pl.col("Purchase") * 2)
    end = time.time()
    polars_time.append(end-start)

pandas_mean = sum(pandas_time)/5
polars_mean = sum(polars_time)/5

plt.bar(['Pandas', 'Polars'], [pandas_mean, polars_mean])
plt.title('Applying Functions to Data')
plt.xlabel('Library')
plt.ylabel('Time (s)')
plt.show()

# Merging Data
pandas_time = []
polars_time = []
df_pandas_user = df_pandas[["User_ID", "Gender"]]
df_pandas_purchase = df_pandas[["User_ID", "Purchase"]]
df_polars_user = df_polars.select(["User_ID", "Gender"])
df_polars_purchase = df_polars.select(["User_ID", "Purchase"])

for i in range(5):
    start = time.time()
    df_pandas_merge = pd.merge(df_pandas_user, df_pandas_purchase, on="User_ID")
    end = time.time()
    pandas_time.append(end-start)

    start = time.time()
    df_polars_merge = df_polars_user.join(df_polars_purchase, on="User_ID")
    end = time.time()
    polars_time.append(end-start)

pandas_mean = sum(pandas_time)/5
polars_mean = sum(polars_time)/5

plt.bar(['Pandas', 'Polars'], [pandas_mean, polars_mean])
plt.title('Merging Data')
plt.xlabel('Library')
plt.ylabel('Time (s)')
plt.show()
