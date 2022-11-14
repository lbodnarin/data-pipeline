# Running Spark ETL Jobs with Airflow

This tutorial explains how to run an ETL workload on your local machine.

I wrote an ETL pipeline that implements the **split-apply-combine** pattern which consists of three steps:

> * Split the data into groups using `DataFrame.groupBy()`.
> * Apply a function on each group using `GroupedData.applyInPandas()`.
> * Combine the results into a new PySpark `DataFrame`.

Here is my [GitHub repo](https://github.com/lbodnarin/data-pipeline.git).

## Prerequisites

Before you begin, ensure that you have the following prerequisites:

1. Install [Docker Engine](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/).
1. Install [Git](https://docs.github.com/en/get-started/quickstart/set-up-git).
1. Install the [Kaggle API](https://www.kaggle.com/docs/api) and authentication.

## Step 1: Download repo

```shell
git clone https://github.com/lbodnarin/data-pipeline.git
```

`OUTPUT`

```shell
data-pipeline/
├── dags/
│   └── tutorial.py
├── docker-build/
│   ├── Dockerfile
│   └── requirements.rst
├── work-dir/
│   ├── DATASET/
│   └── OUTPUT/
├── docker-compose.yml
└── README.md
```

## Step 2: Download dataset

```shell
kaggle datasets download \
yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018 \
--path ./data-pipeline/work-dir/DATASET/ \
--unzip
```

`OUTPUT`

```shell
data-pipeline/work-dir/DATASET/
├── 2009.csv
├── 2010.csv
├── 2011.csv
├── 2012.csv
├── 2013.csv
├── 2014.csv
├── 2015.csv
├── 2016.csv
├── 2017.csv
└── 2018.csv
```

## Step 3: Set Airflow UID

```shell
echo -e "AIRFLOW_UID=$(id -u)" > ./data-pipeline/.env
```

## Step 4: Start ETL workload

```shell
docker compose \
--file ./data-pipeline/docker-compose.yml up \
--detach
```

## Step 5: Airflow UI

* Visit `http://localhost:8080/` in a browser.
* Sign in with `admin` for both your username and password.
* The default page in the Airflow UI is the `DAGs` view.

![Airflow-DAGs.png](https://cdn.hashnode.com/res/hashnode/image/upload/v1668443535836/MerncghpL.png?width=1282)

## Step 6: DAG

* Click the name of the DAG to access the `Grid` view.
* Each column represents one `DAG Run`.
* A DAG Run does not start to run until its associated data interval has ended.

```python
@dag(
    start_date=datetime(2009,1,1),
    end_date=datetime(2018,1,1),
    schedule_interval="@yearly"
)
def tutorial():
```

![Airflow-Grid.png](https://cdn.hashnode.com/res/hashnode/image/upload/v1668443649635/dfD5fWQPg.png?width=1282)

## Step 7: Tasks

* The `Graph` view shows task dependencies and relationships.
* Tasks are represented as `_PythonDecoratedOperators`.
* A task will run when all of its upstream tasks have succeeded.

![Airflow-Graph.png](https://cdn.hashnode.com/res/hashnode/image/upload/v1668443731098/o3Nn_nqeH.png?width=1282)

### aggregate_data

* It computes the `average departure delay per day` for each airport.
* It computes the `number of flights per day` for each airport.
* It saves the results as a `Delta Lake` table.

```python
@task()
def aggregate_data(**kwargs):
    # Function prototype
    ss = configure_spark_with_delta_pip(builder).getOrCreate()
    ss\
        .read\
        .format("csv")\
        .option("mode", "FAILFAST")\
        .option("header", True)\
        .option("path", PATH)\
        .schema(struct)\
        .load()\
        .createOrReplaceTempView("temp_view")
    ss\
        .sql(
            '''
            SELECT ORIGIN, FL_DATE, AVG(DEP_DELAY), COUNT(*)
            FROM temp_view
            GROUP BY ORIGIN, FL_DATE
            ORDER BY ORIGIN, FL_DATE
            '''
        )\
        .write\
        .format("delta")\
        .option("path", PATH)\
        .save()
```

`OUTPUT`

```shell
data-pipeline/work-dir/OUTPUT/aggregate_data/
├── 2009/
├── ...
└── 2018/
  ├── _delta_log/
  │   └── 00000000000000000000.json
  └── part-00000-2207e6e5-3e2f-4b88-b3b3-b6c39c64f45f-c000.snappy.parquet
```

### outlier_detection

* It computes the `LocalOutlierFactor` w.r.t. the aggregated data.
* Inliers are labeled 1, while outliers are labeled -1.
* It saves the results as a `Delta Lake` table.

```python
@task()
def outlier_detection(**kwargs):
    # Function prototype
    ss = configure_spark_with_delta_pip(builder).getOrCreate()
    sdf = ss\
        .read\
        .format("delta")\
        .option("path", PATH)\
        .load()\
        .withColumn("LOF", lit(None))
    def func(pdf):
        clf = LocalOutlierFactor(n_neighbors, contamination)
        pdf["LOF"] = clf.fit_predict(X)
        return pdf
    sdf\
        .groupBy("ORIGIN")\
        .applyInPandas(func, sdf.schema)\
        .write\
        .format("delta")\
        .option("path", PATH)\
        .save()
```

`OUTPUT`

```shell
data-pipeline/work-dir/OUTPUT/outlier_detection/
├── 2009/
├── ...
└── 2018/
  ├── _delta_log/
  │   └── 00000000000000000000.json
  └── part-00000-48b21636-cd2f-4e63-9338-a1e93ad2bde1-c000.snappy.parquet
```

### plot_data

* It graphs the `average departure delay per day` for each airport.
* It graphs the `number of flights per day` for each airport.
* It highlights the `outlier scores`.

```python
@task()
def plot_data(**kwargs):
    # Function prototype
    ss = configure_spark_with_delta_pip(builder).getOrCreate()
    sdf = ss\
        .read\
        .format("delta")\
        .option("path", PATH)\
        .load()
    def func(key, pdf):
        fig = plt.figure()
        gs = fig.add_gridspec(nrows=2, ncols=1)
        # Average departure delay per day
        ax0 = fig.add_subplot(gs[0, 0])
        ax0.fill_between(x, y)
        ax0.scatter(x, y, label="Outlier scores")
        # Number of flights per day
        ax1 = fig.add_subplot(gs[1, 0])
        ax1.fill_between(x, y)
        ax1.scatter(x, y, label="Outlier scores")
        fig.savefig(PATH)
        return pdf
    sdf\
        .groupBy("ORIGIN")\
        .applyInPandas(func, sdf.schema)\
        .collect()
```

`OUTPUT`

```shell
data-pipeline/work-dir/OUTPUT/plot_data/
├── 2009/
├── ...
└── 2018/
  ├── ...
  ├── ATL.png
  ├── ...
  ├── ORD.png
  └── ...
```

![ATL-Airport.png](https://cdn.hashnode.com/res/hashnode/image/upload/v1668443825676/86FpFNL6v.png?width=1282)

## Step 8: Clean up

```shell
docker compose \
--file ./data-pipeline/docker-compose.yml down \
--rmi all
```

## Conclusion

Keep these notes in mind when using the [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html) in production:

`DataFrame.collect()`

> * It collects the distributed data to the driver side as the local data in Python.
> * It can throw an OOM error when the dataset is too large to fit in memory.

`GroupedData.applyInPandas()`

> * It loads all the data of a group into memory before the function is applied.
> * It can throw an OOM error when a group is too large to fit in memory.