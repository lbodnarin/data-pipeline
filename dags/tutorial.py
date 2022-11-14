from airflow.decorators import dag, task
from datetime import datetime
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType
from sklearn.neighbors import LocalOutlierFactor
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import os

@dag(
    start_date=datetime(2009,1,1),
    end_date=datetime(2018,1,1),
    schedule_interval="@yearly"
)
def tutorial():
    @task()
    def aggregate_data(**kwargs):
        YYYY = kwargs["data_interval_start"].year
        builder = SparkSession\
            .builder\
            .appName(kwargs["task_instance_key_str"])\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        ss = configure_spark_with_delta_pip(builder).getOrCreate()
        ss\
            .read\
            .format("csv")\
            .option("mode", "FAILFAST")\
            .option("header", True)\
            .option("path", f"./work-dir/DATASET/{YYYY}.csv")\
            .schema(
                StructType([
                    StructField("FL_DATE", DateType()),
                    StructField("OP_CARRIER", StringType()),
                    StructField("OP_CARRIER_FL_NUM", IntegerType()),
                    StructField("ORIGIN", StringType()),
                    StructField("DEST", StringType()),
                    StructField("CRS_DEP_TIME", IntegerType()),
                    StructField("DEP_TIME", DoubleType()),
                    StructField("DEP_DELAY", DoubleType()),
                    StructField("TAXI_OUT", DoubleType()),
                    StructField("WHEELS_OFF", DoubleType()),
                    StructField("WHEELS_ON", DoubleType()),
                    StructField("TAXI_IN", DoubleType()),
                    StructField("CRS_ARR_TIME", IntegerType()),
                    StructField("ARR_TIME", DoubleType()),
                    StructField("ARR_DELAY", DoubleType()),
                    StructField("CANCELLED", DoubleType()),
                    StructField("CANCELLATION_CODE", StringType()),
                    StructField("DIVERTED", DoubleType()),
                    StructField("CRS_ELAPSED_TIME", DoubleType()),
                    StructField("ACTUAL_ELAPSED_TIME", DoubleType()),
                    StructField("AIR_TIME", DoubleType()),
                    StructField("DISTANCE", DoubleType()),
                    StructField("CARRIER_DELAY", DoubleType()),
                    StructField("WEATHER_DELAY", DoubleType()),
                    StructField("NAS_DELAY", DoubleType()),
                    StructField("SECURITY_DELAY", DoubleType()),
                    StructField("LATE_AIRCRAFT_DELAY", DoubleType()),
                    StructField("Unnamed: 27", StringType())
                ])
            )\
            .load()\
            .createOrReplaceTempView("temp_view")
        ss\
            .sql(
                '''
                SELECT ORIGIN, FL_DATE, CAST(AVG(DEP_DELAY) AS DECIMAL(6, 2)) AS AVG, COUNT(*) AS COUNT
                FROM temp_view
                GROUP BY ORIGIN, FL_DATE
                ORDER BY ORIGIN, FL_DATE
                '''
            )\
            .write\
            .format("delta")\
            .option("path", f"./work-dir/OUTPUT/aggregate_data/{YYYY}/")\
            .save()
    @task()
    def outlier_detection(**kwargs):
        YYYY = kwargs["data_interval_start"].year
        builder = SparkSession\
            .builder\
            .appName(kwargs["task_instance_key_str"])\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        ss = configure_spark_with_delta_pip(builder).getOrCreate()
        sdf = ss\
            .read\
            .format("delta")\
            .option("path", f"./work-dir/OUTPUT/aggregate_data/{YYYY}/")\
            .load()\
            .withColumn("LOF", lit(None).cast(IntegerType()))
        def func(pdf):
            X = pdf.loc[pdf["AVG"].notnull(), ["AVG", "COUNT"]].to_numpy()
            n_samples = len(X)
            if n_samples > 1:
                n_neighbors = (lambda x: 1 if x<=10 else round(x*0.05))(n_samples)
                clf = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=0.01)
                pdf.loc[pdf["AVG"].notnull(), "LOF"] = clf.fit_predict(X)
            return pdf
        sdf\
            .groupBy("ORIGIN")\
            .applyInPandas(func, sdf.schema)\
            .write\
            .format("delta")\
            .option("path", f"./work-dir/OUTPUT/outlier_detection/{YYYY}/")\
            .save()
    @task()
    def plot_data(**kwargs):
        YYYY = kwargs["data_interval_start"].year
        builder = SparkSession\
            .builder\
            .appName(kwargs["task_instance_key_str"])\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        ss = configure_spark_with_delta_pip(builder).getOrCreate()
        sdf = ss\
            .read\
            .format("delta")\
            .option("path", f"./work-dir/OUTPUT/outlier_detection/{YYYY}/")\
            .load()
        os.makedirs(f"./work-dir/OUTPUT/plot_data/{YYYY}/", exist_ok=True)
        def func(key, pdf):
            X = pdf.loc[pdf["AVG"].notnull(), ["FL_DATE", "AVG", "COUNT", "LOF"]].to_numpy()
            fig = plt.figure(figsize=(7.68, 7.68), layout="constrained")
            gs = fig.add_gridspec(nrows=2, ncols=1)
            ax0 = fig.add_subplot(gs[0, 0])
            ax0.set_ylabel("Average departure delay (min)")
            ax0.grid(True, zorder=1)
            ax0.xaxis.set_major_locator(mdates.MonthLocator())
            ax0.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b-%d"))
            ax0.text(
                0.5,
                0.5,
                f"{key[0]}",
                zorder=3,
                color="k",
                fontsize=30,
                ha="center",
                va="center",
                transform=ax0.transAxes,
                alpha=0.5
            )
            ax0.fill_between(
                X[:, 0],
                X[:, 1].astype("single").min(),
                X[:, 1].astype("single"),
                zorder=2,
                linewidth=1.,
                color="darkgrey",
                edgecolor="k",
                alpha=0.25,
                label="Average departure delay per day"
            )
            ax0.scatter(
                X[X[:, 3]==-1, 0],
                X[X[:, 3]==-1, 1],
                zorder=2,
                s=80,
                color="r",
                edgecolor="k",
                alpha=0.75,
                label="Outlier scores: %s" % [(x[0].strftime("%b-%d"), float(x[1])) for x in X[X[:, 3]==-1, :][:, [0, 1]]]
            )
            ax0.legend(
                bbox_to_anchor=(0., 1.02, 1., .102),
                loc="lower left",
                mode="expand",
                borderaxespad=0.
            )
            for label in ax0.get_xticklabels(which='major'):label.set(rotation=30, horizontalalignment='right')
            ax1 = fig.add_subplot(gs[1, 0])
            ax1.set_ylabel("Number of flights")
            ax1.grid(True, zorder=1)
            ax1.xaxis.set_major_locator(mdates.MonthLocator())
            ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b-%d"))
            ax1.text(
                0.5,
                0.5,
                f"{key[0]}",
                zorder=3,
                color="k",
                fontsize=30,
                ha="center",
                va="center",
                transform=ax1.transAxes,
                alpha=0.5
            )
            ax1.fill_between(
                X[:, 0],
                X[:, 2].astype("uintc").min(),
                X[:, 2].astype("uintc"),
                zorder=2,
                linewidth=1.,
                color="darkgrey",
                edgecolor="k",
                alpha=0.25,
                label="Number of flights per day"
            )
            ax1.scatter(
                X[X[:, 3]==-1, 0],
                X[X[:, 3]==-1, 2],
                zorder=2,
                s=80,
                color="r",
                edgecolor="k",
                alpha=0.75,
                label="Outlier scores: %s" % [(x[0].strftime("%b-%d"), x[1]) for x in X[X[:, 3]==-1, :][:, [0, 2]]]
            )
            ax1.legend(
                bbox_to_anchor=(0., 1.02, 1., .102),
                loc="lower left",
                mode="expand",
                borderaxespad=0.
            )
            for label in ax1.get_xticklabels(which='major'):label.set(rotation=30, horizontalalignment='right')
            fig.savefig(f"./work-dir/OUTPUT/plot_data/{pdf.iloc[0, 1].year}/{key[0]}.png")
            plt.close(fig=fig)
            return pdf
        sdf\
            .groupBy("ORIGIN")\
            .applyInPandas(func, sdf.schema)\
            .collect()
    aggregate_data() >> outlier_detection() >> plot_data()
tutorial()