from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Pyspark Assignment") \
    .getOrCreate()

#Task 1    
df_input = spark.read.csv('/Users/saurabhkumar/Desktop/SQL/database.csv', header=True)

#Task 2
df_ts = df_input.withColumn("Timestamp",F.to_timestamp(F.concat(F.col("Date"), F.lit(" "), F.col("Time")))
df_ts.write.csv("/Users/saurabhkumar/Desktop/SQL/task-2")

#Task 3
df_magnitude = df_input.where(df_input.Magnitude > 5.0)
df_magnitude.write.csv("/Users/saurabhkumar/Desktop/SQL/task-3")

#Task 4
df_average = df_input.groupBy("Type").agg(F.mean('Depth'), F.mean('Magnitude'))
df_average.write.csv("/Users/saurabhkumar/Desktop/SQL/task-4")


#Task 6
def cal_lat_log_dist(self, df, lat1, long1, lat2, long2):

    df = df.withColumn('distance_in_kms' , \
        F.round((F.acos((F.sin(F.radians(F.col(lat1))) * F.sin(F.radians(F.lit(lat2)))) + \
               ((F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.lit(lat2)))) * \
                (F.cos(F.radians(F.col(long1)) - F.radians(F.lit(long2)))))
                   ) * F.lit(6371.0)), 4))
    return df
    
df_distance = cal_lat_log_dist(df_input, "Latitude", "Longitude", 0, 0)
df_distance.write.csv("/Users/saurabhkumar/Desktop/SQL/task-6")

#Task 8
# Included the final csv in the repository.


