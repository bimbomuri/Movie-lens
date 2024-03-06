import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col,expr
import collections

spark = SparkSession.builder.master("local").getOrCreate()
    

# Update the file path with the correct one
file_path = '/Users/mac/Downloads/SparkCourse/data/movies.dat'

# Read the data as text and split based on the delimiter
lines = spark.read.text(file_path).rdd.map(lambda x: x[0].split(",")).toDF(["Id", "Movie", "Genre"])

# Extract header from the first row
header = lines.first()

# Filter out the header row from the DataFrame
lines = lines.toDF(*header)
#Add index
lines = lines.withColumn('index',monotonically_increasing_id())
#remove rows
rows_remove = [0]
lines = lines.filter(~lines.index.isin(rows_remove))
#drop index
lines= lines.drop('index')
#split the year from the Movie title
#year = lines.filter(col('Movie').substr(-5,4).cast('int').isNotNull())
#create a new column year
lines = lines.withColumn('Year', col('Movie').substr(-5,4).cast('int'))
# Show the first 5 rows
lines.show(5, truncate=False)