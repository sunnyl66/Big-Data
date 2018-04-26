import sys
from pyspark.sql import SparkSession

def count_cuisine(spark):
    # Load csv
    df = spark.read.csv('/data/share/bdm/nyc_restaurants.csv', header = True, inferSchema = True).cache()

    # Grab Column
    dfCuisine = df.select(df['`CUISINE DESCRIPTION`'])

    # Group Column by count
    dfCuisine = dfCuisine.groupBy('`CUISINE DESCRIPTION`').count()

    # Sort, desecending count
    dfCuisine = dfCuisine.sort('count', ascending = False)

    # Write results
    dfCuisine.write.format('com.databricks.spark.csv').option('header', 'true').save('output')

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    session = SparkSession.builder.appName("Count Cuisine").getOrCreate()

    count_cuisine(session)