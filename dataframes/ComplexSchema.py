'''
DataFrames From Complex Schema

@author: Mallikarjuna G
'''

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import column
    from pyspark.sql import Row
    from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DoubleType, ArrayType, MapType
    
    spark = SparkSession \
        .builder \
        .appName("DataFrame From Complex Schema") \
        .getOrCreate()

    sc = spark.sparkContext

    ##
    ## Example 1: Nested StructType for Nested Rows
    ##

    studentMarks = [
        Row(1, Row("john", "doe"), 6, Row(70.0, 45.0, 85.0)),
        Row(2, Row("jane", "doe"), 9, Row(80.0, 35.0, 92.5))
    ]

    studentMarksRDD = sc.parallelize(studentMarks, 4)
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StructType(
                [
                    StructField("first", StringType(), nullable=True),
                    StructField("last", StringType(), nullable=True)
                ]
            ), nullable=True),
            StructField("standard", IntegerType(), nullable=True),
            StructField("marks", StructType(
                [
                    StructField("maths", DoubleType(), nullable=True),
                    StructField("physics", DoubleType(), nullable=True),
                    StructField("chemistry", DoubleType(), nullable=True)
                ]
            ), nullable=True)
        ]
    )

    studentMarksDF = spark.createDataFrame(studentMarksRDD, schema)

    print("Schema with nested struct")
    studentMarksDF.printSchema()

    print("DataFrame with nested Row")
    studentMarksDF.show()

    print("Select the column with nested Row at the top level")
    studentMarksDF.select("name").show()

    print("Select deep into the column with nested Row")
    studentMarksDF.select("name.first").show()

    print("The column function getField() seems to be the 'right' way")
    studentMarksDF.select(studentMarksDF["name"].getField("last")).show()

    ##
    ## Example 2: ArrayType
    ##

    studentMarks2 = [
        Row(1, Row("john", "doe"), 6, [70.0, 35.0, 85.0]),
        Row(2, Row("jane", "doe"), 9, [80.0, 35.0, 92.5, 35.0, 46.0])
    ]
    
    studentMarks2Rdd = spark.sparkContext.parallelize(studentMarks2, 4)

    schema2 = StructType()\
        .add("id", IntegerType(), nullable=True)\
        .add("name", StructType()\
             .add("first", StringType(), nullable=True)\
             .add("last", StringType(), nullable=True)
             , nullable=True)\
        .add("standard", IntegerType(), True)\
        .add("marks", ArrayType(DoubleType(), containsNull=False), nullable = True)

    studentMarks2DF = spark.createDataFrame(studentMarks2Rdd, schema2)

    print("Schema with array")
    studentMarks2DF.printSchema()

    print("DataFrame with array")
    studentMarks2DF.show()

    print("Count elements of each array in the column")
    studentMarks2DF.select("id", F.size("marks").alias("count")).show()

    print("Explode the array elements out into additional rows")
    studentMarks2DF.select("id", F.explode("marks").alias("scores")).show()

    print("Apply a membership test to each array in a column")
    studentMarks2DF.select("id", F.array_contains("marks", 35.0).alias("has35")).show()

    print("Select the first element of each array in a column")
    studentMarks2DF.select("id", studentMarks2DF.marks[0]).show()

    print("Select the 100th element of each array in a column. Returns Null when no such element")
    studentMarks2DF.selectExpr("id", "marks[100]").show()

    print("Use column function getItem() with am index value for selecting a particular value")
    studentMarks2DF.select("id", studentMarks2DF["marks"].getItem(3)).show()

    print("Select first 3 elements from the Marks array as an Array")
    studentMarks2DF.select("id", "name.first", [studentMarks2DF.marks[i] for i in range(3)]).show()

    ##
    ## Example 3: MapType
    ##

    studentMarks3 = [
        Row(1, Row("john", "doe"), 6, {"m":70.0, "p":35.0, "c":85.0}),
        Row(2, Row("jane", "doe"), 9, {"m":80.0, "p":35.0, "c":92.5, "s":35.0, "e":46.0})
    ]

    studentMarks3RDD = spark.sparkContext.parallelize(studentMarks3, 4)

    schema3 = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StructType(
                [
                    StructField("first", StringType()),
                    StructField("last", StringType())
                ]
            ), True),
            StructField("standard", IntegerType(), True),
            StructField("marks", MapType(StringType(), DoubleType()))
        ]
    )

    studentMarks3DF = spark.createDataFrame(studentMarks3RDD, schema3)

    print("Schema with map type")
    studentMarks3DF.printSchema()

    print("DataFrame with map type field")
    studentMarks3DF.show(truncate=False)

    print("Count elements of each map in the column")
    studentMarks3DF.select("id", F.size("marks").alias("count")).show()

    ## notice you get one column from the keys and one from the values
    print("Explode the map elements out into additional rows")
    studentMarks3DF.select("id", F.explode("marks")).show()

    """ 
        MapType is actually a more flexible version of StructType, since you
        can select down into fields within a column, and the rows where
        an element is missing just return a null
    """
    print("Select deep into the column with a Map")
    studentMarks3DF.select("id", "marks.e").show()

    print("The column function getItem() seems to be the 'right' way")
    studentMarks3DF.select("id", studentMarks3DF["marks"].getItem("p")).show()
