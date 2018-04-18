'''
DataFrame Basics

@author: Mallikarjuna G
'''

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = SparkSession \
        .builder \
        .appName("DataFrame Basics") \
        .getOrCreate()

    sc = spark.sparkContext

    emp = [
        (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
        (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
        (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
        (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
        (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
        (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
        (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
        (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
        (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
        (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
        (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    ]

    headers = ("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

    # Method 1: Creating Dataframe directly from a List of tuples
    empDfFromList = spark.createDataFrame(emp, headers)

    empDfFromList.show(4, truncate=False)

    # Method 2: Create an RDD from Seq of tuples with desired parallelism and then create a DataFrame
    empRDD = sc.parallelize(emp, 4)

    empDfFromRDD = spark.createDataFrame(empRDD, headers)

    print("*** no of partitions in empDfFrom {}".format(empDfFromList.rdd.getNumPartitions()))

    print("*** no of partitions in empDfFromList {}".format(empDfFromRDD.rdd.getNumPartitions()))

    print("*** display first two rows from the dataFrame in Array[Row] format")

    print(empDfFromRDD.head(2))

    print("*** columns just gives you the fields")

    print(empDfFromList.columns)

    print("*** It's better to use printSchema()")

    print(empDfFromList.printSchema())

    print("*** show() gives you neatly formatted data")

    print(empDfFromList.show())

    print("*** use show(n) to display n rows\n")

    empDfFromList.show(2)

    print("*** use select(n,truncate=false) to display n rows and to display column contents without truncation\n")

    empDfFromList.show(2, truncate=False)

    print("*** use select() to choose one column\n")

    empDfFromList.select("empno").show()

    print("*** use select() for multiple columns\n")

    empDfFromList.select("ename", "sal").show()

    print("*** use filter() to choose rows\n")

    empDfFromList.filter(empDfFromList["job"] == "MANAGER").show()

    print("*** use where() to choose rows, same as filter()\n")

    empDfFromList.where(F.col("job") == "MANAGER").show()

    print("*** ADVANCED: using multiple conditions in where() to choose rows\n")

    empDfFromList.where((empDfFromList.job == "MANAGER") & (empDfFromList.deptno.isin(10, 20)) & (empDfFromList.mgr == 7839)).show()

    print("*** use as() or alias to rename columns")

    empDfFromList.select(empDfFromList.ename.alias("EmpName"), empDfFromList["empno"].alias("EmpNo")).show()


    print("*** Select list containing Column Names of type Column")
    empDfFromList.select(F.column("empno"), F.col("ename"), empDfFromList.job, empDfFromList["sal"]).show(2)

    print("*** Select list with Column Names of type String")
    empDfFromList.select("empno", "ename", "mgr").show(2)

    fieldList = ["ename", "sal", "comm", "job"]

    print("*** Select list of Column Names of type String")
    empDfFromList.select([col for col in fieldList]).show(2)

    # Define a List(Column) from List
    fieldListOfCol = map(lambda x : F.col(x), fieldList)
    print(type(fieldListOfCol))
    print("*** Select list with Seq of Column Names of type Column")
    empDfFromList.select([col for col in fieldListOfCol]).show(2)