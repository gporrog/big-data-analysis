/////////////////////
// PART 2 - SPARK ///
/////////////////////

// Docker image
// docker exec -it spark-master bash

// Open the spark shell
// /spark/bin/spark-shell 

// Load the DataFrame
val netflix = spark.read.option("header", "true").option("inferSchema", "true").csv("./netflix.csv")

// Print schema
netflix.printSchema
// Result:
// root
//  |-- show_id: string (nullable = true)
//  |-- type: string (nullable = true)
//  |-- title: string (nullable = true)
//  |-- director: string (nullable = true)
//  |-- cast: string (nullable = true)
//  |-- country: string (nullable = true)
//  |-- date_added: string (nullable = true)
//  |-- release_year: integer (nullable = true)
//  |-- rating: string (nullable = true)
//  |-- duration: integer (nullable = true)
//  |-- duration_type: string (nullable = true)
//  |-- listed_in: string (nullable = true)
//  |-- description: string (nullable = true)

// 1. How many rows in the dataset? 
netflix.count() // 7788 (without header)

// 2. Are there more TV Shows or Movies in the dataset?
netflix.groupBy("type").count().show() // More movies (5377) than TV shows (2410)

// 3. Compute the total duration for each type (Movie/TV Show) in the dataset.  You may
// disregard the season/minute distinction for this question
netflix.groupBy("type").agg(sum("duration")).show()
// TV Show:  4280 Seasons
// Movies:   533979 min

// 4. What is the oldest (by release year) movie in the dataset?
netflix.filter("release_year is not null").sort("release_year").select("title").first.getString(0)
// Result Pioneers: First Women Filmmakers*

// 5.  Determine the number of movies per country. Some country fields will contain multiple
// countries delimited by a semicolon

val cfm = netflix.filter(netflix("type") === "Movie" && netflix("country").isNotNull).select("country").flatMap(f => { f.getString(0).split(";") })
val countriesDF = cfm.toDF("country")

countriesDF.limit(6).show() 
// Result:
+-------------+
|      country|
+-------------+
|       Mexico|
|    Singapore|
|United States|
|United States|
|        Egypt|
|United States|
+-------------+

// Then:
countriesDF.groupBy("country").count().orderBy($"count".desc).limit(3).show()

+--------------+-----+                                                          
|       country|count|
+--------------+-----+
| United States| 2100|
|         India|  883|
|United Kingdom|  341|
+--------------+-----+

// 6. Determine the most frequently occurring term in the the "description" field (i.e what
// word appears the most often in that column!)

val words = netflix.withColumn("description", split(col("description"), " ")).select(explode(col("description")).as("words"))

words.groupBy("words").count().orderBy($"count".desc).limit(3).show()

// Result: 
+-----+-----+                                                                   
|words|count|
+-----+-----+
|    a| 8825|
|  the| 6821|
|  and| 5573|
+-----+-----+

// 7.  To the nearest decade (e.g.  2000-2009 == 2000s), what are the top 5 decades in terms
// of number of films released
val yearDF = netflix.filter(netflix("type") === "Movie").select(netflix("release_year").as("year").cast("String"))

val decadeDF = yearDF.withColumn("decade", when(col("year").startsWith("2"), concat(col("year").substr(1, 3), lit("0s"))).otherwise(concat(col("year").substr(3, 1), lit("0s"))))

decadeDF.show()
+----+------+
|year|decade|
+----+------+
|2016| 2010s|
|2011| 2010s|
|2009| 2000s|
|2008| 2000s|
|2019| 2010s|
|1997|   90s|
+----+------+

decadeDF.groupBy("decade").count().orderBy($"count".desc).show()
// Result:
+------+-----+                                                                  
|decade|count|
+------+-----+
| 2010s| 3951|
| 2000s|  601|
| 2020s|  423|
|   90s|  194|
|   80s|   99|
|   70s|   63|
|   60s|   22|
|   40s|   13|
|   50s|   11|
+------+-----+

// 8. In what month have the most films been added to Netflix (i.e.  sum of all the films
// added in a given month)

val months = netflix.filter(netflix("type") === "Movie").select("date_added").map(f => { f.getString(0).split(" ")(0) }).groupBy("value").count()

months.orderBy($"count".desc).limit(3).show()

// Result:
+--------+-----+                                                                
|   value|count|
+--------+-----+
| January|  560|
|December|  554|
| October|  553|
+--------+-----+
