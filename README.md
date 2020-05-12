# ScalaSparkJoins
Example usin Scala and Spark 2.3  for create joins loading 2 csv.

Example for running application:

spark-submit \
  --class "com.scala.spark.joins.SparkJoins" \
  --master local[*] \
 /tmp/spark/scala/example-0.0.1-SNAPSHOT.jar   /tmp/examples/spark/join/UserDetails.csv /tmp/examples/spark/join/AddressDetails.csv /tmp/examples/spark/scala/joins
