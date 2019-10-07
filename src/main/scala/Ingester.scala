import org.apache.spark.sql.{DataFrame, SparkSession}

class Ingester(spark: SparkSession) {

  def ingestFromCSV (pathCSV: String): DataFrame ={
    spark.read.option("header", true).option("inferSchema", true).csv(pathCSV)
  }
}
