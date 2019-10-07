import org.apache.spark.sql.{DataFrame, SparkSession}

class Cleaner(spark: SparkSession) {

  def cleanData (titanicDF: DataFrame): DataFrame ={
    import spark.implicits._
    titanicDF.filter($"Age".isNotNull)
  }
}
