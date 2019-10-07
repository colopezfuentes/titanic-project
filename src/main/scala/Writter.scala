import org.apache.spark.sql.{DataFrame, SparkSession}

class Writter(spark:SparkSession) {

  def generateCSV(dataFrame: DataFrame, outputPath: String): Unit ={
    dataFrame.write
      .option("header", true)
      .csv(outputPath)
  }

}
