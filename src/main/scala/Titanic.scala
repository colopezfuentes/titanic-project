import org.apache.spark.sql.SparkSession

object Titanic {
  def main(args: Array[String]): Unit = {
    var pathCSV = "./titanic.csv"
    var outputPathCSV = "./survivors"
    var outputPathPriorityCSV = "./priority"

    if(args.length > 0){
      pathCSV = args(0);
      outputPathCSV = args(1)
      outputPathPriorityCSV = args(2)
    }

    val spark = SparkSession.builder.appName("Titanic").config("spark.master", "local").getOrCreate()

    /**
     * Ingester, read CSV
     */
    val ingester = new Ingester(spark)
    val titanicDF = ingester.ingestFromCSV(pathCSV)

    //Cleaner
    val cleaner = new Cleaner(spark)
    val cleanTitanicDF = cleaner.cleanData(titanicDF)

    /**
     *  Transformer
     */

    val transformer = new Transformer(spark)
    val transformerTitanicDF = transformer.transformTitanicDF(cleanTitanicDF)


    /**
     * Generate Reports
     */
    val writter = new Writter(spark)

    //Priority
    val prioritizedDF = transformer.assignPriorityByGender(transformerTitanicDF)
    writter.generateCSV(prioritizedDF, outputPathPriorityCSV)

    // Survivors
    val reportDF = transformer.joinSurvivorsDFAndNotSurvivorsDF(transformerTitanicDF)
    writter.generateCSV(reportDF, outputPathCSV)

  }

}
