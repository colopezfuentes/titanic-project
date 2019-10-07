import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Transformer(spark:SparkSession) {
  val statusAlive = 1
  val statusNotAlive = 0


  def transformTitanicDF (cleanTitanicDF:DataFrame): DataFrame ={
    import spark.implicits._

        cleanTitanicDF
          .withColumn("portNameEmbarkation",
            when($"embarked"=== "C","Cherbourg")
            .when($"embarked"=== "Q","Queenstown")
            .when($"embarked" === "S", "Southampton"))

          .withColumn("ClassOfTicket",
            when($"Pclass" === 1, "1st")
              .when($"Pclass" === 2, "2nd")
              .when($"Pclass" === 3, "3rd"))

  }

  def assignPriorityByGender(transformTitanicDF: DataFrame): DataFrame ={
    import spark.implicits._
  transformTitanicDF
    .withColumn("Priority",
      when($"Age" <= 18, "p1")
        .when($"Age" > 18 && $"Sex" === "female","p2").otherwise("p3")
    )}

  def filterColumnsFromTransformTitanicDF(transformTitanicDF:DataFrame): DataFrame ={
    import spark.implicits._
    transformTitanicDF.select( $"Name", $"Sex", $"Age",$"Survived", $"ClassOfTicket" , $"portNameEmbarkation")
  }
  def getPeopleBySurvivorState(filterColumnsDF:DataFrame, survivorState:Int): DataFrame ={
    import spark.implicits._
    filterColumnsDF.filter($"Survived" === survivorState)
  }

  def getPeopleGroupedByClassOfTicket(filterColumnsDF:DataFrame, survivorState:Int, nameColumn:String): DataFrame ={
    import spark.implicits._
    val notSurvivorsDF = getPeopleBySurvivorState(filterColumnsDF, survivorState)

    notSurvivorsDF
      .select($"Survived", $"ClassOfTicket")
      .groupBy($"ClassOfTicket")
      .agg(count($"Survived") as nameColumn)
  }

  def joinSurvivorsDFAndNotSurvivorsDF(transformerTitanicDF:DataFrame): DataFrame ={
    val filterDF = filterColumnsFromTransformTitanicDF(transformerTitanicDF)
    val survivorsDF = getPeopleGroupedByClassOfTicket(filterDF, statusAlive,"NumberOfSurvivors" )
    val notSurvivorsDF = getPeopleGroupedByClassOfTicket(filterDF, statusNotAlive, "NumberOfNotSurvivors")
    survivorsDF.join(notSurvivorsDF, "ClassOfTicket")
  }


}
