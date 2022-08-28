package com
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.LongType
object WorldCup {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.option("header",true).csv("C:\\Users\\ndarwish\\Desktop\\WorldCupMatches.csv")

    val franceHome = df.filter((col("Home_Team_Goals")-col("Away_Team_Goals")>0) ).groupBy(col("Home_Team_Name").as("Team_Name")).agg(count("*").as("Wins"))

    val franceAway = df.filter(col("Away_Team_Goals")-col("Home_Team_Goals")>0 ).groupBy(col("Away_Team_Name").as("Team_Name")).agg(count("*").as("Wins"))

    val franceDup = franceHome.union(franceAway)

    val france = franceDup.groupBy(col("Team_Name")).agg(sum("Wins").as("Wins")).orderBy(col("Wins").desc)

    france.show()

    val df2 = df.withColumn("Home_Team_Goals",col("Home_Team_Goals").cast(LongType))
      .withColumn("Away_Team_Goals",col("Away_Team_Goals").cast(LongType))

    df2.printSchema()

    df2.createOrReplaceTempView("matches")

    val HomeAwayWins = spark.sql("(SELECT Home_Team_Name as Team_Name,COUNT(*) as Wins FROM matches WHERE Home_Team_Goals - Away_Team_Goals > 0 GROUP BY Home_Team_Name)UNION( SELECT Away_Team_Name as Team_Name,COUNT(*) as Wins FROM matches WHERE Home_Team_Goals - Away_Team_Goals < 0 GROUP BY Away_Team_Name)")

    HomeAwayWins.createOrReplaceTempView("winners")

    val TotalWins= spark.sql("SELECT Team_Name,SUM(Wins) as Total_Wins FROM winners GROUP BY Team_Name HAVING Total_Wins = (SELECT MAX(Total_wins) FROM(SELECT Team_Name,SUM(Wins) as Total_Wins FROM winners GROUP BY Team_Name))")

    TotalWins.printSchema()

    TotalWins.show()

    //--------------------------------------------------------------------------------------------------------------------------------------------------------------//

    val losesHome = df.filter((col("Home_Team_Goals")-col("Away_Team_Goals")<0) ).groupBy(col("Home_Team_Name").as("Team_Name")).agg(count("*").as("loses"))

    val losesAway = df.filter(col("Away_Team_Goals")-col("Home_Team_Goals")<0 ).groupBy(col("Away_Team_Name").as("Team_Name")).agg(count("*").as("loses"))

    val losesDup = losesHome.union(losesAway)

    val loses = losesDup.groupBy(col("Team_Name")).agg(sum("loses").as("loses")).orderBy(col("loses").desc)

    loses.show()

    //--------------------------------------------------------------------------------------------------------------------------------------------------------------//

    val dueling = df.groupBy("Home_Team_Name","Away_Team_Name").agg(count("*").as("Versus")).orderBy(col("Versus").desc)

    dueling.show()

    //--------------------------------------------------------------------------------------------------------------------------------------------------------------//


    val goalsHome = df.groupBy(col("Home_Team_Name").as("Team_Name")).agg(sum("Home_Team_Goals").as("Goals"))

    val goalsAway = df.groupBy(col("Away_Team_Name").as("Team_Name")).agg(sum("Away_Team_Goals").as("Goals"))

    val goalsDup = goalsHome.union(goalsAway)

    val goals = goalsDup.groupBy(col("Team_Name")).agg(sum("Goals").as("Goals")).orderBy(col("Goals").desc)

    goals.show()

    //--------------------------------------------------------------------------------------------------------------------------------------------------------------//

    val cleanHome = df.filter((col("Away_Team_Goals")===0) ).groupBy(col("Home_Team_Name").as("Team_Name")).agg(count("*").as("CleanSheets"))

    val cleanAway = df.filter((col("Home_Team_Goals")===0) ).groupBy(col("Away_Team_Name").as("Team_Name")).agg(count("*").as("CleanSheets"))

    val cleanDup = cleanHome.union(cleanAway)

    val clean = cleanDup.groupBy(col("Team_Name")).agg(sum("CleanSheets").as("CleanSheets")).orderBy(col("CleanSheets").desc)

    clean.show()

  }

}
