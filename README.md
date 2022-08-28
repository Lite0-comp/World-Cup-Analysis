![⚽World_Cup_Dataset_⚽ (2)](https://user-images.githubusercontent.com/69567496/185922257-d6621c85-2cda-4188-a93d-0d0236a75224.png)

### Table of Contents 
---

- [Introduction](#introduction)
- [Dataset](#dataset-structure)
- [Technologies](#technologies)
- [Reading Dataset](#reading-dataset)
- [Analysis](#analysis)


### Introduction
---
#### This project main scope is to analyse the performance of the teams in World Cup through history throughout the wins of each teams and the goals scored and conceded in each match.
#### Spark Apache is used to create this project which is setted to run on the local device otherwise the project could run on other nodes (Cluster).

### Dataset Structure
---
#### -[World Cup Matches](https://www.kaggle.com/datasets/abecklas/fifa-world-cup) Dataset is from Kaggle Open-Source.
#### -Dataset constructed in the following schema:

   | Column name             |                Description                                         |
   |:-----------------------:|:------------------------------------------------------------------:|
   |  Year                   | year of the world cup                                              |
   |  Datetime               | date and time of the match                                         |
   |  Stage                  | which stage of the competition (Group,Round of 16,Quarter...)      |
   |  Stadium                | place where the match played                                       |
   |  Home_Team_Name         | team that the match played on his stadium                          |
   |  Home_Team_Goals        | goals scored by Home team                                          |
   |  Away_Team_Goals        | goals scored by Away team                                          |                                       
   |  Away_Team_Name         | team that the match was not played on his stadium                  |
   |  Win conditions         | the time the match ended in (Default 90 mins, extratime,penalities |
   |  Attendance             | how many people attended the match at the stadium                  |
   |  Half-time Home Goals   | goals scored by the home team in the first leg                     |
   |  Half-time Away Goals   | goals scored by the away team in the first leg                     |
   |  Referee                | name of the referee who managed the match                          |
   |  Assistant 1            | name of the main assistant of the referee                          |
   |  Assistant 2            | name of the secondary assistant of the referee                     |
   |  RoundID                | Id of the round that the match was played in                       |
   |  MatchID                | Id of the match                                                    |
   |  Home Team Initials     | the appreviation of the home team                                  |
   |  Away Team Initials     | the appreviation of the away team                                  |
   
   
### Technologies
---
#### Scala 2.11.12
#### sbt 1.3.13
#### JDK 1.8.0
#### SQL
#### Spark SQL Api
#### sbt 1.3.13

### Reading Dataset
---
```
val df = spark.read.option("header",true).csv("C:\\Users\\ndarwish\\Desktop\\WorldCupMatches.csv")
val df2 = df.withColumn("Home_Team_Goals",col("Home_Team_Goals").cast(LongType))
            .withColumn("Away_Team_Goals",col("Away_Team_Goals").cast(LongType))
df2.createOrReplaceTempView("matches")
```
### Analysis
---
#### Most Winning Teams
- Spark Api method
```
val franceHome =  df2.filter((col("Home_Team_Goals")-col("Away_Team_Goals")>0) )
                     .groupBy(col("Home_Team_Name").as("Team_Name"))
                     .agg(count("*").as("Wins"))

val franceAway = df2.filter(col("Away_Team_Goals")-col("Home_Team_Goals")>0 )
                    .groupBy(col("Away_Team_Name").as("Team_Name"))
                    .agg(count("*").as("Wins"))

val franceDup = franceHome.union(franceAway)

val france = franceDup.groupBy(col("Team_Name"))
                      .agg(sum("Wins").as("Wins"))
                      .orderBy(col("Wins").desc)

```
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (3)](https://user-images.githubusercontent.com/69567496/186506430-d9e65aa8-0721-4c01-abdd-3804c772d97d.png)
#### Most Losing Teams
- Spark Api method
```
val losesHome = df.filter((col("Home_Team_Goals")-col("Away_Team_Goals")<0) )
                  .groupBy(col("Home_Team_Name").as("Team_Name"))
                  .agg(count("*").as("loses"))

val losesAway = df.filter(col("Away_Team_Goals")-col("Home_Team_Goals")<0 )
                  .groupBy(col("Away_Team_Name").as("Team_Name")).agg(count("*").as("loses"))

val losesDup = losesHome.union(losesAway)

val loses = losesDup.groupBy(col("Team_Name"))
                    .agg(sum("loses").as("loses"))
                    .orderBy(col("loses").desc)
```
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (4)](https://user-images.githubusercontent.com/69567496/186506903-713dcdd9-71ac-41c2-90fa-ca3c9712e92a.png)
#### Most Scoring Teams
- Spark Api method
```
val goalsHome = df.groupBy(col("Home_Team_Name").as("Team_Name"))
                  .agg(sum("Home_Team_Goals").as("Goals"))

val goalsAway = df.groupBy(col("Away_Team_Name").as("Team_Name"))
                  .agg(sum("Away_Team_Goals").as("Goals"))

val goalsDup = goalsHome.union(goalsAway)

val goals = goalsDup.groupBy(col("Team_Name"))
                    .agg(sum("Goals").as("Goals"))
                    .orderBy(col("Goals").desc)

```

![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (6)](https://user-images.githubusercontent.com/69567496/186547345-ba69f60d-10ec-4d88-884f-b4fa41fd464c.png)
#### Most Teams with clean sheets
- Spark Api method
```
val cleanHome = df.filter((col("Away_Team_Goals")===0) )
                  .groupBy(col("Home_Team_Name").as("Team_Name"))
                  .agg(count("*").as("CleanSheets"))

val cleanAway = df.filter((col("Home_Team_Goals")===0) )
                  .groupBy(col("Away_Team_Name").as("Team_Name"))
                  .agg(count("*").as("CleanSheets"))

val cleanDup = cleanHome.union(cleanAway)

val clean = cleanDup.groupBy(col("Team_Name"))
                    .agg(sum("CleanSheets").as("CleanSheets"))
                    .orderBy(col("CleanSheets").desc)

```
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (7)](https://user-images.githubusercontent.com/69567496/186547798-0a928286-9c4a-459a-9618-e9b445ae02d8.png)
#### Most Teams facing each other
- Spark Api method
```
val dueling = df.groupBy("Home_Team_Name","Away_Team_Name")
                .agg(count("*").as("Versus"))
                .orderBy(col("Versus").desc)
```
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (5)](https://user-images.githubusercontent.com/69567496/186513688-99eae8e5-8f0f-4257-ae80-2d113d7b75be.png)
