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
 |-- Year: string (nullable = true)
 |-- Datetime: string (nullable = true)
 |-- Stage: string (nullable = true)
 |-- Stadium: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Home_Team_Name: string (nullable = true)
 |-- Home_Team_Goals: long (nullable = true)
 |-- Away_Team_Goals: long (nullable = true)
 |-- Away_Team_Name: string (nullable = true)
 |-- Win conditions: string (nullable = true)
 |-- Attendance: string (nullable = true)
 |-- Half-time Home Goals: string (nullable = true)
 |-- Half-time Away Goals: string (nullable = true)
 |-- Referee: string (nullable = true)
 |-- Assistant 1: string (nullable = true)
 |-- Assistant 2: string (nullable = true)
 |-- RoundID: string (nullable = true)
 |-- MatchID: string (nullable = true)
 |-- Home Team Initials: string (nullable = true)
 |-- Away Team Initials: string (nullable = true)

### Technologies
---
#### Spark 3.0.3
#### Dataframes
#### SQL
#### Spark SQL Api

### Reading Dataset
---

### Analysis
---
#### Most Winning Teams
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (3)](https://user-images.githubusercontent.com/69567496/186506430-d9e65aa8-0721-4c01-abdd-3804c772d97d.png)
#### Most Losing Teams
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (4)](https://user-images.githubusercontent.com/69567496/186506903-713dcdd9-71ac-41c2-90fa-ca3c9712e92a.png)
#### Most Scoring Teams
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (6)](https://user-images.githubusercontent.com/69567496/186547345-ba69f60d-10ec-4d88-884f-b4fa41fd464c.png)
#### Most Teams with clean sheets
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (7)](https://user-images.githubusercontent.com/69567496/186547798-0a928286-9c4a-459a-9618-e9b445ae02d8.png)
#### Most Teams facing each other
![186499868-bce78c02-fa4e-43b1-ac96-5c6dc5f8d747 (5)](https://user-images.githubusercontent.com/69567496/186513688-99eae8e5-8f0f-4257-ae80-2d113d7b75be.png)
