# Car accidents data processing in Apache Flink

<p align="center">
    <img src="flink.png" height="200px">
    <img src="nypd.png" height="200px">
</p>

#### The goal of this project is to infer qualitative data regarding the car accidents in New York City.

## CSV structure

|Field Name|Type|
|:-|:-:|
|`DATE`|`String`|
|`TIME`|`String`|
|`BOROUGH`|`String`|
|`ZIP CODE`|`Integer`|
|`LATITUDE`|`Float`|
|`LONGITUDE`|`Float`|
|`LOCATION`|`String`|
|`ON STREET NAME`|`String`|
|`CROSS STREET NAME`|`String`|
|`OFF STREET NAME`|`String`|
|`NUMBER OF PERSONS INJURED`|`Integer`|
|`NUMBER OF PERSONS KILLED`|`Integer`|
|`NUMBER OF PEDESTRIANS INJURED`|`Integer`|
|`NUMBER OF PEDESTRIANS KILLED`|`Integer`|
|`NUMBER OF CYCLIST INJURED`|`Integer`|
|`NUMBER OF CYCLIST KILLED`|`Integer`|
|`NUMBER OF MOTORIST INJURED`|`Integer`|
|`NUMBER OF MOTORIST KILLED`|`Integer`|
|`CONTRIBUTING FACTOR VEHICLE 1`|`String`|
|`CONTRIBUTING FACTOR VEHICLE 2`|`String`|
|`CONTRIBUTING FACTOR VEHICLE 3`|`String`|
|`CONTRIBUTING FACTOR VEHICLE 4`|`String`|
|`CONTRIBUTING FACTOR VEHICLE 5`|`String`|
|`UNIQUE KEY`|`Long`|
|`VEHICLE TYPE CODE 1`|`String`|
|`VEHICLE TYPE CODE 2`|`String`|
|`VEHICLE TYPE CODE 3`|`String`|
|`VEHICLE TYPE CODE 4`|`String`|
|`VEHICLE TYPE CODE 5`|`String`|

Extract the following information:
- Number of lethal accidents per week throughout the entire dataset.
- Number of accidents and percentage of number of deaths per contributing factor in the dataset.
  - I.e.|
  for each contributing factor|
  we want to know how many accidents were due to
      that contributing factor and what percentage of these accidents were also lethal.
- Number of accidents and average number of lethal accidents per week per borough.
  - I.e.|
  for each borough|
  we want to know how many accidents there were in that borough each week|
  as well as the average number of lethal accidents that the borough had per week.

## How to compile this program
By executing these commands a **target** folder will be generated in the project's directory that's going to contained the generated **.jar** executables
1. `mvn clean install`
2. `mvn package`
  
## How to run this program

1. Fire up Flink
2. Run `flink run <jar> --nypd_data_file <path_to_NYPD_csv>`