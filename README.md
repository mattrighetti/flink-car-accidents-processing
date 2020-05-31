# Processing of car accidents data

#### The goal of this project is to infer qualitative data regarding the car accidents in New York City.


Extract the following information:
- Number of lethal accidents per week throughout the entire dataset.
- Number of accidents and percentage of number of deaths per contributing factor in the dataset.
  - I.e., for each contributing factor, we want to know how many accidents were due to
      that contributing factor and what percentage of these accidents were also lethal.
- Number of accidents and average number of lethal accidents per week per borough.
  - I.e., for each borough, we want to know how many accidents there were in that borough each week, as well as the average number of lethal accidents that the borough had per week.
  
## How to run this program

1. Fire up Flink
2. run `flink run <generated_jar> --nypd_data_file <path_to_NYPD_csv>`