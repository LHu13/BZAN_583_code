
set.seed(123) #for reproducibility

## LOAD NECESSARY PACKAGES
#load all the libraries
library(arrow)
library(dplyr)
library(tidyr)




cat("Starting partitioned data load.\n")
start_part_time <- Sys.time()


## DATA LOADING
#Load in the dataset
part_ds <- open_dataset("/projects/bckj/Team3/flight_data_parquet/itineraries", 
                   partitioning = c("flightDate"), 
                   unify_schemas = TRUE) 

#Clean, format, and prepare data
part_data <- part_ds %>%
  filter(isNonStop == "True") %>% #remove all the not nonstop flights
  #drop unnecessary columns
  select(-c("segmentsDepartureTimeEpochSeconds", #repeat info
            "segmentsArrivalTimeEpochSeconds", #repeat info
            "segmentsAirlineCode", #repeat info
            "legId", #unnecessary
            "travelDuration", #repeat info
            "fareBasisCode")) %>% #unnecessary
  collect()



mid_part_time <- Sys.time()
cat("Finished partitioned data first clean.", 
    round(start_part_time-mid_part_time),"\n")



part_data <- part_data %>%
  #convert time columns into datetime format
  mutate(segmentsArrivalTimeRaw =as.POSIXct(segmentsArrivalTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  mutate(segmentsDepartureTimeRaw=as.POSIXct(segmentsDepartureTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  #DROPS ALL OTHER MONTHS BESIDES MAY BC DATA FUNKY
  filter(as.integer(format(segmentsArrivalTimeRaw, "%m")) %in% c(5)) %>%
  #transforms number columns from character to numeric
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  
  drop_na() #drops the nas


end_part_time <- Sys.time()
cat("Partitioned data total loading and cleaning time ", 
    round(start_part_time-end_part_time,2),"\n")




cat("Starting whole data load.\n")
start_whole_time <- Sys.time()


## DATA LOADING
#Load in the dataset
ds <- open_dataset("/projects/bckj/Team3/itineraries_nopart") 

#Clean, format, and prepare data
data <- ds %>%
  filter(isNonStop == "True") %>% #remove all the not nonstop flights
  #drop unnecessary columns
  select(-c("segmentsDepartureTimeEpochSeconds", #repeat info
            "segmentsArrivalTimeEpochSeconds", #repeat info
            "segmentsAirlineCode", #repeat info
            "legId", #unnecessary
            "travelDuration", #repeat info
            "fareBasisCode")) %>% #unnecessary
  collect()



mid_whole_time <- Sys.time()
cat("Finished whole data first clean.", 
    round(start_whole_time-mid_whole_time),"\n")


data <- data %>%
  #convert time columns into datetime format
  mutate(segmentsArrivalTimeRaw =as.POSIXct(segmentsArrivalTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  mutate(segmentsDepartureTimeRaw=as.POSIXct(segmentsDepartureTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  #DROPS ALL OTHER MONTHS BESIDES MAY BC DATA FUNKY
  filter(as.integer(format(segmentsArrivalTimeRaw, "%m")) %in% c(5)) %>%
  #transforms number columns from character to numeric
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  
  drop_na() #drops the nas


end_whole_time <- Sys.time()

cat("Whole data total loading and cleaning time ", 
    round(start_whole_time-end_whole_time,2),"\n")