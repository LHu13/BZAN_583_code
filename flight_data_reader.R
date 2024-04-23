set.seed(123) #for reproducibility

## LOAD NECESSARY PACKAGES
#load all the libraries
library(arrow)
library(dplyr)
library(tidyr)
library(parallel)

#TIME IT
start_time <- Sys.time()

print("Parallel starting data load.")

## DATA LOADING
#Load in the dataset
ds <- open_dataset("/projects/bckj/Team3/flight_data_parquet/itineraries", 
                   partitioning = c("flightDate"), 
                   unify_schemas = TRUE) 


read_data <- function(month) {
  
  data <- ds %>% 
    #keeps only the rows with flightDate that match the month selected
    filter(as.integer(substr(data$flightDate, 6, 7) == month)) %>%
    filter(isNonStop == "True") %>% #remove all the not nonstop flights
    #drop unnecessary columns
    select(-c("segmentsDepartureTimeEpochSeconds", #repeat info
              "segmentsArrivalTimeEpochSeconds", #repeat info
              "segmentsAirlineCode", #repeat info
              "legId", #unnecessary
              "travelDuration", #repeat info
              "fareBasisCode")) %>% #unnecessary
    collect()
    
  return(data)
}

#commandArgs(TRUE)[2] - gets the second command-line argument as a numeric value
#Determines the number of CPU cores for parallel processing
nc <- as.numeric(commandArgs(TRUE)[2])

rf.parts <- mclapply(ntree, #number of trees to be built on a CPU core
                     rf, #uses the random forest training function rf
                     train = train, #chooses the training data
                     mc.cores = nc) #chooses number of CPU cores to be used


flight_monthly <- mclapply( c(4,5,6,7,8,9,10,11),
                            read_data,
                            ds,
                            mc.cores = nc)


end_time <- Sys.time()

cat("Time taken:", end_time-start_time)
