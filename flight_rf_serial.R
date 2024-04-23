
set.seed(123) #for reproducibility

## LOAD NECESSARY PACKAGES
#load all the libraries
library(arrow)
library(dplyr)
library(tidyr)
library(MERO)
library(randomForest)
library(parallel)

#cat("Serial starting data load.\n")

## DATA LOADING
#Load in the dataset
ds <- open_dataset("/projects/bckj/Team3/flight_data_parquet/itineraries", 
                   partitioning = c("flightDate"), 
                   unify_schemas = TRUE) 

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


#cat("Finished first serial data clean.", format(Sys.time(), "%H:%M:%S"),"\n")


data <- data %>%
  #convert time columns into datetime format
  mutate(segmentsArrivalTimeRaw =as.POSIXct(segmentsArrivalTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  mutate(segmentsDepartureTimeRaw=as.POSIXct(segmentsDepartureTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  #DROPS ALL OTHER MONTHS BESIDES MAY BC DATA FUNKY
  filter(as.integer(format(segmentsArrivalTimeRaw, "%m")) %in% c(5)) %>%
  filter(as.integer(format(segmentsArrivalTimeRaw, "%d")) %in% c(1)) %>%
  #transforms number columns from character to numeric
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  
  drop_na() #drops the nas


#cat("Serial data prep done", format(Sys.time(), "%H:%M:%S"),"\n")


## PREPARE DATA FOR TRAINING AND TESTING
#Counts how many rows there are in the dataset
n <- nrow(data) 
#Gets the number of 20% of the dataset's rows rounded down
n_test <- floor(0.2 * n) 
#Picks random integers that will be the row indices; chooses n_test rows out of n total rows
i_test <- sample.int(n, n_test) 
#Creates the training dataset by excluding the randomly chosen test indices from i_test
train <- data[-i_test, ] 
#Creates the training dataset by including the randomly chosen test indices from i_test
test <- data[i_test, ]


#cat("Serial data split done",format(Sys.time(), "%H:%M:%S"),"\n")


#TIME IT
start_time <- Sys.time()


## TRAIN THE RANDOM FOREST MODEL
#Train random forest model and call it rf.all
rf.all <- randomForest(totalFare ~ ., #chooses the column being predicted
                      train, #selects the training set to train on
                      ntree = 500, #builds 500 trees
                      norm.votes = FALSE) #disables normalizing of votes among trees

#cat("Serial random forest training done", format(Sys.time(), "%H:%M:%S"),"\n")

## PREDICT FOR TEST SET WITH TRAINED RANDOM FOREST MODEL
#Uses the trained random forest model to predict for the test set
pred <- predict(rf.all, test) 

#cat("Serial predictions done", format(Sys.time(), "%H:%M:%S"),"\n")

## CALCULATE THE ACCURACY
#Gets the RMSE
rf_rmse <- RMSE(test$totalFare,pred)

#TIME IT
end_time <- Sys.time()

print("Serial random forest model done.")
cat("Model RMSE:", rf_rmse, "\n")
cat("Time Taken:", round(end_time-start_time,2),"\n")
