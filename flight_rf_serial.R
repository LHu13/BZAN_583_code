
set.seed(123) #for reproducibility

## LOAD NECESSARY PACKAGES
#load all the libraries
library(arrow)
library(dplyr)
library(tidyr)
library(MERO)
library(randomForest)
library(parallel)


### DATA LOADING 
#Load in the dataset
ds <- open_dataset("/projects/bckj/Team3/flight_data_parquet/itineraries", 
                   partitioning = c("flightDate"), 
                   unify_schemas = TRUE) 

#Clean, format, and prepare data
data <- ds %>%
  collect() %>%
  filter(isNonStop == "True") %>% #remove all the not nonstop flights
  #convert time columns into datetime format
  mutate(segmentsArrivalTimeRaw =as.POSIXct(segmentsArrivalTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  mutate(segmentsDepartureTimeRaw=as.POSIXct(segmentsDepartureTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  #DROPS ALL OTHER MONTHS BESIDES MAY BC DATA FUNKY
  filter(as.integer(format(segmentsArrivalTimeRaw, "%m")) %in% c(5)) %>%
  filter(as.integer(format(segmentsArrivalTimeRaw, "%d")) %in% c(1)) %>%
  #transforms number columns from character to numeric
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  #drop unnecessary columns
  select(-c("segmentsDepartureTimeEpochSeconds", #repeat info
            "segmentsArrivalTimeEpochSeconds", #repeat info
            "segmentsAirlineCode", #repeat info
            "legId", #unnecessary
            "travelDuration", #repeat info
            "fareBasisCode")) %>% #unnecessary
  drop_na() %>% #drops the nas
  collect() 


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


## TRAIN THE RANDOM FOREST MODEL
#Train random forest model and call it rf.all
rf.all <- randomForest(totalFare ~ ., #chooses the column being predicted
                      train, #selects the training set to train on
                      ntree = 500, #builds 500 trees
                      norm.votes = FALSE) #disables normalizing of votes among trees


## PREDICT FOR TEST SET WITH TRAINED RANDOM FOREST MODEL
#Uses the trained random forest model to predict for the test set
pred <- predict(rf.all, test) 


## CALCULATE THE ACCURACY
#Counts how many predictions match what it actually is in the dataset
correct <- sum(pred == test$totalFare) 
#Prints the amount of data correct
cat("Proportion Correct:", correct/(n_test), "\n") 
#Prints the RMSE
cat("RMSE:",RMSE(test$totalFare,pred), "\n") 

