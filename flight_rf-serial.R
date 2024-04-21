#load all the libraries
library(arrow)
library(dplyr)
library(tidyr)
library(MERO)
library(randomForest)
library(parsnip)
library(ranger)
library(parallel)

### DATA LOADING 

#load in the dataset
ds <- open_dataset("C:\\Users\\liana\\OneDrive\\Desktop\\BZAN_583_data\\TEST_itineraries", partitioning = c("flightDate"), unify_schemas = TRUE) 

#clean, format, and prepare data
data <- ds %>%
  collect() %>%
  filter(isNonStop == "True") %>%
  mutate(segmentsArrivalTimeRaw =as.POSIXct(segmentsArrivalTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  mutate(segmentsDepartureTimeRaw=as.POSIXct(segmentsDepartureTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  select(-c("segmentsDepartureTimeEpochSeconds",
            "segmentsArrivalTimeEpochSeconds",
            "segmentsAirlineCode",
            "legId",
            "travelDuration",
            "fareBasisCode")) %>%
  drop_na() %>%
  collect() 


## PREPARE DATA FOR TRAINING AND TESTING
n <- nrow(data) #counts how many rows there are in the dataset

n_test <- floor(0.2 * n) #gets the number of 20% of the dataset's rows rounded down

i_test <- sample.int(n, n_test) #picks random integers that will be the row indices; chooses n_test rows out of n total rows

train <- data[-i_test, ] #creates the training dataset by excluding the randomly chosen test indices from i_test

test <- data[i_test, ] #creates the training dataset by including the randomly chosen test indices from i_test



## TRAIN THE RANDOM FOREST MODEL
rf.all = randomForest(totalFare ~ ., #chooses the column being predicted
                      train, #selects the training set to train on
                      ntree = 500, #builds 500 trees
                      norm.votes = FALSE) #disables normalizing of votes among trees

## PREDICT FOR TEST SET WITH TRAINED RANDOM FOREST MODEL
pred = predict(rf.all, test) #uses the trained random forest model to predict for the test set


## CALCULATE THE ACCURACY
correct = sum(pred == test$totalFare) #counts how many predictions match what it actually is in the dataset

cat("Proportion Correct:", correct/(n_test), "\n") #prints the amount of data correct

cat("RMSE:",RMSE(test$totalFare,pred), "\n") #prints the RMSE