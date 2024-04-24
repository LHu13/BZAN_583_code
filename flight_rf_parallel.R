
set.seed(123) #for reproducibility

## LOAD NECESSARY PACKAGES
#load all the libraries
library(arrow)
library(dplyr)
library(tidyr)
library(MERO)
library(randomForest)
library(parallel)

#cat("Parallel starting data load.\n")

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

#cat("Finished first parallel data clean.", format(Sys.time(), "%H:%M:%S"),"\n")

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


#cat("Parallel data preparation done", format(Sys.time(), "%H:%M:%S"),"\n")

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

#cat("Parallel data split done", format(Sys.time(), "%H:%M:%S"),"\n")


#TIME IT
start_time <- Sys.time()

## SET UP FOR PARALLEL
#commandArgs(TRUE)[2] - gets the second command-line argument as a numeric value
#Determines the number of CPU cores for parallel processing
nc <- as.numeric(commandArgs(TRUE)[2])

#cat("Number of cores:", nc)

#Determines number of trees, splits 500 trees on available CPU cores 
ntree <- lapply(splitIndices(500, nc), length) 


## TRAIN RANDOM FOREST MODEL IN PARALLEL
#Define function to train random forest model
rf <- function(x, train) randomForest(totalFare ~ ., train, ntree=x, norm.votes = FALSE)
#Applies the random forest training function in parallel on the number of trees
rf.parts <- mclapply(ntree, #number of trees to be built on a CPU core
                     rf, #uses the random forest training function rf
                     train = train, #chooses the training data
                     mc.cores = nc) #chooses number of CPU cores to be used
#Combines the results of the parallel trained random forest models into rf.all
rf.all <- do.call(combine, rf.parts) 

#cat("Parallel random forest training done", format(Sys.time(), "%H:%M:%S"),"\n")

## PREDICT ON TEST DATA
#Splits the test data into chunks based on number of CPU cores
crows <- splitIndices(nrow(test), nc) 
#Define function to predict on test data
rfp <- function(x) as.vector(predict(rf.all, test[x, ]))
#Applies the prediction function in parallel to the chunks of test data
cpred <- mclapply(crows, #data selection
                  rfp, #prediction function selection
                  mc.cores = nc)  #chooses number of CPU cores
#Combines the predictions from the chunks of data into one vector
pred <- do.call(c, cpred)                            

#cat("Parallel predictions done", format(Sys.time(), "%H:%M:%S"),"\n")

## CALCULATE THE ACCURACY
#Gets the RMSE
rf_rmse <- RMSE(test$totalFare,pred)

#TIME IT
end_time <- Sys.time()

cat( nc, " cores parallel random forest model done \n")
#cat("Model RMSE:", rf_rmse, "\n")
cat("Time Taken:", round(end_time-start_time,2),"\n")
