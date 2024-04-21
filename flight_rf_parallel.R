
set.seed(123) #for reproducibility

## LOAD NECESSARY PACKAGES
#Define function to check if packages are installed and install them if not
package_checker <- function(packages) {
  #Check if packages are installed
  missing_packages <- setdiff(packages, installed.packages()[,"Package"])
  #Install missing packages
  if (length(missing_packages) > 0) {
    install.packages(missing_packages, dependencies = TRUE)
  }
  #Load all packages
  lapply(packages, function(pkg) {
    library(pkg, character.only = TRUE, logical.return = TRUE)
  })
}
#List of packages needed
packages <- c("arrow", "dplyr", "tidyr","MERO","randomForest","parallel")
#Use package_checker function to load necessary libraries
package_checker(packages)


## DATA LOADING
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


## SET UP FOR PARALLEL
#commandArgs(TRUE)[2] - gets the second command-line argument as a numeric value
#Determines the number of CPU cores for parallel processing
nc <- as.numeric(commandArgs(TRUE)[2])
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


## CALCULATE THE ACCURACY
#Counts how many predictions match what it actually is in the dataset
correct <- sum(pred == test$totalFare) 
#Prints the amount of data correct
cat("Proportion Correct:", correct/(n_test), "\n") 
#Prints the RMSE
cat("RMSE:",RMSE(test$totalFare,pred), "\n") 