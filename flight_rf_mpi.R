# Load relevant libraries and suppress loading messages.
suppressMessages(library(pbdMPI))
suppressMessages(library(arrow))
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(randomForest))
suppressMessages(library(parallel))
suppressMessages(library(lubridate))
# Set seed for reproducibility
comm.set.seed(seed = 7654321, diff = FALSE) 

SAMPLE_SIZE <- 10000000

# TIME IT
#start_time <- Sys.time()


################################ DATA LOADING ###################################
# Load in data from server.
ds <- open_dataset("/projects/bckj/Team3/flight_data_parquet/itineraries")
# Create function to extract partition information from the file paths in the dataset
get_hive_var = function(ds, var) # select dataset and partitioning variable
  sub("/.*$", "", sub(paste0("^.*", var, "="), "", ds$files)) # regex to manipulate strings
# Extracts the partition variable values from the dataset
partitions = get_hive_var(ds, "flightDate")  
# Distributes the partitions evenly across different processors
my_partitions = partitions[comm.chunk(length(partitions), #comm.chunk splits the data so each processor works on a different piece of dataset
                                      form = "vector")]
# Print out the MPI rank (i.e. identifier for each processor) and the partitions assigned to each rank
#comm.cat("rank", 
#         comm.rank(), 
#         "partitions", 
#         my_partitions, # selects the distributed partitions
#         "\n", 
#         all.rank = TRUE) # ensures the output is generated from all processors


################################ DATA CLEANING ###################################
# Read only the data for the partitions in my_partitions
my_data <- ds %>% 
  filter(flightDate %in% my_partitions) %>%
  filter(isNonStop == "True") %>% #remove all the not nonstop flights
  #drop unnecessary columns
  select(-c("segmentsDepartureTimeEpochSeconds", #repeat info
            "segmentsArrivalTimeEpochSeconds", #repeat info
            "segmentsAirlineCode", #repeat info
            "legId", #unnecessary
            "travelDuration", #repeat info
            "segmentsArrivalAirportCode",
            "segmentsDepartureAirportCode",
            "fareBasisCode",
            "baseFare",
            "searchDate",
            "flightDate",
            "elapsedDays",
            "isNonStop")) %>% #unnecessary
  collect()

# Filter, select, mutate, and reduce the data (do separately for debug)
my_data <- my_data %>%
  #convert time columns into datetime format
  mutate(segmentsArrivalTimeRaw = ymd_hms(segmentsArrivalTimeRaw))  %>%
  mutate(segmentsDepartureTimeRaw= ymd_hms(segmentsDepartureTimeRaw))  %>%
  #DROPS ALL OTHER MONTHS BESIDES MAY BC DATA FUNKY
  #filter(month(segmentsDepartureTimeRaw) == 5) %>%
  #keep only the hours, minutes, date
#  mutate(minuteArrivalTimeRaw = minute(segmentsArrivalTimeRaw))  %>%
#  mutate(minuteDepartureTimeRaw= minute(segmentsDepartureTimeRaw)) %>%
#  mutate(hourArrivalTimeRaw = hour(segmentsArrivalTimeRaw))  %>%
#  mutate(hourDepartureTimeRaw= hour(segmentsDepartureTimeRaw)) %>%
#  mutate(dayArrivalTimeRaw = day(segmentsArrivalTimeRaw))  %>%
#  mutate(dayDepartureTimeRaw= day(segmentsDepartureTimeRaw)) %>%
#  mutate(monthArrivalTimeRaw = month(segmentsArrivalTimeRaw))  %>%
#  mutate(monthDepartureTimeRaw= month(segmentsDepartureTimeRaw)) %>%
  mutate(weekdayArrivalTimeRaw = factor(wday(segmentsArrivalTimeRaw)))  %>%
  mutate(weekdayDepartureTimeRaw= factor(wday(segmentsDepartureTimeRaw))) %>%
  #transforms number columns from character to numeric
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  #transforms the categorical data into factors
#  mutate(startingAirport = factor(startingAirport)) %>%
#  mutate(destinationAirport = factor(destinationAirport)) %>%
#  mutate(isBasicEconomy = factor(isBasicEconomy)) %>%
#  mutate(isRefundable = factor(isRefundable)) %>%
#  mutate(segmentsAirlineName = factor(segmentsAirlineName)) %>%
#  mutate(segmentsEquipmentDescription = factor(segmentsEquipmentDescription)) %>%
#  mutate(segmentsCabinCode = factor(segmentsCabinCode)) %>%
#  select(-c("segmentsArrivalTimeRaw",
#            "segmentsDepartureTimeRaw")) %>%
  drop_na() %>%#drops the nas
  collect()


# Print dimensions
#comm.cat(comm.rank(), 
#         "dim", 
#         dim(my_data), "\n", # dim(): outputs dataframe dimensions
#         all.rank = TRUE) # ensures the output is generated from all processors


# Create a function to gather up the dataframes
allgather.data.frame = function(x) {
  cnames = names(x) # gets the column names
  x = lapply(x, 
             function(x) do.call(c, 
                                 allgather(x))) # function that 
  x = as.data.frame(x)
  names(x) = cnames
  x
}

# Gather up the data
data = allgather.data.frame(my_data)

rm(my_data) # remove old data to free up space

## Check on the result
#comm.cat(comm.rank(), "dim", dim(data), "\n", all.rank = TRUE)
#comm.print(memuse::Sys.procmem()$size, all.rank = TRUE)

# TIME IT
#end_time <- Sys.time()
#cat("Data Preparation Time: ", round(end_time-start_time,2), "\n")







################################ TRAIN/TEST SPLIT ###################################
# Sample only 100,000 to start with
i_samp = sample.int(nrow(data), SAMPLE_SIZE) #random sample of integers
data = data[i_samp, ] #keep only the random selected data

n = nrow(data)
n_test = floor(0.2 * n)
i_test = sample.int(n, n_test)
train = data[-i_test, ]
my_test = data[i_test, ][comm.chunk(n_test, form = "vector"), ] 
rm(data)  # no longer needed, free up memory


################################ PARALLEL RANDOM FOREST ###################################
ntree = 100
my_ntree = comm.chunk(ntree, form = "number", rng = TRUE, seed = 12345)
rF = function(nt, tr) 
  randomForest(totalFare ~ ., data = tr, ntree = nt, nodesize = SAMPLE_SIZE/100, norm.votes = FALSE) 
nc = as.numeric(commandArgs(TRUE)[2]) 
rf = mclapply(seq_len(my_ntree), rF, tr = train, mc.cores = nc)
rf = do.call(combine, rf)  # reusing rf name to release memory after operation
rf = allgather(rf) 
rf = do.call(combine, rf)
my_pred = as.vector(predict(rf, my_test))




################################ ACCURACY CHECK ###################################
# correct = allreduce(sum(my_pred == my_test$your_true_category))  # classification
# Calculate for SSE
sse = allreduce(sum((my_pred - my_test$totalFare)^2)) # regression
# Calculate for RMSE
rmse = sqrt(sse/n_test)
# comm.cat("Proportion Correct:", correct/(n_test), "\n") #categorical
comm.cat("\n RMSE:", rmse, "\n")

# Calculate for mean
mean = allreduce(sum(my_test$totalFare)) / n_test
comm.cat("Mean:", mean, "\n")

# Calculate for COV
comm.cat("Coefficient of Variation:", 100*rmse/mean, "\n")

# Check predictions
#print("Actual")
#print(my_test$totalFare[1:100])
#print("Predicted")
#print(my_pred[1:100])

# TIME IT
#end_time <- Sys.time()
#cat("Total Time: ", round(end_time-start_time,2),"\n")
cat("Parallel Random Forest Code finished running. \n")

finalize()