suppressMessages(library(pbdMPI))
suppressMessages(library(memuse))
suppressMessages(library(arrow))
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(MERO))
suppressMessages(library(randomForest))
suppressMessages(library(parallel))

start_time <- Sys.time()

ds <- open_dataset("/projects/bckj/Team3/flight_data_parquet/itineraries")

get_hive_var = function(ds, var) 
  sub("/.*$", "", sub(paste0("^.*", var, "="), "", ds$files))

partitions = get_hive_var(ds, "flightDate")  

my_partitions = partitions[comm.chunk(length(partitions), 
                                      form = "vector")]

comm.cat("rank", 
         comm.rank(), 
         "partitions", 
         my_partitions, 
         "\n", 
         all.rank = TRUE)

# Read only the data for the partitions in my_partitions
my_data <- ds %>% 
  filter(flightDate %in% my_partitions) %>% 
  collect()

# Filter, select, mutate, and reduce the data (do separately for debug)
my_data <- my_data %>%
  filter(isNonStop == "True") %>% #remove all the not nonstop flights
  #drop unnecessary columns
  select(-c("segmentsDepartureTimeEpochSeconds", #repeat info
            "segmentsArrivalTimeEpochSeconds", #repeat info
            "segmentsAirlineCode", #repeat info
            "legId", #unnecessary
            "travelDuration", #repeat info
            "fareBasisCode",
            "startingAirport",
            "destinationAirport",
            "baseFare", 
            "segmentsDistance",
            "searchDate",
            "elapsedDays")) %>% #unnecessary
  #convert time columns into datetime format
  mutate(segmentsArrivalTimeRaw =as.POSIXct(segmentsArrivalTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  mutate(segmentsDepartureTimeRaw=as.POSIXct(segmentsDepartureTimeRaw, format = "%Y-%m-%dT%H:%M:%OS", tz = "GMT")) %>%
  #DROPS ALL OTHER MONTHS BESIDES MAY BC DATA FUNKY
  filter(as.integer(format(segmentsArrivalTimeRaw, "%m")) %in% c(5)) %>%
  filter(as.integer(format(segmentsArrivalTimeRaw, "%d")) %in% c(1)) %>%
  #transforms number columns from character to numeric
  transform(segmentsDurationInSeconds=as.numeric(segmentsDurationInSeconds),
            segmentsDistance=as.numeric(segmentsDistance)) %>%
  drop_na() %>% 
  collect()

#comm.cat(comm.rank(), "dim", dim(my_data), "\n", all.rank = TRUE)

## allgather() for data.frames (not in pbdMPI ... yet!)
allgather.data.frame = function(x) {
  cnames = names(x)
  x = lapply(x, function(x) do.call(c, allgather(x)))
  x = as.data.frame(x)
  names(x) = cnames
  x
}
data = allgather.data.frame(my_data)
rm(my_data) # free up memory

## Check on the result
#comm.cat(comm.rank(), "dim", dim(data), "\n", all.rank = TRUE)
#comm.print(memuse::Sys.procmem()$size, all.rank = TRUE)

end_time <- Sys.time()

cat("Finished cleaning data \n", end_time-start_time)

## Parallel random forest part
comm.set.seed(seed = 7654321, diff = FALSE)      #<<

n = nrow(data)
n_test = floor(0.2 * n)
i_test = sample.int(n, n_test)
train = data[-i_test, ][1:1000, ]    # limit to 1k obs for debugging
my_test = data[i_test, ][comm.chunk(n_test, form = "vector"), ]    #<<

end_time <- Sys.time()

cat("Finished splitting data \n", end_time-start_time)

ntree = 64 # start small for debug
my_ntree = comm.chunk(ntree, form = "vector", rng = TRUE, seed = 12345)        #<<
rfc = function(i, mnt, mxn = 6) {
  comm.set.stream(i)                  #<<
  randomForest(totalFare ~ ., 
               train, 
               ntree = mnt[i], 
               maxnodes = mxn, 
               norm.votes = FALSE) #<<
}
my_rf = mclapply(seq_along(my_ntree), rfc, mnt = my_ntree) #<<
my_rf = do.call(combine, my_rf)            #<<
rf_all = allgather(my_rf)                  #<<
rf_all = do.call(combine, rf_all)          #<<

end_time <- Sys.time()

cat("Finished training model \n", end_time-start_time)

my_pred = as.vector(predict(rf_all, my_test))

# correct = allreduce(sum(my_pred == my_test$<your-true-category>))  # classification
sse = allreduce(sum((my_pred - my_test$totalFare)^2)) # regression
# comm.cat("Proportion Correct:", correct/(n_test), "\n")
comm.cat("RMSE:", sqrt(rmse/n_test), "\n")

finalize()

end_time <- Sys.time()

cat("Finished everything \n", end_time-start_time)