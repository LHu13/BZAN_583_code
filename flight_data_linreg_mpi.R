suppressMessages(library(pbdMPI))
suppressMessages(library(arrow))
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(MERO))
suppressMessages(library(parallel))


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
            "fareBasisCode")) %>% #unnecessary
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

comm.cat(comm.rank(), "dim", dim(my_data), "\n", all.rank = TRUE)

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
comm.cat(comm.rank(), "dim", dim(data), "\n", all.rank = TRUE)
comm.print(memuse::Sys.procmem()$size, all.rank = TRUE)


