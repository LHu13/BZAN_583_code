arrow::install_arrow(verbose = TRUE)
library(arrow)
library(dplyr)

flight_data = open_csv_dataset("/projects/bckj/Team3/flight_data_csv/itineraries.csv")

flight_data %>%
  write_dataset(
    path = "/projects/bckj/Team3/flight_data_parquet/itineraries",
    format = c("parquet"),
    partitioning = c("flightDate"))