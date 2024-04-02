# Sets seed to be reproduced
set.seed(123)
# Create a sample data frame with random company names represented as letters and sales ranging from 1 to 200
df <- data.frame(
  Company= sample(letters[1:5], 500000, replace = TRUE),
  Sales = sample(1:200, 500000, replace = TRUE)
)

# Creates a function to group companies, find their averages, and return it as a dataframe
company_mean_finder <- function(df){
  # Return the dataframe with the companies grouped and their averages found
  return(aggregate(Sales ~ Company, data = df, FUN = mean, na.action=na.pass))
}

# Profile the function performance
Rprof()

# Use the function
company_means.df <- company_mean_finder(df)

Rprof(NULL)
summaryRprof()
