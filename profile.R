# Sets seed to be reproduced
set.seed(123)
# Create a sample data frame with random company names represented as letters and sales ranging from 1 to 200
df <- data.frame(
  Company= sample(letters[1:5], 100000, replace = TRUE),
  Sales = sample(1:200, 100000, replace = TRUE)
)

# Creates a function that groups the rows by company and calculates the mean
company_mean_finder <- function(df) {
  # Find all the unique companies
  company_list <- unique(df$Company)
  # Create a list to store the means
  company_means <- numeric(length(company_list))
  # Go through every row of the original data frame
  for (i in 1:nrow(df)) {
    # Go through all the unique companies
    for (j in 1:length(company_list)) {
      # Reset the vector of company sales as zero
      company_sales <- numeric(0) 
      # 
      for (k in 1:length(company_list)) {
        # Checks if the company of the current row of the data frame matches with the current company
        if (df$Company[i] == company_list[k]) {
          # Combines the vector of company sales to the new found Sales
          company_sales <- c(company_sales, df$Sales[i]) 
        }
      }
      # Appends the calculated mean from the vector of company sales to the company means vector
      company_means[j] <- mean(company_sales)
    }
  }
  # Returns the dataframe with the companies and their respective means
  return(data.frame(Company = company_list, Mean = company_means))
}

Rprof()

# Use the function
company_means.df <- company_mean_finder(df)

Rprof(NULL)
summaryRprof()
