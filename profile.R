# Sets seed to be reproduced
set.seed(123)
# Create a sample data frame with random company names represented as letters and sales ranging from 1 to 200
df <- data.frame(
  Company= sample(letters[1:5], 1000000, replace = TRUE),
  Sales = sample(1:200, 1000000, replace = TRUE)
)

# Creates a function that groups the rows by company and calculates the mean
company_mean_finder <- function(df) {
  # Find all the unique companies
  company_list <- unique(df$Company)
  # Create a list to store the means
  company_means <- numeric(length(company_list))
  # Go through all the unique companies to then find all the rows with company values
  for (j in 1:length(company_list)) {
    # Reset the vector of company sales as zero
    company_sales <- numeric(0) 
    # Go through every row of the original data frame
    for (i in 1:nrow(df)) {
      # Checks if the company of the current row of the data frame matches with the current company
      if (df$Company[i] == company_list[j]) {
        # Appends the categorized sales to the larger vector of company sales
        company_sales <- c(company_sales, df$Sales[i]) 
      }
    }
    # Calculates and apends the calculated mean from the vector of company sales to the company means vector
    company_means[j] <- mean(company_sales)
  }
  # Create the company means dataframe
  company_means.df <- data.frame(Company = company_list, Mean = company_means)
  # Alphabetize the company names
  company_means.df <- company_means.df[order(company_means.df$Company),]
  # Reset row index
  rownames(company_means.df) <- NULL
  # Returns the dataframe with the companies and their respective means
  return(company_means.df)
}

# Profile the function performance
Rprof()

# Use the function
company_means.df <- company_mean_finder(df)

Rprof(NULL)
summaryRprof()
