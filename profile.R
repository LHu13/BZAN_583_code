

# Profile the function performance
Rprof()

# Use the function
company_means.df <- company_mean_finder(df)

Rprof(NULL)
summaryRprof()
