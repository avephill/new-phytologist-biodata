library(rsconnect)

rsconnect::deployApp("/home/ahill/Projects/new-phytologist/shiny-aoi-exploration",
  appFiles = c(
    "aoi-exploration.Rmd", "occurrences.parquet",
    "place_boundaries.parquet"
  )
)
