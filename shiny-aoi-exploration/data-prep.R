library(duckdb)
library(dplyr)
library(duckdbfs)
library(tictoc)
library(sf)

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

# Load in GBIF records
con |> dbExecute("CREATE VIEW gbif AS
SELECT * EXCLUDE stateprovince,
ST_Point(decimallongitude, decimallatitude) AS geom,
-- This fixes the problem I was having with queries
nfc_normalize(stateprovince) AS stateprovince
FROM read_parquet('~/Data/Occurrences/GBIF/occurrence.parquet/*', hive_partitioning = true);
")

# Get marble mountain to start data pipeline
con |> dbExecute("
CREATE VIEW ecoregion AS
SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) as geom
FROM PARQUET_SCAN('~/Data/Boundaries/Natural/Ecoregion_Level_4/us_eco_l4_no_st_epsg4326.parquet');")


mm <- con |>
  tbl("ecoregion") |>
  filter(US_L4NAME == "Marble/Salmon Mountains-Trinity Alps") |>
  to_sf(conn = con)
# filter(US_L4NAME |> str_detect("Marble")) |>
# distinct(US_L4NAME)

plot(mm)

# Join GBIF data to marble mountain

con |>
  tbl("gbif") |>
  colnames()
con |>
  tbl("gbif") |>
  filter(
    kingdom == "Plantae",
    !is.na(species),
    species != ""
  ) |>
  inner_join()

con |> dbExecute("SET memory_limit = '200GB';")

x <- con |>
  st_read(query = "SELECT * FROM ecoregion
WHERE US_L4NAME = 'Marble/Salmon Mountains-Trinity Alps'") |>
  select(-geom)

# Why is writing to parquet so much faster than creating a new table????
tic()
con |> dbExecute(
  sprintf(
    "
COPY (
  SELECT gbif.*, US_L4NAME AS place_name
  FROM gbif
  INNER JOIN ecoregion
  ON ST_INTERSECTS(gbif.geom, ecoregion.geom)
  WHERE US_L4NAME = 'Marble/Salmon Mountains-Trinity Alps'
  -- WHERE kingdom = 'Plantae'
  AND kingdom = 'Plantae'
  AND (coordinateuncertaintyinmeters < 500 OR coordinateuncertaintyinmeters is NULL)
  AND species IS NOT NULL
  AND stateprovince = 'California'
  AND NOT species = '')
  TO '%s' (FORMAT PARQUET);
  ",
    "~/Projects/new-phytologist/shiny-aoi-exploration/occurrences.parquet"
  )
)
toc()
# this took 51 seconds


# Can't read it back in, more errors with
con |> dbExecute("
DROP VIEW IF EXISTS target;
CREATE VIEW target AS
SELECT *
FROM read_parquet('~/Projects/new-phytologist/shiny-aoi-exploration/occurrences.parquet');")

x <- con |>
  tbl("target") |>
  to_sf(conn = con)


x <- st_read(con, query = "SELECT *, ST_GEOMFROMWKB(geom) AS geom FROM target")

county_crosswalk <- open_dataset("~/Projects/new-phytologist/shiny-aoi-exploration/occurrences.parquet", conn = con) |>
  to_sf(conn = con)

plot(county_crosswalk)

con |> dbExecute("
COPY (
  SELECT gbif.*
  FROM gbif
  WHERE kingdom = 'Plantae'
  # AND kingdom = 'Plantae'
  AND (coordinateuncertaintyinmeters < 500 OR coordinateuncertaintyinmeters is NULL)
  AND species IS NOT NULL
  AND NOT species = '';
  ")
