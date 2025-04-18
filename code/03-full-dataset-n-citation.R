#' This script preps a full dataset of data used for attachment to the paper
#' and creates a txt file so that we can register it as a derived dataset in GBIF
#' https://docs.ropensci.org/rgbif/articles/gbif_citations.html

library(tidyverse)
library(googlesheets4)
library(duckdb)
library(sf)
library(arrow)
library(googlesheets4)



# Read in verified and unverified occurrences
# from 00-data-prep.R
verified_occ <- st_read("data/verified_occurrences_2025-04-18.gpkg")
unverified_occ <- st_read("data/unverified_occurrences_2025-04-18.gpkg")

full_verif_unverif_occ <- bind_rows(verified_occ, unverified_occ)

# Now deduplicate and show which are duplicates
full_verif_unverif_dedup_occ <- full_verif_unverif_occ |>
  distinct(place_name, order, species, recordedby, basisofrecord,
    decimallatitude, decimallongitude, day, month, year, verifiedTaxon,
    .keep_all = T
  ) |>
  mutate(keepWhenDeduplicating = "keep")

duplicates <- full_verif_unverif_occ |>
  anti_join(full_verif_unverif_dedup_occ |> as_tibble(), by = "gbifid") |>
  mutate(keepWhenDeduplicating = "remove")

occ_w_dup_info <- bind_rows(full_verif_unverif_dedup_occ, duplicates)

occ_w_dup_info |> count(keepWhenDeduplicating)

# Read in species master
gs4_auth(email = "ahill@calacademy.org")
species_master <- read_sheet("https://docs.google.com/spreadsheets/d/19vRvSHKxrfHRVe0jgI3AncQyib1B1NBmDQ5-s3OKs5o/edit?gid=0#gid=0")

# Add 'origin' to data
final_occ_df <- occ_w_dup_info |>
  left_join(species_master |>
    select(species, Origin), by = c("species")) |>
  as_tibble()


final_occ_df |>
  head() |>
  View()
final_occ_df |> filter(species == "Parietaria floridana")
# Looks good
#
final_occ_df |> write_csv("data/full_gbif_for_analysis.csv")

dataset_info <- final_occ_df |>
  group_by(datasetkey) |>
  count()

# I made it in Zenodo

# Now cite it in GBIF
library(rgbif)
derived_dataset_prep(
  citation_data = dataset_info,
  title = "Filtered GBIF dataset for Wilcox et al 2025",
  description = "These are GBIF data for land plants from 3 study areas in California: Marble Mountains, Mount Tamalpais, and Santa Monica Mountains. These data were cleaned, deduplicated, and taxon names were verified. Also included is a column on origin, e.g. naturalized or historically native.",
  source_url = "https://zenodo.org/records/15238775"
)

# If output looks ok, run derived_dataset to register the dataset on GBIF
derived_dataset(
  citation_data = dataset_info,
  title = "Filtered GBIF dataset for Wilcox et al 2025",
  description = "These are GBIF data for land plants from 3 study areas in California: Marble Mountains, Mount Tamalpais, and Santa Monica Mountains. These data were cleaned, deduplicated, and taxon names were verified. Also included is a column on origin, e.g. naturalized or historically native.",
  source_url = "https://zenodo.org/records/15238775"
)



# Troubleshooting --------------------------------------------------------------
# Okay now checking why there are inconsistencies with Becky's data
library(tidyverse)
library(sf)
zenodo_og <- read_csv("data/full_occ_for_upload.csv")
zenodo_og |>
  head(10) |>
  View()
zenodo_og |>
  filter(species == "Parietaria floridana")
con |>
  tbl("targ") |>
  filter(species == "Parietaria floridana")
verified_occ <- st_read("data/verified_occurrences.gpkg")
verified_occ |>
  filter(species == "Parietaria floridana")

con |> dbExecute("CREATE TABLE old_occ AS
SELECT *  EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/shiny-aoi-exploration/occurrences_10-04-24.parquet');")

con |>
  tbl("old_occ") |>
  filter(species == "Parietaria floridana") |>
  collect() |>
  View()

con |> dbExecute("CREATE TABLE shiny_occ AS
SELECT *  EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/shiny-aoi-exploration/occurrences.parquet');")

con |>
  tbl("shiny_occ") |>
  filter(species == "Parietaria floridana") |>
  collect() |>
  View()

# Read in original/all GBIF records
ogcon <- dbConnect(duckdb())
ogcon |> dbExecute("INSTALL spatial; LOAD spatial;")

# Load in GBIF records
ogcon |> dbExecute("CREATE VIEW gbif AS
SELECT * EXCLUDE stateprovince,
ST_Point(decimallongitude, decimallatitude) AS geom,
-- This fixes the problem I was having with queries
nfc_normalize(stateprovince) AS stateprovince
FROM read_parquet('~/Data/Occurrences/GBIF/occurrence.parquet/*', hive_partitioning = true);
")
ogcon |> dbExecute("
DROP VIEW IF EXISTS places;
CREATE VIEW places AS
SELECT * EXCLUDE geom, ST_GeomFromText(geom) as geom
FROM PARQUET_SCAN('~/Projects/new-phytologist/data/place_boundaries.parquet');")
ogcon |> dbGetQuery("
SELECT gbif.*--, name AS place_name
  FROM gbif
--  INNER JOIN places
--  ON ST_INTERSECTS(gbif.geom, places.geom)
  WHERE phylum = 'Tracheophyta'
  -- AND kingdom = 'Plantae'
  -- AND (coordinateuncertaintyinmeters < 500 OR coordinateuncertaintyinmeters is NULL)
  AND species IS NOT NULL
  AND stateprovince = 'California'
  AND NOT species = ''
  AND gbifid = '2565957851'
")
#' Okay Becky said Parietaria floridana is not in the GBIF dataset she has.
#' it is present when you don't do spatial filtering.
#' Let's see where it is
library(arrow)

place_boundaries <- read_parquet("data/place_boundaries.parquet") |>
  st_as_sf(wkt = "geom", crs = 4326)

Parietaria_floridana <- ogcon |>
  tbl("gbif") |>
  filter(gbifid == "2565957851") |>
  collect() |>
  st_as_sf(coords = c("decimallongitude", "decimallatitude"), crs = 4326)

ggplot() +
  geom_sf(data = place_boundaries |> filter(name == "Santa Monica Mountains")) +
  geom_sf(data = Parietaria_floridana, color = "red", size = 3, shape = 4)

# Okay, yay! Just use occurrences_2024-11-25.parquet
