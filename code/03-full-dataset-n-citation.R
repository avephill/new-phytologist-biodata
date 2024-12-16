#' This script preps a full dataset of data used for attachment to the paper
#' and creates a txt file so that we can register it as a derived dataset in GBIF
#' https://docs.ropensci.org/rgbif/articles/gbif_citations.html

library(tidyverse)
library(googlesheets4)


# verified_occ <- st_read("data/verified_occurrences.gpkg")

# Pre-filter occ
con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("CREATE TABLE targ AS
SELECT *  EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")

full_occ <- con |>
  tbl("targ") |>
  filter(
    # species %in% verified_tam$species,
    # place_name == "One Tam",
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con, crs = 4326)

# Add verified filter
gs4_auth()

species_master <- read_sheet("https://docs.google.com/spreadsheets/d/19vRvSHKxrfHRVe0jgI3AncQyib1B1NBmDQ5-s3OKs5o/edit?gid=0#gid=0")

# Verified
verified_tam <- species_master |>
  filter(verifiedStatus_TAM == "verified") |>
  distinct(species) |>
  pull(1)
verified_mm <- species_master |>
  filter(verifiedStatus_MM == "verified") |>
  distinct(species) |>
  pull(1)
verified_sm <- species_master |>
  filter(verifiedStatus_SMM == "verified") |>
  distinct(species) |>
  pull(1)

verified_occ <- full_occ |>
  filter(
    (place_name == "One Tam" & species %in% verified_tam) |
      (place_name == "Santa Monica Mountains" & species %in% verified_sm) |
      (place_name == "Marble/Salmon Mountains" & species %in% verified_mm)
  ) |> 
  mutate(verifiedTaxon = "verified")

verified_dedup_occ <- verified_occ |>
  distinct(place_name, order, species, recordedby, basisofrecord, decimallatitude, decimallongitude, day, month, year, .keep_all = T)

verified_dedup_occ |> select(where(is.list)

verified_dedup_occ |>
  mutate(
    recordedby = as.character(recordedby),
    issue = as.character(issue),
    identifiedby = as.character(identifiedby),
    typestatus = as.character(typestatus),
    mediatype = as.character(mediatype)
  ) |>
  # mutate(across(where(is.list), ~ map(., toString))) |>
  # unnest(cols = where(is.list)) |>
  write_sf("data/verified_occurrences.gpkg")

# Unverified
unverified_tam <- species_master |>
  filter(verifiedStatus_TAM == "not verified") |>
  distinct(species) |>
  pull(1)
unverified_mm <- species_master |>
  filter(verifiedStatus_MM == "not verified") |>
  distinct(species) |>
  pull(1)
unverified_sm <- species_master |>
  filter(verifiedStatus_SMM == "not verified") |>
  distinct(species) |>
  pull(1)

unverified_occ <- full_occ |>
  filter(
    (place_name == "One Tam" & species %in% unverified_tam) |
      (place_name == "Santa Monica Mountains" & species %in% unverified_sm) |
      (place_name == "Marble/Salmon Mountains" & species %in% unverified_mm)
  )  |> 
  mutate(verifiedTaxon = "not verified")


full_verif_unverif_occ <- bind_rows(verified_occ, unverified_occ)

# Now deduplicate and show whic are duplicates
full_verif_unverif_dedup_occ <- full_verif_unverif_occ |> 
  distinct(place_name, order, species, recordedby, basisofrecord, 
decimallatitude, decimallongitude, day, month, year, verifiedTaxon, .keep_all = T) |> 
  mutate(keepWhenDeduplicating = "keep")

duplicates <- full_verif_unverif_occ |>
  anti_join(full_verif_unverif_dedup_occ |> as_tibble(), by = "gbifid") |>
  mutate(keepWhenDeduplicating = "remove")

occ_w_dup_info <- bind_rows(full_verif_unverif_dedup_occ, duplicates)

# Add 'origin' to data
final_occ_df <- occ_w_dup_info |> left_join(species_master |> 
select(species, Origin), by = c("species")) |> 
as_tibble() |> select(-geom)


final_occ_df |> head()  |> View()
# Looks good
#
final_occ_df |> write_csv("data/full_occ_for_upload.csv")

dataset_info <- final_occ_df |>
  group_by(datasetkey) |>
  count()

# I made it in Zenodo

# Now cite it in GBIF
library(rgbif)
derived_dataset_prep(
  citation_data = dataset_info,
  title = "Filtered GBIF dataset for Wilcox et al 2025",
  description = "These are GBIF data from 3 study areas in California: Marble Mountains, Mount Tamalpais, and Santa Monica Mountains. These data were cleaned, deduplicated, and taxon names were verified. Also included is a column on origin, e.g. naturalized or historically native.",
  source_url = "https://zenodo.org/records/14503844?token=eyJhbGciOiJIUzUxMiJ9.eyJpZCI6IjgyNjRmNGJiLTE4MDktNDQ3MS1hZmVhLWMwMTJlMzA0NTcyZCIsImRhdGEiOnt9LCJyYW5kb20iOiJlMDQ5MDQ5ZTA3MTYxYzllOWJjNTkwMWQyMDM2MjgxNiJ9.cvpHr4cU_MidUAtLmzYTkZ9ViRg1eX7NGyYJn6G0MVr0ZJAlaEz8wzdIDbIReiayKJhab0JXC6LHd3GGHMifXQ"
)

# If output looks ok, run derived_dataset to register the dataset on GBIF
derived_dataset(
 citation_data = dataset_info,
  title = "Filtered GBIF dataset for Wilcox et al 2025",
  description = "These are GBIF data from 3 study areas in California: Marble Mountains, Mount Tamalpais, and Santa Monica Mountains. These data were cleaned, deduplicated, and taxon names were verified. Also included is a column on origin, e.g. naturalized or historically native.",
  source_url = "https://zenodo.org/records/14503844?token=eyJhbGciOiJIUzUxMiJ9.eyJpZCI6IjgyNjRmNGJiLTE4MDktNDQ3MS1hZmVhLWMwMTJlMzA0NTcyZCIsImRhdGEiOnt9LCJyYW5kb20iOiJlMDQ5MDQ5ZTA3MTYxYzllOWJjNTkwMWQyMDM2MjgxNiJ9.cvpHr4cU_MidUAtLmzYTkZ9ViRg1eX7NGyYJn6G0MVr0ZJAlaEz8wzdIDbIReiayKJhab0JXC6LHd3GGHMifXQ"
)
