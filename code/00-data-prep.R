library(duckdb)
library(dplyr)
library(duckdbfs)
library(tictoc)
library(sf)
library(googlesheets4)
# library(sfarrow)
library(arrow)

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


# Read in boundaries and organize -----------------------------------------

# Santa Monica Mtns
santamonica <- st_read("data/KML_place_boundaries/KML Boundaries/santa-monica-mountains-ecological-management-area.kml") |>
  st_transform(4326) |>
  select(Name, geom = geometry) |>
  mutate(Name = "Santa Monica Mountains")

# OneTam
onetam1 <- st_read("data/KML_place_boundaries/KML Boundaries/marin-municipal-water-district-watershed.kml") |>
  st_transform(4326) |>
  select(Name, geom = geometry)
onetam2 <- st_read("data/KML_place_boundaries/KML Boundaries/mount-tamalpais-state-park.kml") |>
  st_transform(4326) |>
  select(Name, geom = geometry)
onetam3 <- st_read("data/KML_place_boundaries/KML Boundaries/Muir Woods.kml") |>
  st_transform(4326) |>
  select(Name, geom = geometry) |>
  mutate(Name = "Muir Woods")

onetam_prep <- bind_rows(onetam1, onetam2, onetam3) |>
  summarise(
    geom = st_union(geom |> st_buffer(100)),
    Name = "One Tam"
  )

# Remove the little Tam piece
onetam <- onetam_prep |>
  st_cast("POLYGON") |>
  st_make_valid() %>%
  mutate(area = st_area(.)) |>
  slice_max(area, n = 2) |>
  group_by(Name) |>
  summarise(geom = st_union(geom))

# plot(onetam |> st_geometry())

# Marble Mtn
marble <- st_read("data/KML_place_boundaries/KML Boundaries/marble-salmon-mountains-trinity-alps-us.kml") |>
  st_transform(4326) |>
  select(Name, geom = geometry) |>
  mutate(Name = "Marble/Salmon Mountains")

## Combine them ##
all_places <- bind_rows(santamonica, onetam, marble) |>
  rename(name = Name)


# Write to parquet
# This isn't writing the correct metadata and the last update for sfarrow package is old
# st_write_parquet(all_places, "place_boundaries.parquet")

# New way just using WKT
all_places |>
  tibble() |>
  mutate(geom = st_as_text(geom)) |>
  write_parquet("~/Projects/new-phytologist/data/place_boundaries.parquet")





# Now Join GBIF data to places --------------------------------------------

# Read in boundaries parquet to duckdb
con |> dbExecute("
DROP VIEW IF EXISTS places;
CREATE VIEW places AS
SELECT * EXCLUDE geom, ST_GeomFromText(geom) as geom
FROM PARQUET_SCAN('~/Projects/new-phytologist/data/place_boundaries.parquet');")

con |>
  tbl("places") |>
  colnames()
con |> dbExecute("SET memory_limit = '200GB';")


# Why is writing to parquet so much faster than creating a new table????
tic()
con |> dbExecute(
  sprintf(
    "
COPY (
  SELECT gbif.*, name AS place_name
  FROM gbif
  INNER JOIN places
  ON ST_INTERSECTS(gbif.geom, places.geom)
  AND phylum = 'Tracheophyta'
  -- AND kingdom = 'Plantae'
  -- AND (coordinateuncertaintyinmeters < 500 OR coordinateuncertaintyinmeters is NULL)
  AND species IS NOT NULL
  AND stateprovince = 'California'
  AND NOT species = '')
  TO '%s' (FORMAT PARQUET);
  ",
    "~/Projects/new-phytologist/data/occurrences.parquet"
  )
)
toc()
# this took 358 sec

# # Write to gpkg too
# con <- dbConnect(duckdb())
# con |> dbExecute("INSTALL spatial; LOAD spatial;")

# con |> dbExecute("CREATE TABLE occ AS
# SELECT *  EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
# FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")
# con |> dbGetQuery("DESCRIBE occ")
# con |> dbExecute("
# COPY
# (SELECT * FROM occ)
# TO 'data/occurrences.gpkg' (FORMAT 'GDAL', DRIVER 'GPKG', SRS 'EPSG:4326');")

# con |> dbGetQuery("SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
# FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet') LIMIT 5")

# Read it back in
# Can't use the native geometry for some reason, but just recreate it with lonlat
con |> dbExecute("
DROP VIEW IF EXISTS target;
CREATE VIEW target AS
SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")

con |>
  tbl("target") |>
  count()

con |>
  tbl("target") |>
  colnames()

con |> dbDisconnect(shutdown = T)



# Join verified taxon ---------------------------------------
# Join all the curated data from the other collaborators' work on taxa

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("
CREATE OR REPLACE VIEW target AS
SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/occurrences_2024-11-25.parquet');")

con |> dbExecute("
CREATE OR REPLACE VIEW places AS
SELECT * -- EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');")


full_occ_prep <- con |>
  tbl("target") |>
  filter(
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con, crs = 4326)

full_occ <- full_occ_prep |>
  mutate(across(
    where(is.list) & !matches("geom"),
    ~ map_chr(., ~ paste(.x, collapse = "; "))
  ))

## Verified ------------------------------------------------------------
gs4_auth(email = "ahill@calacademy.org")
species_master <- read_sheet("https://docs.google.com/spreadsheets/d/19vRvSHKxrfHRVe0jgI3AncQyib1B1NBmDQ5-s3OKs5o/edit?gid=0#gid=0")
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
  distinct(place_name, order, species, recordedby, basisofrecord,
    decimallatitude, decimallongitude, day, month, year,
    .keep_all = T
  )

verified_occ |> write_sf(sprintf(
  "data/verified_occurrences_%s.gpkg",
  format(Sys.Date(), "%Y-%m-%d")
))

verified_dedup_occ |>
  write_sf(sprintf(
    "data/verified_occurrences_dedup_%s.gpkg",
    format(Sys.Date(), "%Y-%m-%d")
  ))

## Unverified ------------------------------------------------------------
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
  ) |>
  mutate(verifiedTaxon = "not verified")

unverified_occ |>
  write_sf(sprintf(
    "data/unverified_occurrences_%s.gpkg",
    format(Sys.Date(), "%Y-%m-%d")
  ))


verified_occ |>
  count(basisofrecord)
