# This script checks to see if we miss much biodiversity if we don't include the
# little satellite piece off of Mt. Tam area

library(tidyverse)
library(duckdb)
library(sf)
library(duckdbfs)

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("
CREATE OR REPLACE VIEW target AS
SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")

con |> dbExecute("
CREATE OR REPLACE TABLE places AS
SELECT * EXCLUDE geom, geom AS geom_wkt, ST_GeomFromTEXT(geom) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');

CREATE INDEX places_idx ON places USING RTREE (geom);")

con |> dbGetQuery("DESCRIBE places")

places_prep <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom_wkt", crs = 4326) |>
  select(-geom, geom = geom_wkt)


# Contiguous Tam
contig_tam <- places_prep |>
  filter(name == "One Tam") |>
  st_cast("POLYGON") |>
  st_make_valid() %>%
  mutate(area = st_area(.)) |>
  slice_max(area, n = 2) |>
  group_by(name) |>
  summarise(geom = st_union(geom))

# satellite, removed spot
sat_tam <- places_prep |>
  filter(name == "One Tam") |>
  st_cast("POLYGON") |>
  st_make_valid() %>%
  mutate(area = st_area(.)) |>
  slice_min(area, n = 1) |>
  group_by(name) |>
  summarise(geom = st_union(geom))

ggplot() +
  geom_sf(data = contig_tam, fill = "black") +
  geom_sf(data = sat_tam, fill = "blue")

# Add verified filter
tam_occ <- con |>
  tbl("target") |>
  filter(
    # species %in% verified_tam$species,
    place_name == "One Tam",
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con, crs = 4326)

contig_tam_occ <- tam_occ |> st_intersection(contig_tam)

sat_tam_occ <- tam_occ |> st_intersection(sat_tam)

# Find species unique to sat_tam_occ
unique_species <- sat_tam_occ |>
  distinct(species) |>
  anti_join(contig_tam_occ |> distinct(species), by = "species")
