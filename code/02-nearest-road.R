library(tidyverse)
library(duckdb)
library(glue)
library(tictoc)
library(duckdbfs)
library(sf)

# All trail keys
# https://wiki.openstreetmap.org/wiki/Key:highway

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")
con |> dbExecute("INSTALL json; LOAD json;")
con |> dbExecute("
SET memory_limit = '300GB';
SET preserve_insertion_order = true;
SET threads TO 20;
")


con |> dbExecute("
CREATE OR REPLACE TABLE occ AS
SELECT *, ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true) AS geom3310
FROM st_read('data/verified_occurrences.gpkg');

CREATE INDEX idx_occ_geom3310 ON occ USING RTREE (geom3310);
")


con |> dbExecute("
CREATE OR REPLACE TABLE trails AS
SELECT *, ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true) AS geom3310
FROM st_read('data/place_roads.gpkg');

CREATE INDEX idx_trails_geom3310 ON trails USING RTREE (geom3310);")

con |> dbGetQuery("DESCRIBE trails")
con |> dbGetQuery("SELECT tags_json FROM trails LIMIT 5")
con |> dbGetQuery("DESCRIBE occ")

# con |> dbGetQuery("SELECT unnest((SELECT to_json(tags_json) FROM trails LIMIT 10), recursive:= true)")

nness_query <- glue("
SELECT
    gbifid,
    place_name,
    basisofrecord,
    trails3.feature_id AS nearest_trail_id,
    ST_Distance(occ.geom3310, trails3.geom3310) AS distance
FROM
    occ,
    LATERAL (
        SELECT
            feature_id,
            geom3310
        FROM
            trails
        WHERE occ.place_name = trails.name
        ORDER BY
            ST_Distance(occ.geom3310, trails.geom3310)
        LIMIT 1
    ) AS trails3;
")

con |> dbGetQuery(sprintf("EXPLAIN %s", nness_query))

tic()
con |> dbExecute(glue(sprintf("CREATE OR REPLACE TABLE nearest_trail AS %s", nness_query)))
toc()

con |> dbGetQuery("DESCRIBE nearest_trail")
con |> dbExecute(
  "COPY (SELECT * FROM nearest_trail) TO 'data/occ_nearest_trail.csv';"
)


# Add random points ---------------------------------------
con |> dbExecute("
CREATE OR REPLACE VIEW places AS
SELECT * -- EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');")

places <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom", crs = 4326)

# Generate random points for each site
random_pts <- places |>
  group_split(name) |>
  map_df(function(place) {
    # browser()
    pts <- place |>
      st_make_valid() |>
      st_sample(size = 10000, type = "random") |>
      st_as_sf() |>
      mutate(
        basisofrecord = "random",
        place_name = place |> as_tibble() |> distinct(name) |> pull(1),
        gbifid = NA
      )

    return(pts)
  })

random_pts |> write_sf("data/random_pts.gpkg")

con |> dbExecute("
CREATE OR REPLACE TABLE random AS
SELECT *, ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true) AS geom3310
FROM st_read('data/random_pts.gpkg');

CREATE INDEX idx_rand_geom3310 ON occ USING RTREE (geom3310);
")

nnrand_query <- glue("
SELECT
    gbifid,
    place_name,
    basisofrecord,
    trails3.feature_id AS nearest_trail_id,
    ST_Distance(random.geom3310, trails3.geom3310) AS distance
FROM
    random,
    LATERAL (
        SELECT
            feature_id,
            geom3310
        FROM
            trails
        WHERE random.place_name = trails.name
        ORDER BY
            ST_Distance(random.geom3310, trails.geom3310)
        LIMIT 1
    ) AS trails3
")

con |> dbGetQuery(sprintf("EXPLAIN %s", nnrand_query))

tic()
con |> dbExecute(glue(sprintf("COPY (%s) TO 'data/rand_nearest_trail.csv';", nnrand_query)))
toc()

# COPY (SELECT * FROM nearest_trail) TO 'data/occ_nearest_trail.gpkg' (FORMAT 'GDAL', DRIVER 'GPKG', SRS 'EPSG:4326');


# Testing ---------------------------------------

chosen_pt <- con |>
  tbl("nearest_trail3") |>
  filter(gbifid == "4413376297") |>
  mutate(chosen = 1) |>
  left_join(con |> tbl("occ"), by = "gbifid") |>
  to_sf(conn = con, crs = 4326)

plot(chosen_pt)

roadtypes <- c(
  "residential",
  "primary", "secondary", "tertiary",
  "motorway",
  "trunk"
)

mm_trails <- con |>
  tbl("trails") |>
  filter(name == "Marble/Salmon Mountains") |>
  to_sf(crs = 4326, conn = con) |>
  left_join(
    chosen_pt |> as_tibble() |>
      select(nearest_trail_id, distance, chosen, -geom),
    by = c("feature_id" = "nearest_trail_id")
  ) |>
  mutate(chosen = case_when(
    is.na(chosen) ~ 0,
    T ~ chosen
  )) |>
  mutate(tags = lapply(tags_json, fromJSON)) |>
  filter(map_chr(tags, "highway") %in% roadtypes)

ggplot(mm_trails) +
  geom_sf(aes(color = chosen)) +
  geom_sf(data = chosen_pt)

names(mm_trails)
mm_trails |>
  select(name, tags_json) |>
  mutate(tags = lapply(tags_json, fromJSON)) |>
  filter(map_chr(tags, "highway") %in% roadtypes)

library(jsonlite)

con |>
  tbl("trails") |>
  filter(name == "Marble/Salmon Mountains") |>
  to_sf(crs = 4326, conn = con) |>
  mutate(tags = lapply(tags_json, fromJSON)) |>
  mutate(highway = map_chr(tags, "highway")) |>
  filter(!highway %in% c(
    "unclassified" # ,
    # "footway", "path"
  )) |>
  ggplot() +
  geom_sf(aes(color = highway))
