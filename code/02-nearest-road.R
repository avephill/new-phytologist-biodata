library(tidyverse)
library(duckdb)
library(glue)
library(tictoc)
library(duckdbfs)

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


# Make figure ---------------------------------------
library(jsonlite)
nearest_trail <- read_csv("data/occ_nearest_trail.csv")

nearest_trail |>
  mutate(
    place_name = case_when(
      place_name == "Marble/Salmon Mountains" ~ "Marble Mountains",
      place_name == "One Tam" ~ "Mt. Tamalpais",
      T ~ place_name
    ),
    basisofrecord = case_when(
      basisofrecord == "HUMAN_OBSERVATION" ~ "iNaturalist",
      basisofrecord == "PRESERVED_SPECIMEN" ~ "Herbarium",
      T ~ basisofrecord
    )
  ) |>
  ggplot() +
  geom_boxplot(aes(x = basisofrecord, y = distance)) +
  facet_wrap(~place_name, scales = "free") +
  labs(x = element_blank(), y = "Distance to nearest road (m)")

nearest_trail |>
  left_join(
    con |> tbl("trails") |>
      collect(),
    by = c("nearest_trail_id" = "feature_id")
  ) |>
  mutate(tags = lapply(tags_json, fromJSON)) |>
  mutate(highway = map_chr(tags, "highway")) |>
  distinct(highway)





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
