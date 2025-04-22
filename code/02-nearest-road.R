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
FROM st_read('data/verified_occurrences_dedup_2025-04-21.gpkg');

CREATE INDEX idx_occ_geom3310 ON occ USING RTREE (geom3310);
")

# Create tables trail and remove trails less than 10m in length because they are not real trails
con |> dbExecute("
CREATE OR REPLACE TABLE trails AS
SELECT *,
    ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true) AS geom3310,
    ST_Length(ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true)) AS trail_length
FROM st_read('data/place_roads_2025-04-21.gpkg')
WHERE ST_Length(ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true)) > 10;

CREATE INDEX idx_trails_geom3310 ON trails USING RTREE (geom3310);")

con |> dbGetQuery("DESCRIBE trails")
# con |> tbl('trails') |> filter(trail_length < 50)
con |> dbGetQuery("SELECT tags_json FROM trails LIMIT 5")
con |> dbGetQuery("DESCRIBE occ")

# con |> dbGetQuery("SELECT unnest((SELECT to_json(tags_json) FROM trails LIMIT 10), recursive:= true)")

# Get list of all place names to process in batches
place_names <- con |>
  dbGetQuery("SELECT DISTINCT place_name FROM occ") |>
  pull(1)

# Create the output table first
con |> dbExecute("
    CREATE OR REPLACE TABLE nearest_trail (
        gbifid VARCHAR,
        place_name VARCHAR,
        basisofrecord VARCHAR,
        nearest_trail_id VARCHAR,
        distance DOUBLE
    );
")

# Create a chunking mechanism with a specific batch_size
con |> dbExecute("
  CREATE OR REPLACE TABLE occ_chunks AS
  SELECT
    gbifid,
    place_name,
    basisofrecord,
    geom3310,
    (ROW_NUMBER() OVER (PARTITION BY place_name ORDER BY gbifid)) % 100 AS chunk_id
  FROM occ;

  -- Create both regular and spatial indexes for efficient filtering
  CREATE INDEX idx_chunks ON occ_chunks(place_name, chunk_id);
  CREATE INDEX idx_chunks_spatial ON occ_chunks USING RTREE (geom3310);
")

place_names <- place_names |> str_subset("Santa Monica Mountains", negate = T)
# Process places one by one, and each place by chunks
tic("Total processing time")
for (i in seq_along(place_names)) {
  place <- place_names[i]

  cat(sprintf(
    "\nProcessing place %d of %d: %s\n",
    i, length(place_names), place
  ))

  # Filter trails for this place first to reduce join complexity
  con |> dbExecute(sprintf("
    CREATE OR REPLACE TEMPORARY TABLE place_trails AS
    SELECT * FROM trails WHERE name = '%s';

    -- Add spatial index on the filtered trails
    CREATE INDEX idx_place_trails_spatial ON place_trails USING RTREE (geom3310);
  ", place))

  # Get number of chunks for this place
  n_chunks <- con |>
    dbGetQuery(sprintf("
    SELECT COUNT(DISTINCT chunk_id) AS n_chunks
    FROM occ_chunks
    WHERE place_name = '%s'
  ", place)) |>
    pull(1)

  if (n_chunks == 0) {
    cat("No occurrences for this place, skipping\n")
    next
  }

  cat(sprintf("Processing %d chunks for %s\n", n_chunks, place))

  # Process each chunk separately
  for (chunk in 0:(n_chunks - 1)) {
    tic(sprintf("Chunk %d", chunk))

    # Process this chunk
    con |> dbExecute(sprintf("
      INSERT INTO nearest_trail
      SELECT
        o.gbifid,
        o.place_name,
        o.basisofrecord,
        t.feature_id AS nearest_trail_id,
        ST_Distance(o.geom3310, t.geom3310) AS distance
      FROM
        (SELECT * FROM occ_chunks WHERE place_name = '%s' AND chunk_id = %d) o,
        LATERAL (
          SELECT feature_id, geom3310
          FROM place_trails
          ORDER BY ST_Distance(o.geom3310, geom3310)
          LIMIT 1
        ) t;
    ", place, chunk))

    toc()
  }
}
toc()

# Final export
con |> dbExecute(
  sprintf(
    "COPY (SELECT * FROM nearest_trail) TO 'data/occ_nearest_trail_%s.csv';",
    format(Sys.Date(), "%Y-%m-%d")
  )
)


# Add random points ---------------------------------------
# con |> dbExecute("
# CREATE OR REPLACE VIEW places AS
# SELECT * -- EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
# FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');")

# places <- con |>
#   tbl("places") |>
#   collect() |>
#   st_as_sf(wkt = "geom", crs = 4326)

# # Generate random points for each site
# random_pts <- places |>
#   group_split(name) |>
#   map_df(function(place) {
#     # browser()
#     pts <- place |>
#       st_make_valid() |>
#       st_sample(size = 10000, type = "random") |>
#       st_as_sf() |>
#       mutate(
#         basisofrecord = "random",
#         place_name = place |> as_tibble() |> distinct(name) |> pull(1),
#         gbifid = NA
#       )

#     return(pts)
#   })

# random_pts |> write_sf("data/random_pts.gpkg")

# con |> dbExecute("
# CREATE OR REPLACE TABLE random AS
# SELECT *, ST_TRANSFORM(geom, 'EPSG:4326', 'EPSG:3310', always_xy := true) AS geom3310
# FROM st_read('data/random_pts.gpkg');

# CREATE INDEX idx_rand_geom3310 ON occ USING RTREE (geom3310);
# ")

# nnrand_query <- glue("
# SELECT
#     gbifid,
#     place_name,
#     basisofrecord,
#     trails3.feature_id AS nearest_trail_id,
#     ST_Distance(random.geom3310, trails3.geom3310) AS distance
# FROM
#     random,
#     LATERAL (
#         SELECT
#             feature_id,
#             geom3310
#         FROM
#             trails
#         WHERE random.place_name = trails.name
#         ORDER BY
#             ST_Distance(random.geom3310, trails.geom3310)
#         LIMIT 1
#     ) AS trails3
# ")

# con |> dbGetQuery(sprintf("EXPLAIN %s", nnrand_query))

# tic()
# con |> dbExecute(glue(sprintf(
#   "COPY (%s) TO 'data/rand_nearest_trail_%s.csv';",
#   nnrand_query, format(Sys.Date(), "%Y-%m-%d")
# )))
# toc()

# COPY (SELECT * FROM nearest_trail) TO 'data/occ_nearest_trail.gpkg' (FORMAT 'GDAL', DRIVER 'GPKG', SRS 'EPSG:4326');


# Testing ---------------------------------------
occ_results <- st_read("data/verified_occurrences_dedup_2025-04-21.gpkg")
# trail_results <- read_csv("data/occ_nearest_trail.csv")
trail_results <- read_csv("data/occ_nearest_trail_2025-04-22.csv")
place_roads <- st_read("data/place_roads_2025-04-21.gpkg")


# Calculate length of each linestring in kilometers
# place_roads <- place_roads |>
#   mutate(length_m = st_length(geom) |> units::set_units("m"))
# place_roads |>
#   filter(length_m < 10 |> units::set_units("m")) |>
#   pull(name) |>
#   table()

# place_roads

# place <- "One Tam"
# place <- "Santa Monica Mountains"
place <- "Marble/Salmon Mountains"
road_subset_ids <- place_roads |>
  filter(name == place) |>
  distinct(feature_id) |>
  sample_n(282) |>
  pull(1)
occ_results_rd <- occ_results |>
  # filter(place_name == place) |>
  mutate(gbifid = as.numeric(gbifid)) |>
  right_join(trail_results |>
    filter(nearest_trail_id %in% road_subset_ids) |>
    select(gbifid, nearest_trail_id, distance), by = "gbifid")

# occ_results_rd |> write_sf("data/occ_results_mt_testing.gpkg")

ggplot() +
  geom_sf(data = occ_results_rd, aes(color = nearest_trail_id, alpha = distance, shape = basisofrecord)) +
  # geom_sf(data = occ_results_rd |> sample_n(5000), aes(color = nearest_trail_id), alpha = 0) +
  geom_sf(data = place_roads |> filter(feature_id %in% road_subset_ids), aes(color = feature_id), linewidth = 1) +
  guides(color = "none")


ggplot() +
  geom_sf(
    data = occ_results |>
      filter(place_name == place) |>
      mutate(gbifid = as.numeric(gbifid)) |>
      left_join(trail_results |>
        select(gbifid, nearest_trail_id, distance), by = "gbifid") |>
      sample_n(18225),
    aes(color = nearest_trail_id, alpha = distance, shape = basisofrecord)
  ) +
  # geom_sf(data = occ_results_rd |> sample_n(5000), aes(color = nearest_trail_id), alpha = 0) +
  # geom_sf(data = place_roads |> filter(feature_id %in% road_subset_ids), aes(color = feature_id), linewidth = 1) +
  guides(color = "none")

## Write some additional tests here to be sure that the nearest trail analysis worked

# Test 1: Sampling validation - manually verify a small subset
set.seed(123)
place <- "Marble/Salmon Mountains"
sample_pts <- occ_results |>
  filter(place_name == place) |>
  sample_n(10) |>
  mutate(gbifid = as.numeric(gbifid)) |>
  left_join(trail_results |> select(gbifid, nearest_trail_id, distance), by = "gbifid")

# Plot the sampled points with their nearest roads for visual inspection
ggplot() +
  geom_sf(data = place_roads |> filter(name == place), color = "gray") +
  geom_sf(data = sample_pts, aes(color = "Occurrence"), size = 3) +
  geom_sf(
    data = place_roads |>
      filter(feature_id %in% sample_pts$nearest_trail_id),
    aes(color = "Nearest Road"), linewidth = 1.5
  ) +
  scale_color_manual(values = c("Occurrence" = "red", "Nearest Road" = "blue")) +
  labs(title = "Manual Validation: Occurrences and Their Nearest Roads", color = "Type") +
  theme_minimal()

# Test 2: Distance distribution analysis - examine the pattern of distances
distance_summary <- trail_results |>
  group_by(place_name) |>
  summarize(
    min_dist = min(distance, na.rm = TRUE),
    mean_dist = mean(distance, na.rm = TRUE),
    median_dist = median(distance, na.rm = TRUE),
    max_dist = max(distance, na.rm = TRUE),
    sd_dist = sd(distance, na.rm = TRUE),
    n = n()
  )

print(distance_summary)

# Plot distance histograms by place
trail_results |>
  ggplot(aes(x = distance)) +
  geom_histogram(bins = 30, fill = "steelblue", alpha = 0.7) +
  facet_wrap(~place_name, scales = "free_y") +
  labs(
    x = "Distance to nearest road (m)", y = "Count",
    title = "Distribution of distances to nearest road by location"
  ) +
  theme_minimal()


# Test 5: Compare with alternative method
# For a small sample, manually calculate distances to verify algorithm correctness
sample_check <- occ_results |>
  filter(place_name == place) |>
  sample_n(20) |>
  st_transform(3310) # Transform to the same CRS used in analysis

roads_check <- place_roads |>
  filter(name == place) |>
  st_transform(3310) # Transform to the same CRS used in analysis

# Manual nearest neighbor calculation
manual_nearest <- sample_check |>
  rowwise() |>
  mutate(
    manual_nearest_id = roads_check$feature_id[which.min(st_distance(geom, roads_check$geom)[1, ])],
    manual_distance = min(st_distance(geom, roads_check$geom)[1, ])
  ) |>
  ungroup() |>
  mutate(gbifid = as.numeric(gbifid)) |>
  left_join(
    trail_results |> select(gbifid, nearest_trail_id, distance),
    by = "gbifid"
  ) |>
  mutate(
    match = manual_nearest_id == nearest_trail_id,
    # Convert manual_distance to numeric to remove units before comparison
    dist_diff = as.numeric(manual_distance) - distance
  )

print(paste(
  "Percentage of matching nearest roads:",
  mean(manual_nearest$match, na.rm = TRUE) * 100
))
print(paste(
  "Mean absolute difference in distances:",
  mean(abs(manual_nearest$dist_diff), na.rm = TRUE)
))




# Garbage ---------------------------------------
chosen_pt <- con |>
  tbl("trail3") |>
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
