library(duckdb)
library(tidyverse)

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")
con |> dbExecute("INSTALL json; LOAD json;")
# con |> dbDisconnect(shutdown = T)

con |> dbExecute("
CREATE OR REPLACE VIEW osm AS
SELECT *
FROM read_parquet('~/Data/Environment/OpenStreetMap/files/california.pbf_nofilter_noclip_compact.parquet');")

con |> dbExecute("
CREATE OR REPLACE VIEW places AS
SELECT * EXCLUDE geom, ST_GeomFromTEXT(geom) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');")

con |> tbl("osm")

con |> dbGetQuery("SELECT * FROM osm ORDER BY feature_id LIMIT 10")
con |> dbGetQuery("DESCRIBE osm")
con |> dbGetQuery("DESCRIBE places")
con |> dbGetQuery("SELECT * FROM places LIMIT 4")

con |> dbGetQuery("
SELECT DISTINCT map_keys(tags)
FROM osm
LIMIT 5
")

con |> dbGetQuery("
SELECT osm.*, name
FROM osm
JOIN places
ON ST_INTERSECTS(places.geom, osm.geometry)
WHERE 'highway' IN tags
LIMIT 10
")

# "highway": True,  # All types of highways (roads)
#     "route": "hiking"  # Hiking trails
con |> dbExecute("
COPY (
    SELECT feature_id,
    to_json(tags) AS tags_json,  -- Convert MAP to JSON
    name,
    ST_AsWKB(osm.geometry) AS geom
    FROM osm
    JOIN places
    ON ST_INTERSECTS(ST_ConvexHull(places.geom), osm.geometry)
    WHERE 'highway' IN map_keys(tags)
    AND ST_GeometryType(osm.geometry) != 'POINT'
) TO 'data/place_roads_prep.gpkg' (FORMAT 'GDAL', DRIVER 'GPKG', SRS 'EPSG:4326');
")

# Now filter specific highway types (didn't figure out how to do it in duckdb)
library(sf)
library(jsonlite)
library(tidyverse)
highways <- st_read("data/place_roads_prep.gpkg") |>
  mutate(tags = lapply(tags_json, fromJSON)) |>
  mutate(
    highway = map_chr(tags, "highway") # ,
    # surface = map_chr(tags, "surface")
  )
roadtypes <- c(
  "motorway",
  "trunk",
  "primary", "secondary",
  "tertiary",
  "residential"
)

target_highways <- highways |>
  filter(highway %in% roadtypes) |>
  select(-tags)

target_highways |> select(where(is.list))

write_sf(target_highways, sprintf("data/place_roads_%s.gpkg", format(Sys.Date(), "%Y-%m-%d")))

x <- st_read(sprintf("data/place_roads_%s.gpkg", format(Sys.Date(), "%Y-%m-%d")))
library(sf)
plot(x |> filter(name == "One Tam") |> st_geometry())
View(x)

# All trail keys
# https://wiki.openstreetmap.org/wiki/Key:highway
