library(tidyverse)
library(duckdb)
library(spatialEco)
library(duckdbfs)
library(patchwork)
library(cowplot)
library(ggspatial)
library(sf)
library(spatstat)
library(stars)
library(units)


con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("
CREATE OR REPLACE VIEW target AS
SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")

con |> dbExecute("
CREATE OR REPLACE TABLE places AS
SELECT * EXCLUDE geom, ST_GeomFromTEXT(geom) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');

CREATE INDEX places_idx ON places USING RTREE (geom);")

places <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom", crs = 4326)

# fix one tam
# places <- places_prep |>
#   filter(name != "One Tam") |>
#   bind_rows(places_prep |> filter(name == "One Tam") |>
#     st_cast("POLYGON") |>
#     st_make_valid() %>%
#     mutate(area = st_area(.)) |>
#     slice_max(area, n = 2) |>
#     group_by(name) |>
#     summarise(geom = st_union(geom)))

places_box <- places |>
  group_by(name) |>
  mutate(geom = geom |> st_bbox() |> st_as_sfc()) |>
  ungroup()

# Add verified filter

full_occ <- con |>
  tbl("target") |>
  filter(
    # species %in% verified_tam$species,
    # place_name == "One Tam",
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con, crs = 4326)

tam_occ <- con |>
  tbl("target") |>
  filter(
    # species %in% verified_tam$species,
    place_name == "One Tam",
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con, crs = 4326)

tam_nni <- nni(tam_occ)

area_extent <- st_as_sfc(st_bbox(tam_occ))


statemap <- ggplot() +
  annotation_map_tile("cartolight", zoom = 7) + # add basemap layer
  geom_sf(data = places_box, fill = NA, linewidth = 3, color = "red") +
  coord_sf(xlim = c(-124.48, -114.13), ylim = c(32.53, 42.01), crs = 4326)

statemap


# Eveg ---------------------------------------
con |> dbExecute("
CREATE OR REPLACE TABLE eveg AS
SELECT * EXCLUDE geom, ST_GeomFromWKB(geom) AS geom
FROM read_parquet('~/Data/Environment/EVT/All_CA_EVT.parquet');

CREATE INDEX eveg_idx ON eveg USING RTREE (geom);
")


st_read("~/Data/Boundaries/Natural/Ecoregion_Level_4/us_eco_l4_no_st.shp") |> st_crs()

con |> dbExecute("
CREATE OR REPLACE TABLE ecoregion AS
SELECT *,
    ST_Transform(geom, 'ESRI:102039', 'EPSG:4326', always_xy := true) AS geom_4326
FROM ST_Read('~/Data/Boundaries/Natural/Ecoregion_Level_4/us_eco_l4_no_st.shp');
")

con |> dbExecute("CREATE INDEX ecoregion_idx ON ecoregion USING RTREE (geom);")

con |> dbGetQuery("DESCRIBE ecoregion")
con |> dbGetQuery("DESCRIBE places")

# Join ecoregion to places
con |> dbExecute("
COPY (
SELECT places.name, ST_AsText(places.geom) AS places_wkt, ecoregion.US_L4NAME,
  ecoregion.US_L3NAME, ST_AsText(ecoregion.geom_4326) AS eco_wkt,
  ST_Intersection(places.geom, ecoregion.geom_4326) AS intersect_geom
FROM places
INNER JOIN ecoregion ON ST_INTERSECTS(places.geom, ecoregion.geom_4326))
TO 'data/place_ecoregion.gpkg' (FORMAT 'GDAL', DRIVER 'GPKG', SRS 'EPSG:4326')
")

place_eco <- st_read("data/place_ecoregion.gpkg")

place_eco |> select(-c(eco_wkt, places_wkt))

place_eco |>
  filter(name == "One Tam") |>
  ggplot() +
  geom_sf(aes(fill = US_L4NAME))

plot(places_prep |>
  filter(name == "One Tam"), main = "One Tam")

peco_mapprep <- place_eco |>
  select(-c(eco_wkt, places_wkt)) |>
  # Fix the little Muir Woods
  filter(name != "One Tam") |>
  bind_rows(place_eco |>
    filter(name == "One Tam") |>
    select(-c(eco_wkt, places_wkt)) |>
    st_cast("POLYGON") |>
    st_make_valid() %>%
    rowwise() |>
    mutate(northernmost_lat = st_bbox(geom)["ymax"]) |>
    filter(northernmost_lat < 38.05) |> # Extract the northernmost latitude for each geometry
    ungroup() |>
    group_by(name, US_L4NAME) |>
    summarise(geom = st_union(geom)))


# Get total study area area
study_areakm2 <- peco_mapprep |>
  group_by(name) |>
  summarise(place_areakm2 = st_area(st_union(geom)) |> set_units("km2")) |>
  as_tibble() |>
  select(-geom)

peco_w_area <-
  peco_mapprep |>
  group_by(name, US_L4NAME) |>
  summarize(geom = st_union(geom)) |>
  ungroup() |>
  mutate(polygon_areakm2 = st_area(geom) |> set_units("km2")) |>
  left_join(study_areakm2, by = "name") |>
  mutate(ecoregion_perc = (polygon_areakm2 / place_areakm2 * 100) |>
    round(1) |>
    drop_units()) |>
  mutate(ecoregion_perc_label = sprintf("%s (%s%%)", US_L4NAME, ecoregion_perc)) |>
  filter(ecoregion_perc > .1)

# For recentering
# recenter_point <- st_sfc(st_point(c(-120, 30)), crs = st_crs(place_eco))
# recenter_vector <- place_eco |>
#   # rowwise() %>%
#   group_by(name) |>
#   mutate(offset_geom = (recenter_point - st_centroid(st_union(geom)))) |>
#   ungroup() |>
#   as_tibble() |>
#   select(name, offset_geom)

# peco_recent <- place_eco |>
#   select(-c(eco_wkt, places_wkt)) |>
#   left_join(recenter_vector, by = "name") |>
#   # rowwise() |>
#   mutate(recenter_geom = geom + offset_geom) |>
#   st_set_geometry("recenter_geom") |>
#   st_set_crs(4326)

# peco_recent |>
#   select(-c(geom, offset_geom)) |>
#   ggplot() +
#   geom_sf(aes(fill = US_L4NAME)) +
#   facet_grid(vars(name)) +
#   coord_sf()

# Create a list of plots, one for each facet
facet_plots <- peco_w_area |>
  group_split(name) |>
  map(~ {
    baseplot <- ggplot(data = .x) +
      geom_sf(aes(fill = ecoregion_perc_label)) +
      coord_sf() +
      ggtitle(unique(.x$name)) +
      theme_void() +
      theme(
        legend.position = "right",
        panel.border = element_rect(color = "red", linewidth = 4, fill = NA)
      ) +
      labs(fill = "Level 4 Ecoregion")

    # baseplot
    ggdraw(baseplot) +
      draw_text(paste("Area:", unique(.x$place_areakm2) |> round(0), "km²"),
        x = 0.55, y = 0.75, hjust = 0.5, size = 12,
        fontface = "bold", color = "blue"
      )
  })



# Combine the plots using patchwork
combined_plot <- wrap_plots(
  facet_plots,
  ncol = 1
) # Adjust ncol or nrow as needed
combined_plot

master_plot <- statemap + combined_plot +
  plot_layout(
    widths = c(1.2, 1),
    # heights = c(1, 1),
    ncol = 2
  )
ggsave("results/study_area.png", master_plot, width = 6)
