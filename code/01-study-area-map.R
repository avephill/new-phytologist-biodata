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
library(scales)
library(terra)


con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

# con |> dbExecute("
# CREATE OR REPLACE VIEW target AS
# SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
# FROM read_parquet('~/Projects/new-phytologist/data/verified_occurrences.parquet');")

con |> dbExecute("
CREATE OR REPLACE TABLE places AS
SELECT * EXCLUDE geom, ST_GeomFromTEXT(geom) AS geom, geom AS geom_wkt
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');

CREATE INDEX places_idx ON places USING RTREE (geom);")

con |> dbGetQuery("DESCRIBE places")

places <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom_wkt", crs = 4326) |>
  select(-geom) |>
  rename(geom = geom_wkt)

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

# full_occ <- con |>
#   tbl("target") |>
#   filter(
#     # species %in% verified_tam$species,
#     # place_name == "One Tam",
#     basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
#   ) |>
#   to_sf(conn = con, crs = 4326)

# tam_occ <- con |>
#   tbl("target") |>
#   filter(
#     # species %in% verified_tam$species,
#     place_name == "One Tam",
#     basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
#   ) |>
#   to_sf(conn = con, crs = 4326)


# area_extent <- st_as_sfc(st_bbox(tam_occ))


statemap <- ggplot() +
  annotation_map_tile("cartolight", zoom = 7) + # add basemap layer
  geom_sf(data = places_box, fill = NA, linewidth = 1, color = "black") +
  coord_sf(xlim = c(-124.48, -114.13), ylim = c(32.53, 42.01), crs = 4326) +
  annotation_scale(
    location = "bl",
    width_hint = .3
    # line_width = .5
  ) +
  ggspatial::annotation_north_arrow(
    location = "bl", which_north = "true",
    pad_x = unit(0.4, "in"), pad_y = unit(0.4, "in"),
    style = ggspatial::north_arrow_nautical(
      fill = c("grey40", "white"),
      line_col = "grey20"
    )
  )
statemap


# # Eveg ---------------------------------------
# con |> dbExecute("
# CREATE OR REPLACE TABLE eveg AS
# SELECT * EXCLUDE geom, ST_GeomFromWKB(geom) AS geom
# FROM read_parquet('~/Data/Environment/EVT/All_CA_EVT.parquet');

# CREATE INDEX eveg_idx ON eveg USING RTREE (geom);
# ")

# Ecoregion ---------------------------------------
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
  geom_sf(aes(fill = US_L3NAME))

# Elevation
alt <- rast("~/Data/Environment/CANV_Alt/CANV.tif")


place_alt <- places |>
  # rowwise() %>%
  group_split(name) %>% # Split by the 'name' column
  map_dfr(~ {
    # Process each group (now a data frame) individually
    # browser()
    alt_values <- extract(alt, .x) |> pull(CANV)
    .x %>%
      mutate(
        alt_min = min(alt_values, na.rm = TRUE),
        alt_max = max(alt_values, na.rm = TRUE),
        alt_median = median(alt_values, na.rm = T)
      )
  }) %>%
  as_tibble() |>
  select(-geom)


# Get total study area area
study_areakm2 <- place_eco |>
  group_by(name) |>
  summarise(place_areakm2 = st_area(st_union(geom)) |> set_units("km2")) |>
  as_tibble() |>
  select(-geom)

peco_w_area <-
  place_eco |>
  group_by(name, US_L3NAME) |>
  summarize(geom = st_union(geom)) |>
  ungroup() |>
  mutate(polygon_areakm2 = st_area(geom) |> set_units("km2")) |>
  left_join(study_areakm2, by = "name") |>
  left_join(place_alt, by = "name") |>
  mutate(ecoregion_perc = (polygon_areakm2 / place_areakm2 * 100) |>
    round(1) |>
    drop_units()) |>
  mutate(ecoregion_perc_label = sprintf("%s (%s%%)", US_L3NAME, ecoregion_perc)) |>
  filter(ecoregion_perc > .1) |>
  mutate(name = case_when(
    name == "One Tam" ~ "Mount Tamalpais",
    name == "Marble/Salmon Mountains" ~ "Marble Mountains",
    T ~ name
  )) |>
  # For scale position
  mutate(scale_position = case_when(
    name == "Santa Monica Mountains" ~ "tr",
    T ~ "bl"
  ))



# Create a list of plots, one for each facet
facet_plots <- peco_w_area |>
  group_split(name) |>
  map(~ {
    # browser()

    # need all ecoregions to be present so we can combine legend later
    all_fills <- .x |> right_join(peco_w_area |>
      as_tibble() |>
      select(ecoregion_perc_label, US_L3NAME))
    baseplot <- all_fills |>
      # st_transform(3310) |>
      ggplot() +
      geom_sf(aes(
        fill = US_L3NAME, # ecoregion_perc_label,
        geometry = geom
      )) +
      annotation_scale(
        location = .x$scale_position |> unique(), # "bl",
        width_hint = 0.2,
        line_width = .5
      ) +
      coord_sf() +
      ggtitle(unique(.x$name)) +
      theme_void() +
      theme(
        legend.position = "right",
        legend.key.size = unit(.4, "cm"),
        legend.title.position = "top", # "left",
        # legend.title = element_text(angle = 90, size = 9),
        legend.title = element_text(size = 9),
        legend.text = element_text(size = 7),
        panel.border = element_rect(color = "black", linewidth = 2, fill = NA),
        panel.background = element_rect(fill = "white", color = NA)
      ) +
      labs(fill = "Level 3 Ecoregion") #+
    # guides(colour = guide_legend(nrow = 2))
    # scale_fill_discrete(labels = function(x) str_wrap(x, width = 50))
  })



# Combine the plots using patchwork
# combined_plot <- guide_area() /
#   wrap_plots(
#     facet_plots,
#     ncol = 1
#   ) + plot_layout(
#     guides = "collect",
#     heights = c(1, 5, 5, 5)
#   )

combined_plot <-
  facet_plots[[1]] /
  facet_plots[[2]] /
  facet_plots[[3]] /
  plot_spacer() /
  guide_area() +
  plot_layout(
    guides = "collect",
    heights = c(6, 4, 1.75, .2, 2)
  )
# combined_plot

sm_img <- ggdraw() + draw_image("results/santa_monica.jpg")
mm_img <- ggdraw() + draw_image("results/marble_mts.jpg")
mt_img <- ggdraw() + draw_image("results/Mt_tam.jpeg")
images <- mm_img /
  mt_img /
  sm_img /
  plot_spacer() + plot_layout(heights = c(1, 1, 1, .5))


master_plot <- statemap + combined_plot + images +
  plot_layout(
    widths = c(1.2, .75, .3),
    # heights = c(1, 1),
    ncol = 3
  )

ggsave("results/study_area_L3.pdf", master_plot, width = 11, height = 7, units = "in")

# Now annotate
ann_text <- peco_w_area |>
  group_split(name) |>
  map(~ {
    paste0(
      paste("Area:", unique(.x$place_areakm2) |> round(0), "km²"),
      "\n",
      sprintf("Elev. range: %sm - %sm", .x$alt_min, .x$alt_max),
      "\n",
      sprintf("Elev. median: %sm", .x$alt_median)
    )
  })

names(ann_text) <- peco_w_area$name |> unique()

master_ann_plot <- ggdraw() +
  draw_plot(master_plot) + # Place the patchwork figure
  draw_text(
    ann_text[["Marble Mountains"]],
    x = 0.62, y = 0.9, hjust = 1, size = 8,
    fontface = "bold", color = "blue"
  ) +
  draw_text(
    ann_text[["Mount Tamalpais"]],
    x = 0.62, y = 0.52, hjust = 1, size = 8,
    fontface = "bold", color = "blue"
  ) +
  draw_text(
    ann_text[["Santa Monica Mountains"]],
    x = 0.62, y = 0.25, hjust = 1, size = 8,
    fontface = "bold", color = "blue"
  )
ggsave("results/study_area_L3_ann.pdf", master_ann_plot, width = 11, height = 7, units = "in")




# mt. tam image: https://commons.wikimedia.org/wiki/File:Matt_Davis_Trail_Mt_Tamalpais_%28159382381%29.jpeg
# santa monica image https://commons.wikimedia.org/wiki/File:Backbone_Trail_near_Mishe_Mokwa.jpg
# marble mountains image: https://commons.wikimedia.org/wiki/File:Trinity_Alps_Wilderness_with_Pinus_balfouriana.jpg
