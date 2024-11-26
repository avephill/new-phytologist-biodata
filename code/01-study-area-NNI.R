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


con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("
CREATE OR REPLACE VIEW target AS
SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/shiny-aoi-exploration/occurrences.parquet');")

con |> dbExecute("
CREATE OR REPLACE VIEW places AS
SELECT * -- EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/shiny-aoi-exploration/place_boundaries.parquet');")

places_prep <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom", crs = 4326)

# fix one tam
places <- places_prep |>
  filter(name != "One Tam") |>
  bind_rows(places_prep |> filter(name == "One Tam") |>
    st_cast("POLYGON") |>
    st_make_valid() %>%
    mutate(area = st_area(.)) |>
    slice_max(area, n = 2) |>
    group_by(name) |>
    summarise(geom = st_union(geom)))

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
  annotation_map_tile("cartolight", zoom = 6) + # add basemap layer
  geom_sf(data = places_box, fill = NA, linewidth = 3, color = "red") +
  coord_sf(xlim = c(-124.48, -114.13), ylim = c(32.53, 42.01), crs = 4326)

statemap

densityMaker <- function(input_occ, place_name) {
  place_owin <- places |>
    st_transform(3310) |>
    filter(name == place_name) |>
    st_geometry() |>
    as.owin()

  as.ppp(
    input_occ |>
      st_transform(3310) |>
      st_filter(st_as_sf(place_owin) |> st_set_crs(3310)),
    W = place_owin
  ) |>
    density.ppp(dimyx = 200) |>
    st_as_stars() |>
    st_set_crs(3310) |>
    st_as_sf() |>
    mutate(geom_text = st_as_text(geometry))
}

fullDensCompiler <- function(full_occ, place_name) {
  # browser()
  inat_tamdens <- densityMaker(full_occ |> filter(basisofrecord == "HUMAN_OBSERVATION"), place_name)

  herb_tamdens <- densityMaker(full_occ |> filter(basisofrecord == "PRESERVED_SPECIMEN"), place_name)

  all_tamdens <- densityMaker(full_occ, place_name)

  joinem <- all_tamdens |>
    rename(sum_vals = v) |>
    left_join(inat_tamdens |> as_tibble() |>
      select(inat_vals = v, geom_text), by = "geom_text") |>
    left_join(herb_tamdens |> as_tibble() |>
      select(herb_vals = v, geom_text), by = "geom_text") |>
    mutate(place_name = place_name)
}

full_dens <- map(places |> pull(name), function(x) fullDensCompiler(full_occ, x)) |> bind_rows()

# For recentering
recenter_point <- st_sfc(st_point(c(-120, 30)), crs = st_crs(full_dens))
recenter_vector <- places |>
  group_by(name) %>%
  mutate(recenter_geom = (st_coordinates(recenter_point) - st_coordinates(st_centroid(st_union(geom))))) |>
  ungroup() |>
  as_tibble() |>
  select(place_name = name, recenter_geom)


full_dens_long <- full_dens |>
  mutate(diff_vals = herb_vals - inat_vals) |>
  pivot_longer(
    cols = c(sum_vals, inat_vals, herb_vals, diff_vals),
    names_to = "variable",
    values_to = "value"
  ) |>
  mutate(sqrt_value = sqrt(value)) |>
  left_join(recenter_vector) %>%
  # rowwise() |>
  mutate(recenter_geom = geometry + recenter_geom)

full_dens_long2 <- full_dens |>
  mutate(diff_vals = herb_vals - inat_vals) |>
  pivot_longer(
    cols = c(sum_vals, inat_vals, herb_vals, diff_vals),
    names_to = "variable",
    values_to = "value"
  ) |>
  mutate(sqrt_value = sqrt(value)) |>
  left_join(recenter_vector) %>%
  # rowwise() |>
  mutate(recenter_geom = geometry + recenter_geom)


# joinem |> filter((inat_vals + herb_vals) != sum_vals)

tam_most_plot <- ggplot() +
  geom_sf(
    data = full_dens_long |> filter(variable != "diff_vals") |>
      st_set_geometry("recenter_geom"),
    aes(fill = sqrt_value), color = NA
  ) +
  theme_void() +
  theme(
    plot.background = element_rect(fill = "white") # ,
  ) +
  scale_fill_viridis_c(option = "magma", guide = "none") +
  facet_grid(vars(place_name), vars(variable))

tam_diff_plot <- ggplot() +
  geom_sf(data = full_dens_long |> filter(variable == "diff_vals") |>
    st_set_geometry("recenter_geom"), aes(fill = value), color = NA) +
  theme_void() +
  theme(
    plot.background = element_rect(fill = "white") # ,
  ) +
  scale_fill_gradient2(
    low = "blue", # Color for low values
    mid = "white", # Color for midpoint
    high = "red", # Color for high values
    midpoint = 0, # Center the scale at 0
    guide = guide_colorbar(title = "Difference") # Add legend title
  ) +
  facet_grid(vars(place_name), vars(variable))
# tam_plot

combined_plot <- plot_grid(
  tam_most_plot,
  tam_diff_plot,
  ncol = 2, # Arrange in 2 columns
  # labels = c("A", "B"), # Add labels if desired
  align = "v" # , # Align both horizontally and vertically
  # axis = "tb"
)

ggsave("results/density_plots.png", combined_plot, width = 7, height = 4)

# tam <- places |>
#   filter(name == "One Tam") |>
#   st_make_valid()

# tam_grid <- tam |>
#   st_make_grid(n = c(20, 20)) |>
#   st_intersection(tam) |>
#   st_as_sf() %>%
#   mutate(grid_id = 1:n()) |>
#   st_make_valid() |>
#   st_join(tam_occ |> mutate(present = 1)) %>%
#   group_by(grid_id) %>%
#   summarize(`Obs. count` = sum(present))

# tam_plot <- ggplot() +
#   geom_sf(data = tam_grid, aes(fill = `Obs. count`)) +
#   theme_void() +
#   theme(
#     panel.border = element_rect(color = "red", linewidth = 4, fill = NA), # Red border
#     legend.position = "top"
#   )

ggdraw(statemap) +
  draw_plot(
    {
      tam_plot
    },
    # The distance along a (0,1) x-axis to draw the left edge of the plot
    x = 0.52,
    # The distance along a (0,1) y-axis to draw the bottom edge of the plot
    y = .3,
    # The width and height of the plot expressed as proportion of the entire ggdraw object
    width = 0.46,
    height = 0.46
  )



# # Trails ---------------------------------------
# install.packages("osmdata")
# library(osmdata)

# x <- st_read("~/Data/Transportation/NPS_-_Trails_-_Geographic_Coordinate_System/Public_Trails_dd_nad.shp")
