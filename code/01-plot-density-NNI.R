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
FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")

con |> dbExecute("
CREATE OR REPLACE VIEW places AS
SELECT * -- EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');")

places <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom", crs = 4326)

# # fix one tam
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

# look at the tam one
# mini_tam <- places_prep |>
#   filter(name == "One Tam") |>
#   st_cast("POLYGON") |>
#   st_make_valid() %>%
#   mutate(area = st_area(.)) |>
#   slice_min(area, n = 1)

# mini_tamocc <- full_occ |> st_intersection(mini_tam)
# mini_tamocc |> distinct(species)

# TODO
# Add verified filter

full_occ <- con |>
  tbl("target") |>
  filter(
    # species %in% verified_tam$species,
    # place_name == "One Tam",
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con, crs = 4326)


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


recenter_point <- st_sfc(st_point(c(-120, 30)), crs = 4326) |> st_transform(3310)
recenter_vector <- places |>
  st_transform(3310) |>
  group_by(name) %>%
  mutate(offset_geom = (recenter_point - st_centroid(st_union(geom)))) |>
  ungroup() |>
  as_tibble() |>
  select(place_name = name, offset_geom)


full_dens_long <- full_dens |>
  mutate(diff_vals = herb_vals - inat_vals) |>
  pivot_longer(
    cols = c(sum_vals, inat_vals, herb_vals, diff_vals),
    names_to = "variable",
    values_to = "value"
  ) |>
  mutate(
    sqrt_value = sqrt(value),
    value_km2 = value * 1e6
  ) #|>
# For recentering
# left_join(recenter_vector, by = "place_name") %>%
# mutate(recenter_geom = geometry + offset_geom) |>
# st_set_geometry("recenter_geom") |>
# st_set_crs(3310) |>
# select(-c(geometry, geom_text, offset_geom))

# full_dens_long2 <- full_dens |>
#   mutate(diff_vals = herb_vals - inat_vals) |>
#   pivot_longer(
#     cols = c(sum_vals, inat_vals, herb_vals, diff_vals),
#     names_to = "variable",
#     values_to = "value"
#   ) |>
#   mutate(sqrt_value = sqrt(value)) |>
#   left_join(recenter_vector) %>%
#   # rowwise() |>
#   mutate(recenter_geom = geometry + recenter_geom)


# joinem |> filter((inat_vals + herb_vals) != sum_vals)



trails <- st_read("data/place_roads.gpkg") |>
  filter(!st_geometry_type(geom) %in% c("POLYGON", "MULTIPOLYGON")) |>
  st_filter(places)
# trails_grouped <- trails |>
#   group_by(name) |>
#   summarize(geom = st_union(geom))

full_dens_plotready <- full_dens_long |>
  mutate(variable_label = case_when(
    variable == "diff_vals" ~ "Herbarium - iNat",
    variable == "herb_vals" ~ "Herbarium",
    variable == "inat_vals" ~ "iNat",
    variable == "sum_vals" ~ "Herbarium + iNat"
  ))

facet_plots <- full_dens_plotready |>
  filter(variable != "diff_vals") |>
  group_split(place_name, variable_label) |>
  map(~ {
    tam_most_plot <- ggplot() +
      geom_sf(
        data = .x,
        aes(fill = value_km2), color = NA
      ) +
      geom_sf(
        data = trails |>
          filter(name == unique(.x$place_name)),
        color = "white",
        linewidth = .05 # , size = .01
      ) +
      theme_void() +
      theme(
        plot.background = element_rect(fill = "white"),
        panel.background = element_rect(fill = "white", color = "white"),
        legend.position = "right",
        legend.key.size = unit(.2, "cm"), # Increase size of legend keys
        legend.text = element_text(size = 5), # Increase size of legend text
        legend.title = element_text(size = 7),
        plot.margin = margin(10, 10, 10, 10) # Add margin around each panel
      ) +
      coord_sf() +
      scale_fill_viridis_c(option = "magma") +
      # facet_grid(cols = vars(variable_label)) +
      labs(
        fill = expression("Obs/km"^2),
        title = .x$variable_label
      )
  })


# x <- plot_grid(
#   plotlist = facet_plots,
#   # rel_widths = c(2, 1, 1),
#   nrow = 3, ncol = 3

#   # labels = "auto"
# )
# ggsave("results/testing.png", x, width = 12, height = 12, units = "in")

# combined_plot <- wrap_plots(
#   facet_plots,
#   nrow = 3
# )
# combined_plot


# ggsave("results/density_plots1.png", combined_plot, width = 7, height = 4)

diff_plots <-
  full_dens_plotready |>
  filter(variable == "diff_vals") |>
  group_split(place_name) |>
  map(~ {
    tam_diff_plot <- ggplot() +
      geom_sf(
        data = .x,
        aes(fill = value), color = NA
      ) +
      geom_sf(
        data = trails |>
          filter(name == unique(.x$place_name)),
        color = "white",
        linewidth = .05 # , size = .01
      ) +
      theme_void() +
      theme(
        plot.background = element_rect(fill = "white"),
        panel.background = element_rect(fill = "white", color = "white"),
        legend.position = "right",
        legend.key.size = unit(.2, "cm"), # Increase size of legend keys
        legend.text = element_text(size = 5), # Increase size of legend text
        legend.title = element_text(size = 7),
        plot.margin = margin(10, 10, 10, 10) # Add margin around each panel
      ) +
      # scale_fill_gradient2(
      #   low = "blue", # Color for low values
      #   mid = "white", # Color for midpoint
      #   high = "red", # Color for high values
      #   midpoint = 0, # Center the scale at 0
      #   guide = guide_colorbar(title = "Difference (Herb - iNat)") # Add legend title
      # ) +
      scale_fill_gradient2(
        low = "#D73027", # Red
        mid = "#4575B4", # Neutral blue
        high = "#1A9850", # Green
        midpoint = 0,
        guide = guide_colorbar(title = "Difference (Herb - iNat)") # Add legend title
      ) +
      labs(
        fill = expression("Obs/km"^2),
        title = .x$variable_label
      ) +
      coord_sf()
  })


# Define target positions for diff_plots
target_positions <- c(4, 8, 12)
total_length <- 12
# Create big_plotlist using map()
big_plotlist <- map(seq_len(total_length), function(i) {
  if (i %in% target_positions) {
    # Insert from diff_plots
    diff_plots[[which(target_positions == i)]]
  } else {
    # Insert from facet_plots
    facet_plots[[i - sum(i > target_positions)]]
  }
})
x <- plot_grid(
  plotlist = big_plotlist,
  # rel_widths = c(2, 1, 1),
  nrow = 3, ncol = 4

  # labels = "auto"
)



ggsave("results/density_plots.png", x, width = 12, height = 10)

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
