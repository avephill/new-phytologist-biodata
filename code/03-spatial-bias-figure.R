# Make spatial bias figure
# relies directly on analysis from 02-nearest-road and 01-plot-density-NNI


# Count Rasters ---------------------------------------
library(terra)
library(tidyterra)
library(duckdb)
library(sf)
library(tidyverse)
library(ggspatial)
library(patchwork)
library(ggsignif)

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("
CREATE OR REPLACE TABLE occ AS
SELECT *
FROM st_read('data/verified_occurrences.gpkg');
")

con |> dbExecute("
CREATE OR REPLACE VIEW places AS
SELECT * -- EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/place_boundaries.parquet');")

places <- con |>
  tbl("places") |>
  collect() |>
  st_as_sf(wkt = "geom", crs = 4326)

# So that all have same extent
template_raster <- con |>
  tbl("occ") |>
  mutate(
    latitude = round(decimallatitude, 2),
    longitude = round(decimallongitude, 2)
  ) |>
  count(
    longitude, latitude
  ) |>
  collect() |>
  rast(crs = "epsg:4326") |>
  classify(cbind(NA, 0))

makeCARaster <- function(data_source, template) {
  rast_occ <- con |>
    tbl("occ") |>
    filter(
      # place_name == place_name,
      basisofrecord %in% data_source
    ) |>
    mutate(
      latitude = round(decimallatitude, 2),
      longitude = round(decimallongitude, 2)
    ) |>
    count(
      longitude, latitude
    ) |>
    collect()

  count_rast <- rast_occ |>
    rast(crs = "epsg:4326", extent = ext(template)) |>
    classify(cbind(NA, 0))

  return(count_rast)
}

inat_rast <- makeCARaster("HUMAN_OBSERVATION", template_raster)
herb_rast <- makeCARaster("PRESERVED_SPECIMEN", template_raster)
ca_stack <- c(inat_rast, herb_rast)
# names(ca_stack) <- c("inat_n", "herb_n")

# create list of rasters
rast_cnt_list <- places |>
  group_split(name) |>
  map(function(place) {
    ca_stack |> crop(place, mask = T)
  })


rast_cnt_nest <- rast_cnt_list |>
  # Unstack and combine all layers into a single list using map
  map(function(r) {
    # browser()
    r |> as.list()
  })

# Flatten it out into the right format
# Feel weird about hardcoding this, but whatever
rast_cnt_flat <- list(
  rast_cnt_nest[[1]][[1]], rast_cnt_nest[[2]][[1]], rast_cnt_nest[[3]][[1]],
  rast_cnt_nest[[1]][[2]], rast_cnt_nest[[2]][[2]], rast_cnt_nest[[3]][[2]]
)

names(rast_cnt_flat) <- places |>
  arrange(name) |>
  pull(name) |>
  rep(2)

# mm_minmax <- minmax(rast_cnt_list[[1]])
fill_max_values <- c(
  "Santa Monica Mountains" = 40,
  "One Tam" = 50,
  "Marble/Salmon Mountains" = 25
)

# Now make list of plots

rast_cnt_plots <-
  map(seq(1:length(rast_cnt_flat)), function(i) {
    # browser()
    place_outline <- places |> filter(name == names(rast_cnt_flat)[[i]])
    bbox <- st_bbox(place_outline)

    rast_cnt_flat[[i]] |>
      mutate(across(contains("n"), ~ sqrt(.), .names = "{.col}_sqrt")) |>
      ggplot() +
      geom_sf(
        data = place_outline,
        color = "white", fill = NA
      ) +
      geom_tile(aes(x = x, y = y, fill = n_sqrt), color = NA) +
      # geom_tile(aes(x = x, y = y, fill = n_sqrt, color = n_sqrt)) +

      # guides(color = "none") +
      # scale_fill_viridis_c(option = "magma", na.value = "transparent") +
      scale_fill_gradient(
        low = "#F3F7FF",
        # high = "#422040",
        # high = "#CC2936",
        high = "#023DC6",
        # high = "#F87575",
        na.value = "transparent",
        limits = c(0, fill_max_values[place_outline$name]) # Set manual min and max for the legend
      ) +
      theme_minimal() +
      scale_x_continuous(
        breaks = seq(bbox["xmin"], bbox["xmax"], length.out = 3) # Divide x-axis into 5 ticks
      ) +
      scale_y_continuous(
        breaks = seq(bbox["ymin"], bbox["ymax"], length.out = 4) # Divide x-axis into 5 ticks
      ) +
      # theme_void() +
      theme(
        # plot.background = element_rect(fill = "white"),
        # panel.background = element_rect(fill = "white", color = "white"),
        # legend.position = "right",
        legend.position = if (i %in% c(1, 2, 3)) "inside" else "none",
        legend.position.inside = if (i %in% c(1, 2, 3)) c(0, 0) else 0,
        # legend.position = "inside", legend.position.inside = c(.8, .8),
        legend.key.size = unit(.5, "cm"), # Increase size of legend keys
        legend.text = element_text(size = 10), # Increase size of legend text
        legend.title = element_text(size = 13),
        axis.text = element_blank()
        # plot.margin = margin(10, 10, 10, 10) # Add margin around each panel
      ) +
      coord_sf(xlim = c(bbox["xmin"], bbox["xmax"]), ylim = c(bbox["ymin"], bbox["ymax"])) +
      # facet_grid(cols = vars(variable_label)) +
      labs(
        fill = expression(sqrt("Obs/km"^2)),
        x = element_blank(), y = element_blank()
      )
  })
# rast_cnt_flat[[3]] |> values()
rast_cnt_plots[[2]]

# rast_cnt_plots |> wrap_plots(ncol = 2)


## Trails  ---------------------------------------
trails <- st_read("data/place_roads.gpkg")

trail_plots <- trails |>
  group_split(name) |>
  map(function(place) {
    # browser()
    place_outline <- places |> filter(name == unique(place$name))
    bbox <- st_bbox(place_outline)
    ggplot() +
      geom_sf(
        data = place_outline,
        fill = "#ECECEC",
        color = "#ECECEC", fill = NA # ,
        # linetype = "longdash"
      ) +
      geom_sf(
        data = place # ,
        # aes(color = highway)
      ) +
      scale_x_continuous(
        breaks = seq(bbox["xmin"], bbox["xmax"], length.out = 3), # Divide x-axis into 5 ticks
        labels = function(x) paste0(round(x, 1), "°W") # Round labels to the first decimal point
      ) +
      scale_y_continuous(
        breaks = seq(bbox["ymin"], bbox["ymax"], length.out = 4), # Divide x-axis into 5 ticks
        labels = function(x) paste0(round(x, 1), "°N") # Round labels to the first decimal point
      ) +
      theme_minimal() +
      labs(color = "OSM Road Type") +
      coord_sf(xlim = c(bbox["xmin"], bbox["xmax"]), ylim = c(bbox["ymin"], bbox["ymax"])) +
      annotation_scale(
        location = "bl",
        width_hint = .3
        # line_width = .5
      )
  })



trail_lengths <- trails |>
  st_cast("LINESTRING") |>
  group_by(name) |>
  mutate(length = st_length(geom) |> units::set_units("km")) %>% # Add a new column with line lengths
  summarize(total_length = sum(length)) |>
  as_tibble() |>
  select(-geom)

trail_lengths |> write_csv("results/spatial_bias_a-c.csv")


# NNI ---------------------------------------
nni_figuredf <- read_csv("results/nni_results.csv") |>
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
  )

nni_plots <- nni_figuredf |>
  group_split(place_name) |>
  map(function(x) {
    # browser()
    x |>
      ggplot(aes(x = basisofrecord, y = log(NNI), fill = basisofrecord)) +
      geom_hline(yintercept = 0, linetype = "longdash", color = "darkgrey", linewidth = 1) +
      geom_boxplot(
        outlier.colour = "black", outlier.shape = 16,
        outlier.size = 1.5, # color = "black",
        width = 0.6
      ) + # Customize the boxplot appearance
      scale_fill_manual(values = c("Herbarium" = "#2F4F4F", "iNaturalist" = "#FFB90F")) + # Custom fill colors
      # scale_color_manual(values = c("Herbarium" = "darkslategray", "iNaturalist" = "darkgoldenrod1")) + # Custom fill colors

      # stat_summary(
      #   fun = median,
      #   geom = "line",
      #   aes(group = basisofrecord),
      #   color = "black",
      #   size = 1.2,
      #   linetype = "solid"
      # ) +
      geom_signif(
        comparisons = list(c("Herbarium", "iNaturalist")),
        map_signif_level = TRUE,
        test = "wilcox.test",
        y_position = 3, # Adjust y position of the significance annotation
        textsize = 6, # Increase size of p-value text
        tip_length = 0.02 # Adjust the line length for significance
      ) +
      labs(x = element_blank(), y = "Log(NNI)") + # Add axis labels
      theme_bw() + # Clean theme
      theme(
        # axis.text.x = element_text(angle = 45, hjust = 1), # Rotate x-axis text for better readability
        axis.title = element_text(size = 14, face = "bold"),
        axis.text = element_text(size = 12),
        legend.position = "none" # Remove the legend (since it's already clear from axis labels)
      ) +
      ylim(-2.5, 4)
  })

# nni_plots |> wrap_plots(ncol = 3)

nni_sum_df <- nni_figuredf |>
  mutate(log_NNI = log(NNI)) |>
  group_split(place_name) |>
  map_df(function(x) {
    # browser()
    herb_nni <- x |>
      filter(basisofrecord == "Herbarium") |>
      pull(log_NNI)

    inat_nni <- x |>
      filter(basisofrecord == "iNaturalist") |>
      pull(log_NNI)

    wctest <- wilcox.test(
      herb_nni,
      inat_nni
    )

    # if (!is.finite(mean(herb_nni))) {
    #   browser()
    # }

    tibble(
      place_name = x |> pull(place_name) |> unique(),
      pvalue = wctest[[3]],
      herb_nni = herb_nni[which(is.finite(herb_nni))] |> mean(),
      inat_nni = mean(inat_nni, na.rm = T)
    )
  })
nni_sum_df |> write_csv("results/spatial_bias_j-l.csv")

# Nearest Road ---------------------------------------

# library(jsonlite)
# random_trail <- read_csv("data/rand_nearest_trail.csv")
nearest_trail <- read_csv("data/occ_nearest_trail.csv") # |>
# bind_rows(random_trail)


nt_figready <- nearest_trail |>
  mutate(
    place_name = case_when(
      place_name == "Marble/Salmon Mountains" ~ "Marble Mountains",
      place_name == "One Tam" ~ "Mount Tamalpais",
      T ~ place_name
    ),
    basisofrecord = case_when(
      basisofrecord == "HUMAN_OBSERVATION" ~ "iNaturalist",
      basisofrecord == "PRESERVED_SPECIMEN" ~ "Herbarium",
      basisofrecord == "random" ~ "Random",
      T ~ basisofrecord
    )
  )

# Perform GLM for each site
glm_results <- nt_figready |>
  group_split(place_name) |>
  map(function(site_data) {
    site_name <- unique(site_data$place_name) # Extract site name
    glm_model <- glm(distance ~ basisofrecord, data = site_data)
    ci <- confint(glm_model)
    # browser()
    # Calculate estimated means for each level of basisofrecord
    herb_mean <- glm_model$coefficients[["(Intercept)"]]
    inat_effect <- glm_model$coefficients[["basisofrecordiNaturalist"]]
    inat_mean <- herb_mean + inat_effect
    herb_ci <- ci["(Intercept)", ]
    inat_ci <- ci["basisofrecordiNaturalist", ] + herb_mean
    pvalue <- summary(glm_model)$coefficients[["basisofrecordiNaturalist", "Pr(>|t|)"]]

    meanci_df <- bind_rows(
      tibble(
        basisofrecord = "iNaturalist", mean = inat_mean,
        ci_2.5 = inat_ci[1], ci_97.5 = inat_ci[2]
      ),
      tibble(
        basisofrecord = "Herbarium", mean = herb_mean,
        ci_2.5 = herb_ci[1], ci_97.5 = herb_ci[2]
      )
    ) |>
      mutate(pvalue = pvalue)

    list(
      site_name = site_name,
      model = glm_model,
      summary = summary(glm_model),
      mean_ci = meanci_df
    )
  })

# x <- glm_results[[1]]
# x$mean_ci
# x$summary
# glm_results[[]]

# x$mean_ci
# x$ci

neartrail_plots <- nt_figready |>
  group_split(place_name) |>
  map(function(x) {
    # browser()

    # Pulls GLM results for each place
    group_glm <- glm_results |>
      keep(~ .x$site_name == x$place_name |> unique())
    group_ci <- group_glm[[1]]$mean_ci

    group_ci_wide <- group_ci |>
      pivot_wider(
        names_from = basisofrecord, # Use 'basisofrecord' for new column names
        values_from = c(mean, ci_2.5, ci_97.5), # Values to spread across columns
        names_glue = "{.value}_{basisofrecord}" # Custom names
      )


    # browser()
    # group_glm[[1]]$summary
    x |> ggplot() +
      # geom_boxplot(aes(x = basisofrecord, y = distance)) +
      geom_density(aes(x = distance, fill = basisofrecord, color = basisofrecord), alpha = 0.6) +
      guides(color = "none", fill = "none") +
      scale_fill_manual(values = c("Herbarium" = "darkslategray", "iNaturalist" = "darkgoldenrod1", "Random" = "lightgrey")) +
      scale_color_manual(values = c("Herbarium" = "darkslategray", "iNaturalist" = "darkgoldenrod1", "Random" = "black")) +
      # facet_wrap(~place_name, scales = "free") +# Add vertical lines for the mean
      geom_vline(
        data = group_ci, aes(xintercept = mean, color = basisofrecord),
        linetype = "solid", linewidth = .5
      ) +
      # Add a shaded area between confidence intervals
      geom_rect(
        data = group_ci,
        aes(
          xmin = ci_2.5, xmax = ci_97.5,
          ymin = 0, ymax = Inf,
          fill = basisofrecord
        ),
        alpha = 0.2, inherit.aes = FALSE
      ) +
      # Significance annotation
      geom_signif(
        data = group_ci_wide,
        inherit.aes = FALSE,
        manual = TRUE,
        aes(
          # # group = group,
          xmin = mean_iNaturalist,
          xmax = mean_Herbarium,
          annotations = ifelse(pvalue < 0.001, "***",
            ifelse(pvalue < 0.01, "**",
              ifelse(group_ci_widevalue < 0.05, "*", "ns")
            )
          )
        ),
        y_position = max(density(x$distance)$y, na.rm = TRUE) * .85,
        tip_length = 0.01, textsize = 5
      ) +
      # Add vertical lines for confidence intervals
      # geom_vline(
      #   data = group_ci, aes(xintercept = ci_2.5, color = basisofrecord),
      #   linetype = "dotted", linewidth = 0.8
      # ) +
      # geom_vline(
      #   data = group_ci, aes(xintercept = ci_97.5, color = basisofrecord),
      #   linetype = "dotted", linewidth = 0.8
      # ) +
      labs(y = "Density", x = "Distance to nearest road (m)") +
      theme_bw()
  })

neartrail_plots |> wrap_plots(ncol = 3)
glm_results[[3]]$summary$coefficients

glm_results_df <- glm_results |>
  map_df(function(x) {
    x$mean_ci |> mutate(site_name = x$site_name)
  })

glm_results_df |> write_csv("results/spatial_bias_m-o.csv")


# Master Plot ---------------------------------------
# Now stitch em up

master_plot <- list(trail_plots, rast_cnt_plots, nni_plots, neartrail_plots) |>
  list_flatten() |>
  wrap_plots(ncol = 3, heights = 1) +
  plot_annotation(tag_levels = "a") &
  theme(plot.tag = element_text(size = 16, face = "bold"))

ggsave("results/spatial_bias.pdf", master_plot, width = 15, height = 18, units = "in")
