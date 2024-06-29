library(tidyverse)
library(sf)
library(RPostgreSQL)
library(rpostgis)

con <- dbConnect(RPostgreSQL::PostgreSQL(),
                 host = "flor",
                 port = 5432,
                 user = "postgres",
                 password = "postgres"
)

# Do this to access schemas
con %>% dbExecute("
SET search_path TO new_phytologist, public;
")



# con %>% dbExecute("CREATE SCHEMA new_phytologist;")

grid_res <- .25

# Make polygon 'grid' for all of California
con %>% dbExecute(
    sprintf(
            "
    DROP TABLE IF EXISTS new_phytologist.tempras;
    
    SELECT gcol, grow, geom
    INTO new_phytologist.tempras
    FROM ST_RegularGrid(
        (select ST_UNION(geom) geom from ca_layers_sub where category='County'),
        -- Set resolution in degrees
        %s, %s,
        FALSE);
    
    ALTER TABLE tempras ADD COLUMN id SERIAL PRIMARY KEY;
    CREATE INDEX tempras_idx ON tempras USING GIST(geom);
    ", grid_res, grid_res
    )
)


# Pre-selecting like with the 'with' clause totally saved this
con %>% dbExecute(
    "
DROP TABLE IF EXISTS new_phytologist.gbif_pts;

-- Get species points that meet criteria
CREATE TABLE new_phytologist.gbif_pts AS
SELECT ca_core.geom, species, basisofrecord, institutioncode
FROM ca_core
NATURAL JOIN ca_species
NATURAL JOIN ca_extra
-- NATURAL JOIN ca_crosswalks
-- JOIN ca_layers ON countyid=ca_layers.id
WHERE (coordinateuncertaintyinmeters < 500 OR coordinateuncertaintyinmeters is NULL)
-- AND ca_layers.name = 'Napa'
AND species IS NOT NULL
AND kingdom = 'Plantae'
AND NOT species = '';

CREATE INDEX gbifpts_idx ON gbif_pts USING GIST(geom);
"
)

# Now populate grid with counts of preserved_specimen and human_observation obs
# per grid cell
con %>% dbExecute(
    "
DROP TABLE IF EXISTS new_phytologist.multi_heatmap;

CREATE TABLE new_phytologist.multi_heatmap AS
SELECT
    grid.id,
    COUNT(CASE WHEN basisofrecord = 'PRESERVED_SPECIMEN' THEN 1 END) AS specimen_cnt,
    COUNT(CASE WHEN basisofrecord = 'HUMAN_OBSERVATION' THEN 1 END) AS observation_cnt,
    grid.geom
FROM
    new_phytologist.gbif_pts,
    tempras AS grid
WHERE
    st_intersects(gbif_pts.geom, grid.geom)
GROUP BY
    grid.id;
"
)

multi_heatmap <- con %>% st_read(query = "SELECT * FROM new_phytologist.multi_heatmap")

ca.sf <- st_read("~/Data/Boundaries/Political/CA/", quiet = T) %>%
    filter(NAME == "California")

multi_heatmap_cleaned <- multi_heatmap %>%
    mutate(herbarium_iNat_ratio = specimen_cnt / observation_cnt) %>%
    filter(is.finite(herbarium_iNat_ratio)) %>%
    st_intersection(ca.sf)

multi_heatmap_cleaned %>%
    write_sf(paste0("/home/ahill/Projects/random-requests/random-requests/code/new-phytologist-maps/herbrat_",
                    grid_res %>% str_replace("[.]", "_"),
                    "heatmap.gpkg"),
             overwrite = T
    )

multi_heatmap_cleaned %>%
    st_transform(3310) %>%
    ggplot() +
    geom_sf(aes(fill = herbarium_iNat_ratio), color = NA) +
    coord_sf() +
    # scale_fill_viridis_c(option = "plasma", midpoint = 1) +
    scale_fill_gradient2(
        trans = "log",
        # low = "#1b9e77",
        low = "#762a83",
        mid = "#f7f7f7",
        # high = "#d95f02",
        high = "#e66101",
        midpoint = 1,
        breaks = c(0.001, 0.1, 1, 10, 1000), labels = c(0.001, 0.1, 1, 10, 1000)
    ) +
    labs(fill = "Log of Ratio Herbarium/iNat") +
    # scale_fill_continuous(trans = "log", name = "Log Ratio") +
    theme_minimal()

# TODO


con %>% dbExecute(
    "
WITH species_observations AS (
    -- Selecting relevant columns from gbif_pts and joining with grid based on spatial intersection
    SELECT
        grid.id,
        gbif_pts.species,
        gbif_pts.basisofrecord
    FROM
        gbif_pts
    JOIN
        tempras AS grid ON st_intersects(gbif_pts.geom, grid.geom)
),
preserved_species AS (
    -- Filtering species observed as PRESERVED_SPECIMEN but not as HUMAN_OBSERVATION
    SELECT
        id,
        species
    FROM
        species_observations
    GROUP BY
        id, species
    HAVING
        COUNT(DISTINCT CASE WHEN basisofrecord = 'PRESERVED_SPECIMEN' THEN 1 END) > 0
        AND COUNT(DISTINCT CASE WHEN basisofrecord = 'HUMAN_OBSERVATION' THEN 1 END) = 0
),
observed_species AS (
    -- Filtering species observed as HUMAN_OBSERVATION but not as PRESERVED_SPECIMEN
    SELECT
        id,
        species
    FROM
        species_observations
    GROUP BY
        id, species
    HAVING
        COUNT(DISTINCT CASE WHEN basisofrecord = 'HUMAN_OBSERVATION' THEN 1 END) > 0
        AND COUNT(DISTINCT CASE WHEN basisofrecord = 'PRESERVED_SPECIMEN' THEN 1 END) = 0
)
SELECT
    grid.id,
    COUNT(DISTINCT ps.species) AS unique_preserved_species_count, -- Counting distinct preserved species
    COUNT(DISTINCT os.species) AS unique_observed_species_count, -- Counting distinct observed species
    grid.geom
FROM
    tempras AS grid
LEFT JOIN
    preserved_species ps ON grid.id = ps.id -- Joining with preserved_species to count unique preserved species
LEFT JOIN
    observed_species os ON grid.id = os.id -- Joining with observed_species to count unique observed species
GROUP BY
    grid.id, grid.geom; -- Grouping by grid id and geometry to aggregate counts

"
)
