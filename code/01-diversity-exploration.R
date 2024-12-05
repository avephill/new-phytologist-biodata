library(googlesheets4)
library(tidyverse)
library(duckdb)
library(duckdbfs)
library(sf)

gs4_auth()

species_master <- read_sheet("https://docs.google.com/spreadsheets/d/19vRvSHKxrfHRVe0jgI3AncQyib1B1NBmDQ5-s3OKs5o/edit?gid=0#gid=0")

verified_tam <- species_master |> filter(verifiedStatus_TAM == "verified")

con <- dbConnect(duckdb())
con |> dbExecute("INSTALL spatial; LOAD spatial;")

con |> dbExecute("
CREATE OR REPLACE VIEW target AS
SELECT * EXCLUDE geom, ST_Point(decimallongitude, decimallatitude) AS geom
FROM read_parquet('~/Projects/new-phytologist/data/occurrences.parquet');")

tam_occ <- con |>
  tbl("target") |>
  filter(
    species %in% verified_tam$species,
    place_name == "One Tam",
    basisofrecord %in% c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN")
  ) |>
  to_sf(conn = con)

tam_occ |>
  mutate(
    source_type = case_when(
      basisofrecord == "HUMAN_OBSERVATION" ~ "iNaturalist",
      basisofrecord == "PRESERVED_SPECIMEN" ~ "Herbarium"
    )
  ) |>
  ggplot() +
  geom_sf(aes(color = source_type), shape = 5) +
  facet_wrap(~source_type)

tam_occ |>
  as_tibble() |>
  group_by(basisofrecord) |>
  summarise(N_occ = n())

tam_occ |>
  as_tibble() |>
  group_by(basisofrecord) |>
  distinct(species) |>
  summarise(N_spec = n())


unique_species <- tam_occ %>%
  as_tibble() |>
  group_by(species) %>%
  summarize(
    count_human_obs = sum(basisofrecord == "HUMAN_OBSERVATION"),
    count_preserved_specimen = sum(basisofrecord == "PRESERVED_SPECIMEN")
  ) %>%
  ungroup() %>%
  mutate(
    unique_to_human_obs = count_human_obs > 0 & count_preserved_specimen == 0,
    unique_to_preserved_specimen = count_preserved_specimen > 0 & count_human_obs == 0
  )

unique_species |>
  filter(unique_to_human_obs) |>
  count()

unique_species |>
  filter(unique_to_preserved_specimen) |>
  count()

unique_species |>
  filter(!unique_to_human_obs & !unique_to_preserved_specimen) |>
  count()

tam_occ |>
  as_tibble() |>
  distinct(species) |>
  count()

unique_species |>
  filter(unique_to_preserved_specimen) |>
  left_join(tam_occ |> as_tibble() |> select(family, species)) |>
  distinct(species, family) |>
  group_by(family) |>
  summarize(N_fam = n()) |>
  arrange(desc(N_fam)) |>
  slice_head(n = 10) |>
  ggplot() +
  geom_col(aes(y = reorder(family, -N_fam), x = N_fam)) +
  labs(title = "Unique speces to herbaria sample")


unique_species |>
  filter(unique_to_human_obs) |>
  left_join(tam_occ |> as_tibble() |> select(family, species)) |>
  distinct(species, family) |>
  group_by(family) |>
  summarize(N_fam = n()) |>
  arrange(desc(N_fam)) |>
  slice_head(n = 10) |>
  ggplot() +
  geom_col(aes(y = reorder(family, -N_fam), x = N_fam)) +
  labs(title = "Unique speces to iNat sample")

unique_species |>
  filter(unique_to_human_obs) |>
  left_join(verified_tam |>
    as_tibble() |>
    select(species, Origin)) |>
  group_by(Origin) |>
  summarise(N_origin = n()) |>
  mutate(type = "iNat")

unique_species |>
  filter(unique_to_preserved_specimen) |>
  left_join(verified_tam |>
    as_tibble() |>
    select(species, Origin)) |>
  group_by(Origin) |>
  summarise(N_origin = n()) |>
  mutate(type = "Herbarium")

# Unique species list
unq_prep <- unique_species |>
  filter(unique_to_preserved_specimen | unique_to_human_obs) |>
  mutate(
    `iNaturalist unique` = case_when(
      unique_to_human_obs ~ species,
      TRUE ~ NA_character_
    ),
    `Herbarium unique` = case_when(
      unique_to_preserved_specimen ~ species,
      TRUE ~ NA_character_
    )
  )

unique_species_df <- bind_cols(
  unq_prep |>
    select(`iNaturalist unique`) |>
    arrange(`iNaturalist unique`),
  unq_prep |>
    select(`Herbarium unique`) |>
    arrange(`Herbarium unique`)
) |>
  filter(!(is.na(`Herbarium unique`) & is.na(`iNaturalist unique`)))


sheet_write(unique_species_df,
  ss = "https://docs.google.com/spreadsheets/d/1sT8iHPiXsnQu9-EpbDRwTJXHaYyK-lNDI8pbCn6twgI/edit?gid=0#gid=0",
  sheet = "Sheet1"
)


# Checking collector name ---------------------------------------
tam_occ |>
  # filter(basisofrecord == "HUMAN_OBSERVATION") |>
  filter(basisofrecord == "PRESERVED_SPECIMEN") |>
  mutate(recordedby = unlist(recordedby)) |>
  group_by(recordedby) |>
  summarize(N = n()) |>
  arrange(desc(N)) |>
  View()

tam_occ |>
  # filter(basisofrecord == "HUMAN_OBSERVATION") |>
  filter(basisofrecord == "PRESERVED_SPECIMEN") |>
  mutate(
    recordedby =
      case_when(
        length(unlist(recordedby)) > 1 ~ # if there are more than 2 collectors
          recordedby |> paste(collapse = ","), # collapse them into comma separated
        T ~ recordedby
      )
  )

tam_occ |>
  filter(length(unlist(recordedby)) > 2) |>
  select(recordedby)

tam_occ |>
  mutate(recordedby = as.character(recordedby)) |>
  select(recordedby)
tam_occ |>
  head(10) |>
  pull(recordedby) |>
  unlist() |>
  length()
