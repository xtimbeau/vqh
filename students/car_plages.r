library(arrow)
library(tidyverse)
library(sf)
library(geoarrow)
library(tmap)
people <- open_dataset("data4ws/c200_amp.parquet") |> 
  st_as_sf() |> 
  filter(str_detect(INSEE_COM, "^132"))
tmap_mode("view")

plages <- st_read("students/marseille_bases_nautiques_plages_2020.geojson")
tm_shape(people)+tm_fill(fill="ind")+tm_shape(plages %>% st_buffer(200))+tm_fill("red", fill_alpha=0.5)
tm_shape(plages %>% st_buffer(200))+tm_fill()
plages <- plages %>% st_transform(3035) %>%  st_join(people %>% select(idINS), join = st_nearest_feature)

pairs <- cross_join(
  people |> rename(fromidINS = idINS) |> st_drop_geometry(),
  plages |> rename(toidINS = idINS) |> st_drop_geometry())

distances <- open_dataset("data4ws/car.parquet") |> 
  collect()
pairs <- pairs |> 
  left_join(distances, by=c("fromidINS", "toidINS")) 

spairs <- pairs %>% drop_na(tt) %>% group_by(fromidINS) %>% summarize(tt = min(tt, na.rm=TRUE)) %>% r3035::idINS2sf(idINS = "fromidINS")

tm_shape(spairs)+tm_fill(fill = "tt")

st_write(spairs %>% st_transform(4326), "plages_car.geojson")
