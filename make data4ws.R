library(geoarrow)
library(arrow)
library(sf)
library(stars)
library(sfarrow)
library(tidyverse)
library(tmap)
library(archive)
library(osmdata)

# communes ------------

curl::curl_download("https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG-CARTO/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2025-04-02/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2025-04-02.7z", 
                    destfile = "/tmp/carto.shp.7z")
cartac <- archive::archive("/tmp/carto.shp.7z") 
archive_extract("/tmp/carto.shp.7z", dir = "/tmp/")
efil <- cartac |> 
  filter(str_detect(path, '/EPCI\\.')) |>
  pull(path)
afil <- cartac |> 
  filter(str_detect(path, '/ARRONDISSEMENT_MUNICIPAL\\.')) |>
  pull(path)
cfil <-  cartac |> 
  filter(str_detect(path, '/COMMUNE\\.')) |>
  pull(path)

epci <- st_read(str_c("/tmp/", efil[efil |> str_detect("shp$")]))
amp_e <- epci |> filter(NOM |> str_detect("Aix-Marseille-Provence")) |> st_drop_geometry()
communes <- st_read(str_c("/tmp/", cfil[cfil |> str_detect("shp$")]))
arrondissements <- st_read(str_c("/tmp/", afil[afil |> str_detect("shp$")]))
amp <- communes |> 
  semi_join(amp_e, by=c("SIREN_EPCI" = "CODE_SIREN")) 
amp_c <- amp |> distinct(INSEE_COM)
amp <- amp |> 
  bind_rows(
    arrondissements |> semi_join(amp |> st_drop_geometry(), by = "INSEE_COM") |>
      mutate(STATUT="arrondissement",
             INSEE_DEP = "13",
             INSEE_REG = "93")) |> 
  mutate(INSEE_COM = ifelse(INSEE_ARM |> is.na(), INSEE_COM, INSEE_ARM)) |> 
  st_transform(3035) |> 
  mutate(ar = ifelse(INSEE_COM=="13055", FALSE, TRUE))
amp_c <- amp |> distinct(INSEE_COM)

arrow::write_parquet(amp |> select(-ID), "data4ws/communes_amp.parquet")
st_write(amp |> select(-ID) |> st_transform(3857) , dsn = "data4ws/communes_amp.geojson")

# IRIS ------------

curl::curl_download("https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/CONTOURS-IRIS_3-0__SHP__FRA_2023-01-01/CONTOURS-IRIS_3-0__SHP__FRA_2023-01-01.7z",
                    destfile = "/tmp/contours_iris.7z")
archive_extract("/tmp/contours_iris.7z", dir = "/tmp/")
iris_shp <- archive::archive("/tmp/contours_iris.7z") |> 
  filter(str_detect(path, "LAMB93_FXX"), str_detect(path, "\\.shp")) |>
  pull(path)  
iris <- st_read(str_c("/tmp/", iris_shp)) |>
  st_transform(3035) |> 
  semi_join(amp |> st_drop_geometry(), by = "INSEE_COM")

iris |> select(-ID) |> write_parquet("data4ws/iris_amp.parquet")
st_write(iris |> select(-ID) |> st_transform(3857) , dsn = "data4ws/iris_amp.geojson")

# carreaux habités -----------------

c200ze <- ofce::bd_read("c200ze") |> 
  st_drop_geometry() |> 
  select(-com22,-dep) |>
  mutate(idINS = r3035::expand_idINS(idINS))

curl::curl_download("https://www.insee.fr/fr/statistiques/fichier/7655475/Filosofi2019_carreaux_200m_csv.zip",
                    destfile = "/tmp/c200_2019.csv.zip")
aa <- unzip("/tmp/c200_2019.csv.zip", exdir = "/tmp/")
archive_extract(aa, dir = "/tmp/")
c200 <- vroom::vroom("/tmp/carreaux_200m_met.csv")
#r3035::idINS2square(c200$idcar_200m)

c200_amp <- c200 |>
  semi_join(amp_c, by=c("lcog_geo"="INSEE_COM")) |> 
  r3035::idINS2sf(idINS = "idcar_200m") |> 
  transmute(idINS = str_replace(idcar_200m, "CRS3035RES200m", "r200"), 
            INSEE_COM = lcog_geo) |> 
  left_join(c200ze |> select(-emp, -emp_resident, -ze, -scot, -fuite_mobpro, -com, -amenite), by = "idINS") |> 
  mutate(ind_snv  = ind_snv/ind)

c200_amp |> arrow::write_parquet("data4ws/c200_amp.parquet")
st_write(c200_amp |> st_transform(3857) , dsn = "data4ws/c200_amp.geojson")

# grille 200 -----------------

curl::curl_download("https://www.insee.fr/fr/statistiques/fichier/6214726/grille200m_shp.zip",
                    destfile = "/tmp/grille200m.zip")
gg <- unzip("/tmp/grille200m.zip", exdir = "/tmp")
archive::archive_extract(gg |> keep(~str_detect(.x, "metropole")), dir = "/tmp/")
grille200 <- sf::st_read("/tmp/grille200m_metropole.shp") 

coords <- grille200$idINSPIRE |> r3035::idINS2coord() 

bbox <- st_bbox(amp)

in_amp <- (bbox$xmin <= coords[,1]-200) & (coords[,1] <= bbox$xmax+200) &
  (bbox$ymin <= coords[,2]-200) & (coords[,2] <= bbox$ymax+200)

grille_amp <- grille200 |>
  filter(in_amp) |>
  st_transform(3035) |>
  st_filter(amp) |> 
  mutate(idINS = str_replace(idINSPIRE, "CRS3035RES200m", "r200")) |> 
  st_join(iris) |> 
  distinct(idINS, .keep_all = TRUE) |> 
  select(-ID, -idINSPIRE, id_carr_1k, -IRIS) 

grille_amp |> write_parquet("data4ws/grille200.parquet")
st_write(grille_amp |> st_transform(3857), dsn = "data4ws/grille200.geojson", append=FALSE)

# raster template ---------------

r200 <- st_as_stars(st_bbox(c200_amp), dx=200, dy=200, values = 0)

# Emplois -------------

emplois <- ofce::bd_read("c200ze") |> 
  st_drop_geometry() |> 
  filter(emp>0) |> 
  select(idINS, dep, insee_com = com22, IRIS, emp, emp_resident) |> 
  mutate(idINS = r3035::expand_idINS(idINS)) 

emplois |> write_parquet("data4ws/emplois.parquet")

# Bruit ---------------

bruit <- read_parquet("data/bruit-routier-type-a-lden-sur-24h-etude-de-bruit-impedance-ingenierie.parquet") |> 
  filter(!is.na(geo_shape)) |>
  st_as_sf(crs=4326)  |> 
  st_transform(3035)

bruit.r <-  st_rasterize(bruit |> transmute(isophone = 10^(0.1*isophone), n = 1), 
                         template = r200,
                         options = c("MERGE_ALG_ADD", "ALL_TOUCHED=TRUE")) |> 
  mutate(isophone = log10(isophone/n)/0.1)

bruit_data <- bruit.r |> 
  as_data_frame() |>
  drop_na() |> 
  mutate(idINS = r3035::coord2idINS(x=x, y=y)) |> 
  select(idINS, isophone)
  
bruit_data |> r3035::idINS2sf() |> write_parquet("data4ws/bruit.parquet")

# tmap_mode("plot")
# tm_shape(bruit)+tm_fill(fill="isophone")

# chaleur ------------

curl::curl_download("https://www.data.gouv.fr/api/1/datasets/r/6d0378f5-0c75-44ab-822d-078d2cb9ff9a",
                    destfile = "/tmp/chaleur_marseille.zip")
cc <- unzip("/tmp/chaleur_marseille.zip", exdir = "/tmp")

chaleur <- st_read(cc |> keep(~str_detect(.x, "shp$"))) |> 
  st_transform(3035) |> 
  mutate(
    sensibilite = case_when(
      lcz %in% c(1, 2) ~ 5,
      lcz == 3 ~ 4,
      lcz %in% c(4,5) ~ 3,
      lcz %in% c(6, 9) ~ 2,
      lcz %in% c("7","8","E") ~ 2,
      TRUE ~ 1    )
  )

chaleur.r <- st_rasterize(chaleur |> transmute(sensibilite, n = 1), template = r200, options = c("MERGE_ALG=ADD", "ALL_TOUCHED=TRUE")) |> 
  mutate(sensibilite = sensibilite/n)

chaleur.r |> 
  as_data_frame() |>
  drop_na() |> 
  mutate(idINS = r3035::coord2idINS(x=x, y=y)) |> 
  select(idINS, lcz = sensibilite, -x, -y) |> 
  write_parquet("data4ws/localclimatezone.parquet")

# points de deal --------------------

## data donwloaded from https://umap.openstreetmap.fr/en/map/liste-des-quartiers-et-point-de-deal-a-marseille_1125636#6/51.000/2.000

point_de_deal <- vroom::vroom("data/liste_des_quartiers_et_point_de_deal_a_marseille.csv") |> 
  st_as_sf(coords = c( "Longitude", "Latitude"), crs = 4326) |> 
  st_transform(3035) 
cc <- st_coordinates(point_de_deal)
point_de_deal <- point_de_deal |> 
  mutate(idINS = r3035::idINS3035(x=cc[,1], y=cc[,2]))

point_de_deal |> write_parquet("data4ws/point_de_deal.parquet")
point_de_deal |> st_transform(3857) |> st_write("data4ws/point_de_deal.geojson", append=FALSE)

# commerces from fichiers fonciers ---------------------

## from Fichiers Fonciers 2023, base locaux. Les commerces et autres activités professionnelles sont sélectionnées à partir des codes NAF qui sont ceux de l'utilisateur.

commerces <- ofce::bd_read("commerces") |> 
  mutate(idINS = r3035::expand_idINS(toidINS)) |> 
  select(-toidINS, -sprincp.raw)

alim_ns <- commerces |> 
  filter(!is.na(cconaco)) |> 
  group_by(X,Y) |> 
  summarize(sprincp = sum(sprincp),
            across(c(type, cconaco, idINS, w),  ~first(.x))) |> 
  mutate(cconac = cconaco)

commerces <- bind_rows(
  commerces |> filter(is.na(cconaco)), 
  alim_ns)

lonlat <- sf::sf_project(from=st_crs(3035), to=st_crs(4326), pts=matrix(c(commerces$X, Y=commerces$Y), ncol = 2, byrow = FALSE))

commerces_ff <- commerces |> 
  mutate(lon = lonlat[,1],
         lat = lonlat[,2]) |> 
  relocate(idINS, X, Y, lon, lat) |>
  drop_na(X, Y) |> 
  st_as_sf(coords = c("X", "Y"), crs = st_crs(3035))

commerces_ff |> select(-lon, -lat, -w, -cconaco) |>  write_parquet("data4ws/commerces_ff.parquet")
commerces_ff |> select(-lon, -lat, -w, -cconaco) |> st_transform(3857) |> st_write("data4ws/commerces_ff.geojson")

# commerces from OSM ------------

## télécharge les données de la base OSM, le code est facilement adaptable pour d'autres amémités (voir la liste des keys et des tags https://wiki.openstreetmap.org/wiki/Map_features) 

bbox <- amp |> 
  st_transform(4326) |> 
  st_buffer(5000) |> 
  st_bbox(geometry)

am_bbox <-  matrix(c(bbox["xmin"],bbox["ymin"],bbox["xmax"],bbox["ymax"]), nrow = 2, ncol = 2, byrow = FALSE,
                   dimnames = list(c("x", "y"), c("min", "max")))

shops_names <- tribble(
  ~brand, ~shop,
  "Decathlon", "sports",
  "Intersport", "sports",
  "GO sport", "sports",
  "Sport 2000", "sports",
  "Leroy Merlin", "doityourself",
  "Castorama", "doityourself",
  "Bricorama", "doityourself",
  "Weldom", "doityourself",
  "Mr.Bricolage", "doityourself",
  "Spar", "supermarket",
  "Supeco", "supermarket",
  "E.Leclerc", "supermarket",
  "Carrefour", "supermarket",
  "Leader Price", "supermarket",
  "Super U", "supermarket",
  "Lidl", "supermarket",
  "Intermarché", "supermarket",
  "Franprix", "supermarket",
  "Auchan", "supermarket",
  "Aldi", "supermarket",
  "Action", "variety_store",
  "GiFi", "variety_store",
  "La Foir'Fouille", "variety_store",
  "Maxi Bazar", "variety_store",
  "Système U", "supermarket",
  "U Express", "supermarket",
  "Chronodrive", "supermarket", 
  "Casino", "supermarket",
  "Géant Casino", "supermarket",
  "Monoprix", "supermarket",
  "Biocoop", "supermarket",
  "Grand Frais", "supermarket",
  "Picard", "frozen_food",
  "Thiriet", "frozen_food") 

type_names <- tribble(
  ~ type,
  "supermarket",
  "doityourself",
  "sports"
)

shops_data <- pmap(shops_names, \(brand, shop, ...) {
  request <- purrr::safely(~opq(am_bbox, timeout = 1000) |> 
                             add_osm_feature(key = "shop", value = shop) |> 
                             add_osm_feature(key = "brand", value = brand, value_exact = FALSE) |> 
                             osmdata_sf())
  rr <- request()
  i <- 1
  while(!is.null(rr$error)) {
    i <- i + 1
    cli::cli_alert("{i} eme tentative")
    rr <- request()
  }
  rr$result
}, .progress=TRUE)

shops_data <- set_names(shops_data, shops_names$brand)

commerces_osm <- map(shops_data, \(x) {
  x <- unique_osmdata(x)
  polyg <- pluck(x, "osm_polygons") |> 
    mutate(geometry = st_centroid(geometry))
  punkt <- pluck(x, "osm_points") 
  binded <- bind_rows(polyg, punkt)
  if(nrow(binded) == 0) return(NULL) 
  binded |> select(osm_id, name, brand, shop, geometry)
}) |> 
  list_rbind() |> 
  st_as_sf() |> 
  st_transform(3035)

commerces_osm |> st_drop_geometry() |> count(shop, brand)

commerces_osm <- commerces_osm |> 
  st_filter(st_union(amp) |> st_buffer(5000)) 

cc <- st_coordinates(commerces_osm)

commerces_osm <- commerces_osm |> 
  mutate(idINS = r3035::idINS3035(x=cc[,1], y=cc[,2]))
commerces_osm |> write_parquet("data4ws/commerces_osm.parquet")
commerces_osm |> st_transform(3857) |> st_write("data4ws/commerces_osm.geojson")

# Schools ---------------

curl::curl_download("https://www.insee.fr/fr/statistiques/fichier/8217525/BPE24.parquet",
                    destfile = "/tmp/bpe.parquet")

ecoles <- arrow::read_parquet("/tmp/bpe.parquet") |> 
  dplyr::filter(DOM=="C") |> 
  semi_join(amp_c, by=c("DEPCOM"="INSEE_COM")) |> 
  select(SDOM, TYPEQU, NOMRS, LAMBERT_X, LAMBERT_Y, CAPACITE) |>
  st_as_sf(coords=c("LAMBERT_X", "LAMBERT_Y"), crs=2154) |> 
  st_transform(3035) |> 
  mutate(
    type = case_when(
      TYPEQU %in% c("C107", "C108", "C109") ~ "Ecole élémentaire",
      TYPEQU == "C201" ~ "Collège",
      TYPEQU %in% c("C301", "C302", "C303") ~ "Lycée",
      TRUE  ~ "Autres"))

ecoles <- ecoles |> 
  mutate(idINS = r3035::idINS3035(st_coordinates(ecoles))) 

ecoles |> write_parquet("data4ws/ecoles.parquet")
ecoles |> st_transform(3857) |> st_write("data4ws/ecoles.geojson", append=FALSE)

# distances ------------

