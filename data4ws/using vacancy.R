
vacancy <- open_dataset("data4ws/c200_amp.parquet") |> 
  st_as_sf() |> 
  select(idINS, IRIS, INSEE_COM) |> 
  left_join(read_parquet("data4ws/vacanceRS.parquet") , by="idINS")

vacancy |> write_parquet("data4ws/vacancy2.parquet")
vacancy |> st_transform(3857) |> st_write("data4ws/vacancy2.geojson")


iris <- open_dataset("data4ws/iris_amp.parquet") |> st_as_sf()

vacancy.iris <- open_dataset("data4ws/vacancy2.parquet") |> 
  st_as_sf() |> 
  group_by(IRIS) |> 
  st_drop_geometry() |> 
  summarize(
    across(-c(idINS, INSEE_COM), ~sum(.x, na.rm=TRUE) ) ) |> 
  left_join(iris, by = "IRIS")

vacancy.iris |> write_parquet("data4ws/vacancy_iris.parquet")

vacancy.iris |> st_transform(3857) |> write_parquet("data4ws/vacancy_iris.geojson")
    