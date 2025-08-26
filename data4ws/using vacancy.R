
vacancy <- open_dataset("data4ws/c200_amp.parquet") |> 
  st_as_sf() |> 
  select(idINS, IRIS, INSEE_COM) |> 
  left_join(read_parquet("data4ws/vacanceRS.parquet") , by="idINS")

vacancy |> write_parquet("data4ws/vacancy2.parquet")
vacancy |> st_transform(3857) |> st_write("data4ws/vacancy2.geojson")
