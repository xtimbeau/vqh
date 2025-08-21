
import cdsapi

dataset = "derived-utci-historical"
request = {
    "version": "1_1",
    "year": ["2025"],
    "month": ["08"],
    "day": ["11"]
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()
