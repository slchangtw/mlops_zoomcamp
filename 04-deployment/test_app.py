import requests

trips_params = {
    "color": "yellow",
    "year": "2022",
    "month": "02",
}

url = "http://localhost:9696/predict"
response = requests.post(url, json=trips_params)
print(response.json())
