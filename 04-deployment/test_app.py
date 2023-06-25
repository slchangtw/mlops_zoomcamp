import requests
import typer

app = typer.Typer()

@app.command()
def send_request(color: str, year: str, month: str) -> None:
    trips_params = {
        "color": color,
        "year": year,
        "month": month,
    }
    url = "http://localhost:9696/predict"
    response = requests.post(url, json=trips_params)
    print(response.json())

if __name__ == "__main__":
    app()
