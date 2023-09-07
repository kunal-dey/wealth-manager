from quart import Quart
from quart_cors import cors

app = Quart(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True
app = cors(app, allow_origin="*")


@app.get("/")
async def home():
    return {"message": "Welcome to the Zerodha trading system"}


if __name__ == "__main__":
    app.run(port=8081)
