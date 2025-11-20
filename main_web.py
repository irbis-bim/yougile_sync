import os
from flask import Flask

app = Flask(__name__)

@app.get("/status")
def status():
    return {"ok": True}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
