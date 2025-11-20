import os
from flask import Flask, jsonify
from main_worker import run_sync_once

app = Flask(__name__)

@app.get("/status")
def status():
    return {"ok": True}, 200

@app.post("/sync")
def manual_sync():
    # Запускает синк один раз и возвращает ответ
    run_sync_once()
    return jsonify({"ok": True}), 200

if __name__ == "__main__":
    # Render выставляет переменную PORT; слушаем её
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
