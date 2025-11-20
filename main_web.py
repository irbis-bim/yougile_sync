import os
from flask import Flask, jsonify, render_template_string
from main_worker import run_sync_once

app = Flask(__name__)

INDEX_HTML = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>YouGile Sync</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    html,body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; background:#0b1220; color:#e8eefc; }
    .card { max-width: 560px; margin: 12vh auto; padding: 24px; background: #121a2b; border: 1px solid #1f2a44; border-radius: 12px; }
    h1 { margin: 0 0 12px 0; font-size: 20px; font-weight: 600; }
    p { margin: 0 0 16px 0; color:#b5c3e7; }
    button { appearance: none; border: 0; background: #3563ff; color: white; padding: 10px 16px; border-radius: 8px; font-weight: 600; cursor: pointer; }
    button[disabled] { background:#2a3b7a; cursor: not-allowed; }
    .status { margin-top: 12px; font-size: 14px; white-space: pre-line; }
  </style>
</head>
<body>
  <div class="card">
    <h1>YouGile → PostgreSQL</h1>
    <p>Нажмите кнопку для синхронизации</p>
    <button id="syncBtn" onclick="runSync()">Синхронизировать</button>
    <div class="status" id="status"></div>
  </div>
<script>
async function runSync() {
  const btn = document.getElementById('syncBtn');
  const st = document.getElementById('status');
  btn.disabled = true;
  st.textContent = 'Выполняется...';
  try {
    const res = await fetch('/sync', { method: 'POST' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    st.textContent = 'Готово: ' + JSON.stringify(data);
  } catch (e) {
    st.textContent = 'Ошибка: ' + e.message;
  } finally {
    btn.disabled = false;
  }
}
</script>
</body>
</html>
"""

@app.get("/")
def index():
    return render_template_string(INDEX_HTML)

@app.get("/status")
def status():
    return {"ok": True}, 200

@app.post("/sync")
def manual_sync():
    run_sync_once()
    return jsonify({"ok": True}), 200

if __name__ == "__main__":
    raw_port = os.getenv("PORT")
    port = int(raw_port) if raw_port and raw_port.isdigit() else 10000
    app.run(host="0.0.0.0", port=port)
