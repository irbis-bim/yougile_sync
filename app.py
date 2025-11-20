import sys
import traceback
from datetime import datetime
from PyQt6 import QtWidgets, QtCore

from config import API_TOKEN, PG_DSN, APP_TITLE, SCHEMA
from yougile_api import YougileClient
from db import connect, ensure_schema, upsert_rows, get_existing_ids

# Конфиг для конкретной доски
SPECIAL_BOARD_ID = "b3ca4ebc-858e-46b9-8d43-c34035fe9f07"
SPECIAL_PROJECT_STICKER_ID = "5b0a3b20-1dbb-4df5-b3e6-37ae4581905a"
SPECIAL_DIRECTION_STICKER_ID = "093eef50-9bde-4d5a-b790-b902e0d1d1b9"

# Конфиг по умолчанию для остальных досок
DEFAULT_PROJECT_STICKER_ID = "c3e14cd1-7d09-437c-9fe2-e009fb8cd313"
DEFAULT_DIRECTION_STICKER_ID = "120b46c6-ffac-42cb-87b4-e914077e0404"

def _parse_dt(v):
    """Парсим дату из миллисекунд или ISO строки, возвращаем дату (YYYY-MM-DD)"""
    try:
        if not v:
            return None
        
        if isinstance(v, (int, float)):
            if v > 100000000000:
                dt = datetime.fromtimestamp(v / 1000.0)
            else:
                dt = datetime.fromtimestamp(v)
            return dt.date()
        
        if isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return dt.date()
        
        return None
    except (ValueError, TypeError, OSError):
        return None

class Worker(QtCore.QThread):
    progress = QtCore.pyqtSignal(str)
    done = QtCore.pyqtSignal(str)
    error = QtCore.pyqtSignal(str)

    def __init__(self, api_token: str, pg_dsn: str, schema: str):
        super().__init__()
        self.client = YougileClient(api_token)
        self.pg_dsn = pg_dsn
        self.schema = schema

    def run(self):
        try:
            self.progress.emit("Подключение к PostgreSQL…")
            conn = connect(self.pg_dsn)
            ensure_schema(conn, self.schema)
            self.progress.emit(f"Схема '{self.schema}' готова.")

            # Получаем существующие ID
            self.progress.emit("Проверка существующих данных…")
            existing_task_ids = get_existing_ids(conn, "tasks", self.schema)
            existing_board_ids = get_existing_ids(conn, "boards", self.schema)
            existing_user_ids = get_existing_ids(conn, "users", self.schema)
            self.progress.emit(f"В БД уже: досок={len(existing_board_ids)}, пользователей={len(existing_user_ids)}, задач={len(existing_task_ids)}")

            self.progress.emit("Загрузка данных из API…")
            boards = self.client.list_boards() or []
            users_api = self.client.list_users() or []
            columns = self.client.list_columns() or []
            tasks_raw = self.client.list_tasks() or []
            
            self.progress.emit(f"Получено: досок={len(boards)}, пользователей={len(users_api)}, колонок={len(columns)}, задач={len(tasks_raw)}")

            self.progress.emit("Загрузка стикеров…")
            sticker_states = self.client.get_all_sticker_states()
            self.progress.emit(f"Стикеров загружено: {len(sticker_states)}")

            # --- Подготовка данных для досок (только новые) ---
            self.progress.emit("Подготовка досок…")
            board_rows = [
                (str(b.get("id")), str(b.get("name") or b.get("title") or b.get("caption") or ""))
                for b in boards
                if b.get("id") and str(b.get("id")) not in existing_board_ids
            ]
            self.progress.emit(f"Новых досок к загрузке: {len(board_rows)}")

            # --- Подготовка маппинга колонок -> доски ---
            col_to_board = {
                str(c.get("id")): str(c.get("boardId"))
                for c in columns
                if c.get("id") and c.get("boardId")
            }
            self.progress.emit(f"Маппинг колонок: {len(col_to_board)} связей")

            # --- Собираем user_id из задач ---
            self.progress.emit("Сбор пользователей из задач…")
            all_user_ids_from_tasks = set()
            for t in tasks_raw:
                assigned = t.get("assigned") or []
                for uid in assigned:
                    if uid:
                        all_user_ids_from_tasks.add(str(uid))

            # --- Подготовка данных для пользователей (только новые) ---
            self.progress.emit("Подготовка пользователей…")
            user_rows = []
            processed_users = set(existing_user_ids)

            for u in users_api:
                uid = u.get("id")
                if uid:
                    uid_str = str(uid)
                    if uid_str not in existing_user_ids:
                        user_rows.append((uid_str, str(u.get("realName") or u.get("name") or "")))
                        processed_users.add(uid_str)

            for uid_str in all_user_ids_from_tasks:
                if uid_str not in processed_users:
                    user_rows.append((uid_str, f"Unknown User {uid_str[:8]}"))
                    processed_users.add(uid_str)

            self.progress.emit(f"Новых пользователей к загрузке: {len(user_rows)}")

            # --- Сохранение досок ---
            if board_rows:
                self.progress.emit("Сохранение новых досок…")
                upsert_rows(conn, "boards", ["id", "name"], board_rows, self.schema)

            # --- Сохранение пользователей ---
            if user_rows:
                self.progress.emit("Сохранение новых пользователей…")
                upsert_rows(conn, "users", ["id", "name"], user_rows, self.schema)

            # --- Подготовка данных для задач (только новые, ВСЕ доски) ---
            self.progress.emit("Подготовка задач…")
            task_rows = []
            skipped_tasks = 0

            for t in tasks_raw:
                task_id = t.get("id")
                if not task_id:
                    continue

                # Пропускаем если задача уже в БД
                if str(task_id) in existing_task_ids:
                    continue

                col_id = t.get("columnId")
                board_id = col_to_board.get(str(col_id)) if col_id else None

                if not board_id:
                    skipped_tasks += 1
                    continue

                title = str(t.get("title") or t.get("name") or "")
                assignee_id = None
                assigned = t.get("assigned") or []
                if assigned and len(assigned) > 0:
                    assignee_id = str(assigned[0])

                created_at = _parse_dt(t.get("createdAt") or t.get("timestamp"))

                actual_time = None
                tt = t.get("timeTracking") or {}
                if "work" in tt:
                    try:
                        actual_time = float(tt["work"])
                    except (ValueError, TypeError):
                        pass

                # ЛОГИКА ЗАВИСИТ ОТ ДОСКИ
                sprint_name = None
                project_name = None
                direction = None
                state_category = None

                # Выбираем ID стикеров в зависимости от доски
                if board_id == SPECIAL_BOARD_ID:
                    project_sticker_id = SPECIAL_PROJECT_STICKER_ID
                    direction_sticker_id = SPECIAL_DIRECTION_STICKER_ID
                else:
                    project_sticker_id = DEFAULT_PROJECT_STICKER_ID
                    direction_sticker_id = DEFAULT_DIRECTION_STICKER_ID

                stickers = t.get("stickers") or {}
                for state_id in stickers.values():
                    state_id_str = str(state_id)
                    
                    if state_id_str in sticker_states:
                        state_name_val, parent_id, parent_name = sticker_states[state_id_str]
                        
                        # Сопоставляем по ID стикера для этой доски
                        if parent_id == project_sticker_id:
                            project_name = state_name_val
                        elif parent_id == direction_sticker_id:
                            direction = state_name_val
                        
                        # Спринт (одинаков для всех)
                        name_lower = state_name_val.lower()
                        if "спринт" in name_lower or "sprint" in name_lower:
                            sprint_name = state_name_val
                        else:
                            state_category = parent_name

                task_rows.append((
                    str(task_id),
                    title,
                    board_id,
                    assignee_id,
                    created_at,
                    actual_time,
                    sprint_name,
                    project_name,
                    direction,
                    state_category
                ))

            self.progress.emit(f"Новых задач к загрузке: {len(task_rows)} (пропущено: {skipped_tasks})")

            # --- Сохранение задач ---
            if task_rows:
                self.progress.emit("Сохранение новых задач…")
                upsert_rows(
                    conn,
                    "tasks",
                    ["id", "title", "board_id", "assignee_id", "created_at", "actual_time", "sprint_name", "project_name", "direction", "state_category"],
                    task_rows,
                    self.schema
                )

            msg = f"✓ Импорт завершён!\n\nДобавлено:\n  Досок: {len(board_rows)}\n  Пользователей: {len(user_rows)}\n  Задач: {len(task_rows)}"
            self.done.emit(msg)

        except Exception as e:
            tb = traceback.format_exc()
            self.error.emit(f"❌ Ошибка: {e}\n\n{tb}")

class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.resize(500, 400)

        # Логи
        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet("""
            QPlainTextEdit {
                background-color: #ffffff;
                color: #000000;
                font-family: 'Menlo', monospace;
                font-size: 10pt;
                padding: 5px;
            }
        """)

        # Кнопка импорта
        self.run_btn = QtWidgets.QPushButton("📥 Импорт")
        self.run_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
                font-size: 12pt;
                padding: 10px;
                border-radius: 5px;
                border: none;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3d8b40;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.run_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.run_btn.clicked.connect(self.on_run)

        # Layout
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self.log)
        layout.addWidget(self.run_btn)
        layout.setContentsMargins(10, 10, 10, 10)
        self.setLayout(layout)

        self.worker = None

    def on_run(self):
        self.log.clear()
        self.append_log("🔄 Запуск импорта…\n")
        self.run_btn.setEnabled(False)

        self.worker = Worker(API_TOKEN, PG_DSN, SCHEMA)
        self.worker.progress.connect(self.append_log)
        self.worker.done.connect(self.on_done)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    def append_log(self, text: str):
        self.log.appendPlainText(text)
        # Автоскроллинг в конец
        self.log.verticalScrollBar().setValue(
            self.log.verticalScrollBar().maximum()
        )

    def on_done(self, msg: str):
        self.append_log("\n" + msg)
        self.run_btn.setEnabled(True)

    def on_error(self, msg: str):
        self.append_log("\n" + msg)
        self.run_btn.setEnabled(True)

def main():
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
