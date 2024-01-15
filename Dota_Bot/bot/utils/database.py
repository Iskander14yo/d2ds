import sqlite3

class DataBase():
    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
        self.create_db()

    def create_db(self):
        try:
            query = ('CREATE TABLE IF NOT EXISTS users('
                     'id INTEGER PRIMARY KEY,'
                     'user_mark TEXT,'
                     'telegram_id TEXT);')
            self.cursor.execute(query)
            self.connection.commit()

        except sqlite3.Error as Error:
            print("Oшибка при создании", Error)


    def add_mark(self, user_mark, telegram_id):
        self.cursor.execute(f"INSERT INTO users (user_mark, telegram_id) VALUES (?,?)", (user_mark, telegram_id))
        self.connection.commit()

    def select_marks(self):
        query = 'SELECT user_mark FROM users;'
        marks = self.cursor.execute(query)
        return marks.fetchall()

    def select_user_id(self, telegram_id):
        users = self.cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        return users.fetchone()




    def __del__(self):
        self.cursor.close()
        self.connection.close()

