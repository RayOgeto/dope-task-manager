from flask import Flask, render_template, request, redirect, url_for
from dopedb.engine import Database
import random
import os

app = Flask(__name__)
db = Database()

# Initialize DB table if not exists
try:
    db.execute("CREATE TABLE tasks (id INT PRIMARY KEY, title TEXT, status TEXT)")
except Exception:
    pass # Table likely already exists

@app.route('/')
def index():
    tasks = db.execute("SELECT * FROM tasks")
    return render_template('index.html', tasks=tasks or [])

@app.route('/add', methods=['POST'])
def add():
    title = request.form.get('title')
    if title:
        task_id = random.randint(1000, 9999)
        db.execute(f"INSERT INTO tasks (id, title, status) VALUES ({task_id}, '{title}', 'pending')")
    return redirect(url_for('index'))

@app.route('/complete/<int:task_id>')
def complete(task_id):
    db.execute(f"UPDATE tasks SET status = 'done' WHERE id = {task_id}")
    return redirect(url_for('index'))

@app.route('/delete/<int:task_id>')
def delete(task_id):
    db.execute(f"DELETE FROM tasks WHERE id = {task_id}")
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
