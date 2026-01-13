from dopedb.engine import Database
import os
import shutil

def test():
    # Clean up previous data
    if os.path.exists('data'):
        shutil.rmtree('data')
    
    db = Database()
    
    # 1. Create Tables
    print(db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT UNIQUE)"))
    print(db.execute("CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT)"))
    
    # 2. Insert Data
    print(db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    print(db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
    
    print(db.execute("INSERT INTO posts (id, user_id, title) VALUES (101, 1, 'Hello World')"))
    print(db.execute("INSERT INTO posts (id, user_id, title) VALUES (102, 1, 'My second post')"))
    print(db.execute("INSERT INTO posts (id, user_id, title) VALUES (103, 2, 'Bobs thoughts')"))
    
    # 3. Select
    print("\nUsers:")
    print(db.execute("SELECT * FROM users"))
    
    # 4. Join
    print("\nPosts with User Names (JOIN):")
    joined = db.execute("SELECT * FROM posts JOIN users ON posts.user_id = users.id")
    for row in joined:
        print(row)
        
    # 5. Unique constraint check
    try:
        db.execute("INSERT INTO users (id, name) VALUES (3, 'Alice')")
    except ValueError as e:
        print(f"\nExpected Error (Unique constraint): {e}")

    # 6. Update
    print("\nUpdate Bob to Bobby:")
    print(db.execute("UPDATE users SET name = 'Bobby' WHERE id = 2"))
    print(db.execute("SELECT * FROM users WHERE id = 2"))
    
    # 7. Delete
    print("\nDelete post 102:")
    print(db.execute("DELETE FROM posts WHERE id = 102"))
    print(db.execute("SELECT * FROM posts"))

    # 8. Commas in strings
    print("\nInsert post with comma in title:")
    print(db.execute("INSERT INTO posts (id, user_id, title) VALUES (104, 1, 'Buy milk, eggs and bread')"))
    print(db.execute("SELECT * FROM posts WHERE id = 104"))

if __name__ == "__main__":
    test()