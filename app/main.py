from typing import Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import psycopg2
import time
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Connecting to database
while True:
    try:
        conn = psycopg2.connect(host='advaid.postgres.database.azure.com', user='advaid', dbname='fastapidb', password='*assIGNMENT1234', cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        print("Connection established")
        break
    except Exception as error:
        print("Can't connect to the database")
        print("Error:", error)
        time.sleep(2)

# Validate the input
class User(BaseModel):
    email: str
    is_active: bool
    salary: int
    privilage: Optional[str] = None

class Update(BaseModel):
    is_active: bool
    salary: int
    privilage: Optional[str] = None

# Get the Whole Table
@app.get("/data")
async def get_data():
    try:
        cursor.execute("SELECT * FROM test1")
        x = cursor.fetchall()
        return x
    except psycopg2.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# Post the Data
@app.post("/data", status_code=status.HTTP_201_CREATED)
async def create_data(user: User):
    try:
        cursor.execute("INSERT INTO test1 (is_active, email, salary, privilage) VALUES (%s, %s, %s, %s) RETURNING * ", (user.is_active, user.email, user.salary, user.privilage))
        data1 = cursor.fetchone()
        conn.commit()
        return {"data": data1}
    except psycopg2.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# Delete Data By Email
@app.delete("/users/{email}")
async def delete_post_by_email(email: str):
    try:
        cursor.execute("DELETE FROM test1 WHERE email=%s RETURNING * ", (email,))
        data1 = cursor.fetchone()
        conn.commit()
        return {"deleted": data1}
    except psycopg2.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# Update a user data
@app.put("/users/{email}")
async def update_post_by_email(email: str, user1: Update):
    try:
        cursor.execute("UPDATE test1 SET is_active=%s, privilage=%s, salary=%s WHERE email=%s RETURNING * ", (user1.is_active, user1.privilage, user1.salary, email))
        data1 = cursor.fetchone()
        conn.commit()
        return {"Updated": data1}
    except psycopg2.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
