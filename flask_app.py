from flask import Flask, jsonify
import psycopg2
import logging


app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="hoda",
        user="postgres",
        password="admin"
    )
    return conn

# API endpoint to fetch details of cities and geometry
@app.route('/api/comune', methods=['GET'])
def get_all_cities():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM public."CITY"')
        cities = [dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]
        return jsonify(cities)
    conn.close()

# API endpoint to fetch details of a specific city
@app.route('/api/comune/<int:uid>', methods=['GET'])
def get_city_by_uid(uid):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM public."CITY" WHERE uid = %s', (uid,))
        city = [dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]
        return jsonify(city)
    conn.close()

if __name__ == '__main__':
    app.run(port=5005, debug=False)