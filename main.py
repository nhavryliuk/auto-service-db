import psycopg2

conn = psycopg2.connect(
    dbname="auto_service",
    user="adminn",
    password="admin123",
    host="localhost",
    port="5432"
)

cur = conn.cursor()




cur.execute("""
CREATE TABLE IF NOT EXISTS clients (
    client_id SERIAL PRIMARY KEY,
    company_name VARCHAR(100),
    bank_account VARCHAR(34),
    phone VARCHAR(20),
    contact_person VARCHAR(100),
    address TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS cars (
    car_id SERIAL PRIMARY KEY,
    brand VARCHAR(50),
    price NUMERIC,
    client_id INT REFERENCES clients(client_id) ON DELETE CASCADE
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS repairs (
    repair_id SERIAL PRIMARY KEY,
    start_date DATE,
    car_id INT REFERENCES cars(car_id) ON DELETE CASCADE,
    repair_type VARCHAR(20),
    hourly_rate NUMERIC,
    discount NUMERIC,
    hours INT
);
""")

conn.commit()




cur.executemany("""
INSERT INTO clients(company_name, bank_account, phone, contact_person, address)
VALUES (%s,%s,%s,%s,%s)
""", [
("AutoOne", "UA111", "050-111-1111", "Ivan Petrenko", "Kyiv"),
("SpeedCar", "UA222", "050-222-2222", "Oleh Ivanov", "Lviv"),
("DriveMax", "UA333", "050-333-3333", "Anna Kovalenko", "Odesa"),
("CarPlus", "UA444", "050-444-4444", "Serhii Bondar", "Dnipro"),
("FordService", "UA555", "050-555-5555", "Iryna Shevchenko", "Kharkiv"),
("AutoLux", "UA666", "050-666-6666", "Dmytro Koval", "Kyiv")
])

cur.executemany("""
INSERT INTO cars(brand, price, client_id)
VALUES (%s,%s,%s)
""", [
("fiesta", 20000, 1),
("focus", 25000, 2),
("fusion", 27000, 3),
("mondeo", 30000, 4),
("fiesta", 21000, 5),
("focus", 26000, 6)
])

cur.executemany("""
INSERT INTO repairs(start_date, car_id, repair_type, hourly_rate, discount, hours)
VALUES (%s,%s,%s,%s,%s,%s)
""", [
("2026-01-01",1,"гарантійний",500,0,2),
("2026-01-02",2,"плановий",600,5,3),
("2026-01-03",3,"капітальний",800,10,5),
("2026-01-04",4,"плановий",550,0,2),
("2026-01-05",5,"гарантійний",500,0,1),
("2026-01-06",6,"капітальний",900,10,6),
("2026-01-07",1,"плановий",600,5,2),
("2026-01-08",2,"гарантійний",500,0,3),
("2026-01-09",3,"плановий",550,5,4),
("2026-01-10",4,"капітальний",800,10,5),
("2026-01-11",5,"плановий",600,0,2),
("2026-01-12",6,"гарантійний",500,0,1),
("2026-01-13",1,"капітальний",900,10,3),
("2026-01-14",2,"плановий",550,5,2),
("2026-01-15",3,"гарантійний",500,0,1)
])

conn.commit()




queries = {

"1. Гарантійні ремонти":
"""
SELECT * FROM repairs
WHERE repair_type = 'гарантійний';
""",

"2. Клієнти по алфавіту":
"""
SELECT * FROM clients
ORDER BY company_name;
""",

"3. Вартість ремонту":
"""
SELECT repair_id,
hours * hourly_rate AS total,
(hours * hourly_rate) * (1 - discount/100) AS total_with_discount
FROM repairs;
""",

"4. Ремонти по марці (focus)":
"""
SELECT c.brand, r.*
FROM repairs r
JOIN cars c ON r.car_id = c.car_id
WHERE c.brand = 'focus';
""",

"5. Сума по клієнтах":
"""
SELECT cl.company_name,
SUM(r.hours * r.hourly_rate * (1 - r.discount/100)) AS total_spent
FROM clients cl
JOIN cars c ON cl.client_id = c.client_id
JOIN repairs r ON c.car_id = r.car_id
GROUP BY cl.company_name;
""",

"6. Кількість типів ремонтів":
"""
SELECT cl.company_name, r.repair_type, COUNT(*)
FROM clients cl
JOIN cars c ON cl.client_id = c.client_id
JOIN repairs r ON c.car_id = r.car_id
GROUP BY cl.company_name, r.repair_type;
""",

"7. Ремонти по марках":
"""
SELECT c.brand, COUNT(*)
FROM cars c
JOIN repairs r ON c.car_id = r.car_id
GROUP BY c.brand;
"""
}




def print_table(title, rows):
    print("\n" + "="*60)
    print(title)
    print("="*60)
    for row in rows:
        print(row)




for title, q in queries.items():
    cur.execute(q)
    rows = cur.fetchall()
    print_table(title, rows)


cur.close()
conn.close()