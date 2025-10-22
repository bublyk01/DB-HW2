#!/usr/bin/env python3
import os, csv, random, argparse, math
from datetime import datetime, timedelta
from faker import Faker
import mysql.connector as mysql

def connect(user, password, host, port):
    return mysql.connect(
        user=user, password=password, host=host, port=port,
        allow_local_infile=True, autocommit=True
    )

DDL = {
"customers": """
CREATE TABLE IF NOT EXISTS customers (
  customer_id BIGINT PRIMARY KEY,
  first_name VARCHAR(64),
  last_name  VARCHAR(64),
  email      VARCHAR(128),
  signup_date DATE,
  country    VARCHAR(64),
  city       VARCHAR(64),
  marketing_opt_in TINYINT(1),
  acquisition_source VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"products": """
CREATE TABLE IF NOT EXISTS products (
  product_id BIGINT PRIMARY KEY,
  category   VARCHAR(64),
  subcategory VARCHAR(64),
  brand      VARCHAR(64),
  price      DECIMAL(10,2),
  cost       DECIMAL(10,2),
  created_at DATE,
  is_active  TINYINT(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"orders": """
CREATE TABLE IF NOT EXISTS orders (
  order_id   BIGINT PRIMARY KEY,
  customer_id BIGINT,
  order_date DATETIME,
  status     VARCHAR(16),
  currency   CHAR(3),
  payment_method VARCHAR(16),
  shipping_country VARCHAR(64),
  discount   DECIMAL(10,2),
  total_amount DECIMAL(12,2),
  INDEX idx_orders_customer (customer_id),
  INDEX idx_orders_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"order_items": """
CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT PRIMARY KEY,
  order_id  BIGINT,
  product_id BIGINT,
  quantity  INT,
  unit_price DECIMAL(10,2),
  line_total DECIMAL(12,2),
  INDEX idx_items_order (order_id),
  INDEX idx_items_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
}

def run_sql(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def ensure_db(conn, db):
    run_sql(conn, f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4;")
    run_sql(conn, f"USE `{db}`;")

def create_tables(conn):
    for name, ddl in DDL.items():
        run_sql(conn, ddl)

def daterange(start_days_ago=365*2, end_days_ago=0):
    now = datetime.utcnow()
    start = now - timedelta(days=start_days_ago)
    end   = now - timedelta(days=end_days_ago)
    return (start, end)

def rand_datetime(start, end):
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)

def write_csv(path, header, row_iter, chunk=100_000):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    written = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in row_iter:
            w.writerow(row)
            written += 1
            if written % chunk == 0:
                print(f"  wrote {written:,} rows -> {os.path.basename(path)}")
    print(f"  done {written:,} rows -> {os.path.basename(path)}")
    return written

def ensure_local_infile_enabled(conn):
    cur = conn.cursor()
    cur.execute("SHOW VARIABLES LIKE 'local_infile'")
    name, val = cur.fetchone()
    cur.close()
    if str(val).lower() in ("on", "1", "true"):
        return
    try:
        # Requires SUPER/SESSION_VARIABLES_ADMIN
        run_sql(conn, "SET GLOBAL local_infile = 1;")
        print("Enabled GLOBAL local_infile=1")
    except Exception as e:
        print("Warning: couldn't set GLOBAL local_infile=1 (need privileges). "
              "Will attempt LOAD DATA LOCAL anyway. Details:", e)

def load_csv(conn, db, table, path):
    ensure_local_infile_enabled(conn)   # <--- add this
    run_sql(conn, "SET FOREIGN_KEY_CHECKS=0;")
    q = (f"LOAD DATA LOCAL INFILE %s INTO TABLE `{db}`.`{table}` "
         "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
         "LINES TERMINATED BY '\n' IGNORE 1 LINES")
    cur = conn.cursor()
    cur.execute(q, (os.path.abspath(path),))
    cur.close()
    run_sql(conn, "SET FOREIGN_KEY_CHECKS=1;")

def gen_products(fake, n_products):
    categories = [
        ("Electronics", ["Phones","Laptops","Headphones","Monitors","Cameras"], ["Acme","Zebra","Lux","Nova","Kite"]),
        ("Home", ["Kitchen","Bedding","Furniture","Decor"], ["Homely","Casa","Nido","Oak&Co"]),
        ("Outdoors", ["Camping","Cycling","Hiking","Fishing"], ["Trail","Peak","Rivera"]),
        ("Beauty", ["Skincare","Haircare","Fragrance"], ["Aura","Bloom","Velvet"]),
        ("Toys", ["Blocks","RC","Puzzles","Plush"], ["PlayCo","Kiddo","FunLab"]),
    ]
    def iterator():
        pid = 1
        for _ in range(n_products):
            cat, subs, brands = random.choice(categories)
            sub = random.choice(subs)
            brand = random.choice(brands)
            base = random.uniform(5, 900)
            price = round(base, 2)
            cost = round(price * random.uniform(0.5, 0.85), 2)
            created = fake.date_between(start_date="-5y", end_date="today")
            is_active = 1 if random.random() > 0.15 else 0
            yield (pid, cat, sub, brand, price, cost, created.isoformat(), is_active)
            pid += 1
    return iterator

def gen_customers(fake, n_customers):
    sources = ["seo","sem","email","social","direct","referral","marketplace"]
    countries = ["UA","PL","DE","FR","GB","US","CA","ES","IT","NL","SE","NO"]
    def iterator():
        cid = 1
        for _ in range(n_customers):
            first = fake.first_name()
            last  = fake.last_name()
            email = f"{first}.{last}.{cid}@example.com".lower()
            signup = fake.date_between(start_date="-3y", end_date="today")
            country = random.choice(countries)
            city = fake.city()
            mkt = 1 if random.random() < 0.35 else 0
            source = random.choice(sources)
            yield (cid, first, last, email, signup.isoformat(), country, city, mkt, source)
            cid += 1
    return iterator

def gen_orders(fake, n_orders, n_customers):
    statuses = ["paid","paid","paid","shipped","shipped","cancelled","refunded"]
    pay_methods = ["card","card","card","paypal","cod","applepay","googlepay"]
    currencies = ["USD","EUR","PLN","GBP"]
    countries = ["UA","PL","DE","FR","GB","US","CA","ES","IT","NL","SE","NO"]
    start, end = daterange(730, 0)
    def iterator():
        oid = 1
        for _ in range(n_orders):
            cust = random.randint(1, n_customers)
            dt = rand_datetime(start, end)
            status = random.choice(statuses)
            curr = random.choice(currencies)
            pay = random.choice(pay_methods)
            ship = random.choice(countries)
            discount = round(max(0.0, random.gauss(3, 7)), 2) if random.random() < 0.25 else 0.00
            # placeholder total; will be recomputed later if you want—keep something realistic
            total = round(abs(random.gauss(80, 70)) + (5 if status in ("shipped","paid") else 0), 2)
            yield (oid, cust, dt.strftime("%Y-%m-%d %H:%M:%S"), status, curr, pay, ship, discount, total)
            oid += 1
    return iterator

def gen_order_items(n_orders, n_products, avg_items=3):
    # ~Poisson-like distribution: 1–6 items per order
    def items_per_order():
        r = random.random()
        if   r < 0.40: return 1
        elif r < 0.70: return 2
        elif r < 0.88: return 3
        elif r < 0.95: return 4
        elif r < 0.985: return 5
        else: return 6

    def iterator():
        item_id = 1
        for oid in range(1, n_orders+1):
            k = items_per_order()
            for _ in range(k):
                pid = random.randint(1, n_products)
                qty = 1 if random.random() < 0.7 else random.randint(2,5)
                # Unit price around product price distribution (approximate, independent of products CSV to keep it fast)
                unit = round(max(1.0, random.gauss(60, 50)), 2)
                line_total = round(unit * qty, 2)
                yield (item_id, oid, pid, qty, unit, line_total)
                item_id += 1
    return iterator

def main():
    ap = argparse.ArgumentParser(description="Generate large e-commerce sample data and load to MySQL.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default=3306, type=int)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="ecommerce_synth")
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--customers", type=int, default=1_200_000)
    ap.add_argument("--products",  type=int, default=1_200_000)
    ap.add_argument("--orders", type=int, default=2_000_000)
    ap.add_argument("--items_avg", type=float, default=3.0, help="Average items per order (approx)")
    args = ap.parse_args()

    print("Connecting to MySQL…")
    conn = connect(args.user, args.password, args.host, args.port)
    ensure_db(conn, args.database)
    create_tables(conn)

    fake = Faker()
    fake.seed_instance(42)
    random.seed(42)

    os.makedirs(args.outdir, exist_ok=True)

    # PRODUCTS
    print("\nGenerating products…")
    products_csv = os.path.join(args.outdir, "products.csv")
    p_rows = write_csv(
        products_csv,
        ["product_id", "category", "subcategory", "brand", "price", "cost", "created_at", "is_active"],
        gen_products(fake, args.products)()  # <-- call it
    )
    print("Loading products into MySQL…")
    load_csv(conn, args.database, "products", products_csv)

    # CUSTOMERS
    print("\nGenerating customers…")
    customers_csv = os.path.join(args.outdir, "customers.csv")
    c_rows = write_csv(
        customers_csv,
        ["customer_id","first_name","last_name","email","signup_date","country","city","marketing_opt_in","acquisition_source"],
        gen_customers(fake, args.customers)()
    )
    print("Loading customers into MySQL…")
    load_csv(conn, args.database, "customers", customers_csv)

    # ORDERS
    print("\nGenerating orders…")
    orders_csv = os.path.join(args.outdir, "orders.csv")
    o_rows = write_csv(
        orders_csv,
        ["order_id","customer_id","order_date","status","currency","payment_method","shipping_country","discount","total_amount"],
        gen_orders(fake, args.orders, args.customers)()
    )
    print("Loading orders into MySQL…")
    load_csv(conn, args.database, "orders", orders_csv)

    # ORDER ITEMS
    print("\nGenerating order_items (streamed; target ~avg items/order)…")
    items_csv = os.path.join(args.outdir, "order_items.csv")
    i_rows = write_csv(
        items_csv,
        ["order_item_id","order_id","product_id","quantity","unit_price","line_total"],
        gen_order_items(args.orders, args.products, args.items_avg)()
    )
    print("Loading order_items into MySQL…")
    load_csv(conn, args.database, "order_items", items_csv)

    # (Optional) Add FKs after load (comment in if you want strict integrity; this can take a while on huge tables)
    # print("\nAdding foreign keys… (optional)")
    # run_sql(conn, f"ALTER TABLE `{args.database}`.`orders` ADD CONSTRAINT fk_orders_customer "
    #               f"FOREIGN KEY (customer_id) REFERENCES `{args.database}`.`customers`(customer_id);")
    # run_sql(conn, f"ALTER TABLE `{args.database}`.`order_items` ADD CONSTRAINT fk_items_order "
    #               f"FOREIGN KEY (order_id) REFERENCES `{args.database}`.`orders`(order_id);")
    # run_sql(conn, f"ALTER TABLE `{args.database}`.`order_items` ADD CONSTRAINT fk_items_product "
    #               f"FOREIGN KEY (product_id) REFERENCES `{args.database}`.`products`(product_id);")

    print("\nAll done!")
    print(f"Rows written -> products: {p_rows:,}, customers: {c_rows:,}, orders: {o_rows:,}, order_items: {i_rows:,}")
    conn.close()

if __name__ == "__main__":
    main()
