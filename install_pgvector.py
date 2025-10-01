import psycopg2

# Install pgvector extension
conn = psycopg2.connect(
    host="kalshi-rag-db.cr4oq4mee56z.ap-southeast-2.rds.amazonaws.com",
    database="postgres",
    user="postgres",
    password="YourSecurePassword123",
    port=5432
)

with conn.cursor() as cur:
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    print("pgvector extension installed successfully!")

conn.commit()
conn.close()