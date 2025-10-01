import boto3
import psycopg2
import json
from psycopg2.extras import execute_values

class RDSVectorStore:
    def __init__(self, host, database, username, password, port=5432):
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password,
            port=port
        )
        self.setup_database()
    
    def setup_database(self):
        """Create pgvector extension and table"""
        with self.conn.cursor() as cur:
            # Enable pgvector extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # Create table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kalshi_documents (
                    id SERIAL PRIMARY KEY,
                    text TEXT NOT NULL,
                    embedding vector(1024),
                    topic VARCHAR(500),
                    source VARCHAR(100),
                    date TIMESTAMP,
                    url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create index for vector similarity search
            cur.execute("""
                CREATE INDEX IF NOT EXISTS kalshi_embedding_idx 
                ON kalshi_documents USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)
            
        self.conn.commit()
    
    def load_from_s3_gold(self, s3_bucket, s3_key):
        """Load data from S3 Gold bucket"""
        s3 = boto3.client('s3')
        
        # Download and parse JSON
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        data = json.loads(response['Body'].read())
        
        # Insert data
        with self.conn.cursor() as cur:
            values = []
            for item in data:
                if item and item.get('text') and item.get('embedding'):  # Skip None entries
                    # Check embedding dimension
                    if len(item['embedding']) == 1024:
                        values.append((
                            item['text'],
                            item['embedding'],
                            item.get('topic', ''),
                            item.get('source', 'kalshi'),
                            item.get('date'),
                            item.get('url', '')
                        ))
                    else:
                        print(f"Skipping item with {len(item['embedding'])} dimensions")
            
            if values:
                execute_values(
                    cur,
                    """INSERT INTO kalshi_documents (text, embedding, topic, source, date, url) 
                       VALUES %s""",
                    values
                )
        
        self.conn.commit()
        print(f"Loaded {len(values)} documents from {s3_key}")
    
    def search(self, query_embedding, limit=10):
        """Search for similar documents"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT text, topic, source, date, url,
                       embedding <=> %s::vector as distance
                FROM kalshi_documents
                ORDER BY embedding <=> %s::vector
                LIMIT %s
            """, (query_embedding, query_embedding, limit))
            
            return cur.fetchall()

# Usage example
def load_all_gold_data():
    """Load all Gold bucket files into RDS"""
    # Get RDS endpoint
    rds_client = boto3.client('rds', region_name='ap-southeast-2')
    response = rds_client.describe_db_instances(DBInstanceIdentifier='kalshi-rag-db')
    endpoint = response['DBInstances'][0]['Endpoint']['Address']
    
    print(f"Connecting to RDS: {endpoint}")
    
    rds = RDSVectorStore(
        host=endpoint,
        database="postgres",
        username="postgres", 
        password="YourSecurePassword123"
    )
    
    # Load all your Gold files
    gold_files = [
        "kalshi/kalshi_political_forecasts_20250929_072114.json",
        "kalshi/kalshi_political_forecasts_20250929_072434.json",
        "kalshi/kalshi_political_forecasts_20250929_072753.json",
        "kalshi/kalshi_political_forecasts_20250929_073120.json",
        "kalshi/kalshi_political_forecasts_20250929_073848.json",
        "kalshi/kalshi_political_forecasts_20250929_073933.json"
    ]
    
    for file_key in gold_files:
        rds.load_from_s3_gold("kalshi-gold-anubh-001", file_key)

if __name__ == "__main__":
    load_all_gold_data()