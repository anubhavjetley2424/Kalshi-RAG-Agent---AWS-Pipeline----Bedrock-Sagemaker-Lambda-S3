import boto3
import json
import os
import csv
from io import StringIO
import datetime

# AWS Clients
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-southeast-2"))
bedrock = boto3.client("bedrock-runtime", region_name=os.environ.get("AWS_REGION", "ap-southeast-2"))
rds_data = boto3.client("rds-data", region_name=os.environ.get("AWS_REGION", "ap-southeast-2"))

# Buckets
BUCKET_BRONZE = "kalshi-bronze-anubh-001"

# Aurora cluster + secret
CLUSTER_ARN = "arn:aws:rds:ap-southeast-2:647664611140:cluster:kalshi-aurora-rds"
SECRET_ARN = "arn:aws:secretsmanager:ap-southeast-2:647664611140:secret:rds!cluster-7e004f54-e48c-406b-99e8-3a57cea73662-P4k120"
DATABASE_NAME = "postgres"

def generate_embedding(text: str):
    """Call Bedrock Titan to generate embeddings"""
    print(f"🧠 Generating embedding for: {text[:50]}...")
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text}),
        contentType="application/json",
        accept="application/json"
    )
    embedding = json.loads(response["body"].read()).get("embedding", [])
    print(f"✅ Embedding generated: {len(embedding)} dimensions")
    return embedding

def insert_document(text, question, date, embedding):
    """Insert a single record into Aurora via Data API"""
    print(f"💾 Inserting to RDS: {question[:30]}...")
    sql = """
        INSERT INTO kalshi_documents (text, topic, date, embedding)
        VALUES (:text, :topic, :date, :embedding::jsonb)
    """
    rds_data.execute_statement(
        resourceArn=CLUSTER_ARN,
        secretArn=SECRET_ARN,
        database=DATABASE_NAME,
        sql=sql,
        parameters=[
            {"name": "text", "value": {"stringValue": text}},
            {"name": "topic", "value": {"stringValue": question}},
            {"name": "date", "value": {"stringValue": date}},
            {"name": "embedding", "value": {"stringValue": json.dumps(embedding)}}
        ]
    )
    print("✅ RDS insert successful")



import urllib.parse

def lambda_handler(event, context):
    try:
        key = event.get("file") or event["Records"][0]["s3"]["object"]["key"]
        
        # URL decode the key to handle spaces
        decoded_key = urllib.parse.unquote_plus(key)
        print(f"📁 Processing file: {decoded_key}")
        
        obj = s3.get_object(Bucket=BUCKET_BRONZE, Key=decoded_key)
        content = obj["Body"].read().decode("utf-8")
        data = list(csv.DictReader(StringIO(content)))
        print(f"📊 CSV rows found: {len(data)}")

        processed = 0
        for item in data:
            text = None
            
            if decoded_key.startswith("social/"):
                context = item.get("context", "").strip()
                topic_raw = item.get("topic", "").strip()
                if context and topic_raw:
                    text = context
                    topic = f"{topic_raw} social sentiment"
                    date_raw = item.get('datetime', '')
                    try:
                        if '/' in date_raw:
                            dt = datetime.strptime(date_raw, '%d/%m/%Y %H:%M')
                            date = dt.strftime('%Y-%m-%d %H:%M')
                        else:
                            date = date_raw
                    except:
                        date = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
            
            if text:
                embedding = generate_embedding(text)
                insert_document(text, topic, date, embedding)
                processed += 1

        return {"status": "success", "processed": processed}
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {"status": "error", "error": str(e)}
