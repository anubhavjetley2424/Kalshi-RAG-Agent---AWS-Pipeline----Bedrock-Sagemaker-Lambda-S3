import boto3
import json
import psycopg2
import numpy as np

# Cell 3: Configuration
def get_rds_password():
    secrets_client = boto3.client('secretsmanager', region_name='ap-southeast-2')
    response = secrets_client.get_secret_value(
        SecretId='arn:aws:secretsmanager:ap-southeast-2:647664611140:secret:rds!cluster-7e004f54-e48c-406b-99e8-3a57cea73662-P4k120'
    )
    secret = json.loads(response['SecretString'])
    return secret['password']

RDS_HOST = "kalshi-aurora-rds-instance-1.cr4oq4mee56z.ap-southeast-2.rds.amazonaws.com"
RDS_DATABASE = "postgres"
RDS_USERNAME = "postgres"
RDS_PASSWORD = get_rds_password()
AWS_REGION = "ap-southeast-2"

bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
print("✅ Configuration loaded")

# Cell 4: Generate embedding function
def generate_embedding(text):
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text}),
        contentType="application/json",
        accept="application/json"
    )
    return json.loads(response["body"].read()).get("embedding", [])

# Cell 5: Test database connection
def test_connection():
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DATABASE, user=RDS_USERNAME,
            password=RDS_PASSWORD, port=5432, sslmode='require'
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM kalshi_documents;")
            count = cur.fetchone()[0]
            print(f"✅ Connected! Found {count} documents")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

test_connection()

# Cell 6: Check sentiment data in database
def check_sentiment_data():
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DATABASE, user=RDS_USERNAME,
            password=RDS_PASSWORD, port=5432, sslmode='require'
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT topic, COUNT(*) as count, MIN(date) as earliest, MAX(date) as latest
                FROM kalshi_documents 
                WHERE topic LIKE '%sentiment%'
                GROUP BY topic
                ORDER BY count DESC
            """)
            
            sentiment_results = cur.fetchall()
            print("📊 Sentiment Data in Database:")
            for row in sentiment_results:
                topic, count, earliest, latest = row
                print(f"  • {topic}: {count} posts ({earliest} to {latest})")
            
            cur.execute("""
                SELECT topic, COUNT(*) as count
                FROM kalshi_documents 
                WHERE topic ILIKE '%jersey%' OR topic ILIKE '%governor%'
                GROUP BY topic
                ORDER BY count DESC
            """)
            
            nj_results = cur.fetchall()
            print("\n🏛️ New Jersey Related Topics:")
            for row in nj_results:
                topic, count = row
                print(f"  • {topic}: {count} records")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

check_sentiment_data()

# Cell 7: Pure Vector K-NN RAG (No hardcoded filters)
def kalshi_pure_vector_rag(question, top_k=50):
    try:
        print(f"🔍 Pure Vector Search: {question}")
        
        query_vector = generate_embedding(question)
        
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DATABASE, user=RDS_USERNAME,
            password=RDS_PASSWORD, port=5432, sslmode='require'
        )
        
        with conn.cursor() as cur:
            # Pure vector similarity - no topic filtering
            cur.execute("""
                SELECT text, topic, date, 
                       (embedding::text::vector) <=> %s::vector as distance
                FROM kalshi_documents
                ORDER BY (embedding::text::vector) <=> %s::vector
                LIMIT %s
            """, (query_vector, query_vector, top_k))
            
            results = cur.fetchall()
        
        conn.close()
        
        # Separate by data type
        kalshi_data = []
        social_data = []
        
        print("📊 Top Vector Matches:")
        for i, (text, topic, date, distance) in enumerate(results[:10]):
            print(f"  {i+1}. [{distance:.3f}] {topic[:50]}...")
            
            if "social sentiment" in topic:
                social_data.append(f"[{date}] {text}")
            else:
                kalshi_data.append(text)
        
        kalshi_context = "\n".join(kalshi_data[:15])
        social_context = "\n".join(social_data[:10])
        
        prompt = f"""You are a Kalshi ROI analyst. Use market data + social sentiment for profitable opportunities.

MARKET DATA (Question,Option,Date,Odds %):
{kalshi_context}

SOCIAL SENTIMENT:
{social_context}

Question: {question}

Analyze for ROI opportunities:
1. Market odds trends and momentum
2. Social sentiment direction and intensity  
3. Market inefficiencies (sentiment vs odds mismatch)
4. Calculate expected ROI based on true vs implied probability

Return JSON:
{{
    "best_opportunity": "option name",
    "current_odds": float,
    "implied_probability": float,
    "true_probability_estimate": float,
    "expected_roi_percentage": float,
    "sentiment_momentum": "bullish/bearish/neutral",
    "market_assessment": "undervalued/overvalued/fair",
    "confidence": "high/medium/low",
    "key_insights": ["insight1", "insight2"],
    "reasoning": "detailed analysis"
}}"""

        # Generate analysis
        llm_response = bedrock.invoke_model(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 700
            }),
            contentType="application/json",
            accept="application/json"
        )
        
        llm_result = json.loads(llm_response["body"].read())
        completion = llm_result["content"][0]["text"]

        # Parse response
        try:
            if "```json" in completion:
                json_start = completion.find("```json") + 7
                json_end = completion.find("```", json_start)
                json_text = completion[json_start:json_end].strip()
            else:
                json_text = completion
            
            analysis = json.loads(json_text)
        except:
            analysis = {"error": "Parse failed", "raw": completion[:200]}

        return {
            "question": question,
            "roi_analysis": analysis,
            "data_breakdown": {
                "kalshi_records": len(kalshi_data),
                "social_posts": len(social_data),
                "total_matches": len(results)
            }
        }

    except Exception as e:
        return {"error": str(e)}

# Cell 8: Test pure vector approach
result = kalshi_pure_vector_rag("New Jersey Governor Election")
print("\n💰 Pure Vector ROI Analysis:")
print(json.dumps(result, indent=2))

# Cell 9: Interactive Query Function
def interactive_roi_query():
    while True:
        question = input("\n💰 Enter your Kalshi question (or 'quit'): ")
        if question.lower() == 'quit':
            break
        
        result = kalshi_pure_vector_rag(question)
        
        if "error" in result:
            print(f"❌ Error: {result['error']}")
        else:
            analysis = result["roi_analysis"]
            print(f"\n🎯 Best Opportunity: {analysis.get('best_opportunity', 'N/A')}")
            print(f"📊 Expected ROI: {analysis.get('expected_roi_percentage', 0)}%")
            print(f"📈 Sentiment: {analysis.get('sentiment_momentum', 'N/A')}")
            print(f"🎲 Confidence: {analysis.get('confidence', 'N/A')}")
            print(f"💡 Reasoning: {analysis.get('reasoning', 'N/A')}")

# Uncomment to run interactive mode
# interactive_roi_query()

print("\n✅ Kalshi Pure Vector ROI Analysis System Ready!")
print("Use kalshi_pure_vector_rag('your question') for ROI-focused predictions")
