# agent.py
import boto3
import os
import json
import subprocess



def generate_embedding(text: str):
    """
    Use Amazon Titan Embeddings to get vector embeddings for a string.
    """
    bedrock = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))
    model_id = "amazon.titan-embed-text-v1"
    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps({"inputText": text}),
        contentType="application/json",
        accept="application/json"
    )
    result = json.loads(response["body"].read())
    embedding = result.get("embedding", [])
    return embedding

import subprocess

def summarize_topic(question: str) -> str:
    """
    Summarize a Kalshi question into a short, searchable topic
    (lowercase, underscores instead of spaces, max 20 chars).
    """
    prompt = f"Summarize this Kalshi question into a short, searchable topic (max 4 words, lowercase, no special characters):\n{question}"

    try:
        result = subprocess.run(
            ["ollama", "run", "phi3:latest"],
            input=prompt,
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout.strip()
        # Clean up: replace spaces with underscores, lowercase, truncate
        topic = output.replace("\n", " ").strip()
        topic = output.lower()
        # Fallback in case output is empty
      
    except Exception as e:
        print(f"⚠️ Ollama summarization failed: {e}")
        topic = question.lower().replace(" ", "_")[:20]

    return topic



