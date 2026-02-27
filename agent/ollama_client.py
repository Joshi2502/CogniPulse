import requests
import json

OLLAMA_URL = "http://ollama:11434/api/generate"

def ask_llm(prompt: str) -> str:
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": "tinyllama",
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
                "num_predict": 200
            }
        },
        timeout=60
    )

    data = response.json()

    print("\n🔎 RAW OLLAMA RESPONSE:")
    print(data)

    if "response" in data:
        return data["response"].strip()

    raise Exception(f"Unexpected Ollama response: {data}")
