import os
from typing import Any, Dict

import requests
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="CogniPulse LLM Reasoner", version="0.1")

MCP_BASE_URL = os.getenv("MCP_BASE_URL", "http://mcp-server:8080")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")


def _get_json(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {url} ({exc})")


def _build_prompt(device_id: str, state: Dict[str, Any], alerts: Dict[str, Any], actions: Dict[str, Any]) -> str:
    return f"""
You are a reliability analyst for an industrial IoT platform called CogniPulse.

Create a concise incident analysis for device {device_id} using the provided JSON inputs.
Output strict markdown with these sections:
1) Summary
2) Likely Root Cause
3) Recommended Next Actions
4) Risk Level (low/medium/high)

Current state JSON:
{state}

Recent alerts JSON:
{alerts}

Recent actions JSON:
{actions}

Rules:
- Be deterministic and factual from the inputs only.
- If data is sparse, state assumptions clearly.
- Do NOT invent sensor readings.
""".strip()


def _generate_with_ollama(prompt: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,
        },
    }

    try:
        resp = requests.post(f"{OLLAMA_BASE_URL}/api/generate", json=payload, timeout=90)
        resp.raise_for_status()
        body = resp.json()
        return body.get("response", "").strip()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Ollama generation failed: {exc}")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "mcp_base_url": MCP_BASE_URL,
        "ollama_base_url": OLLAMA_BASE_URL,
        "ollama_model": OLLAMA_MODEL,
    }


@app.get("/reason/device")
def reason_device(device_id: str = Query(...), limit: int = Query(10, ge=1, le=100)) -> Dict[str, Any]:
    state = _get_json(f"{MCP_BASE_URL}/latest/device_state", params={"device_id": device_id})
    alerts = _get_json(f"{MCP_BASE_URL}/latest/alerts", params={"limit": limit})
    actions = _get_json(f"{MCP_BASE_URL}/latest/actions", params={"limit": limit})

    prompt = _build_prompt(device_id=device_id, state=state, alerts=alerts, actions=actions)
    analysis_md = _generate_with_ollama(prompt)

    return {
        "device_id": device_id,
        "model": OLLAMA_MODEL,
        "analysis_markdown": analysis_md,
        "inputs": {
            "state": state,
            "alerts_count": alerts.get("count", 0),
            "actions_count": actions.get("count", 0),
        },
    }
