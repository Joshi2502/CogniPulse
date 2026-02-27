ALLOWED_ACTIONS = {"LOG_ONLY", "NOTIFY", "COOLING", "SHUTDOWN"}

def validate_action(decision):
    action = decision.get("action_type", "LOG_ONLY")
    confidence = float(decision.get("confidence", 0.5))
    reasoning = decision.get("reasoning", "No reasoning provided")

    if action not in ALLOWED_ACTIONS:
        action = "LOG_ONLY"

    return {
        "action_type": action,
        "confidence": confidence,
        "reasoning": reasoning
    }
