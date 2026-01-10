## CogniPulse (No LLM) + Redpanda MCP

```
[ event-simulator ]  -->  (cogni.events)  -->  [ cpp-analyzer ]  -->  (cogni.alerts)  -->  [ rule-engine ]  -->  (cogni.actions)
                                   |                         |                         |
                                   |                         +--> Postgres (events/alerts)
                                   +--> Mongo (device_state) +--> Mongo (alerts/actions)
                                                                 |
                                                                 +--> MCP Server (tools via rpk connect mcp-server)
```

MCP tools exposed:
- get_latest_alerts(limit?)
- get_latest_actions(limit?)
- publish_test_event(device_id?, temperature?, vibration?)
