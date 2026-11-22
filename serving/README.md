# serving/

ML Platform · one image, three services picked by `API_SURFACE`: Feature API (`features.py`), Policy API (`policy.py`) and the MCP server (`mcp_server.py`) that agents call; shared models in `schemas.py`, entry point `app.py`. Predictions are served by KServe (`ml/serve.py`). See the repository map in the [root README](../README.md#repository-layout).
