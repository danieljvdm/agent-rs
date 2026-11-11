"""Independent stdio MCP fixture. All effects and control files live in argv[1]."""
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

root = Path(sys.argv[1]).resolve()
benchmark = "--benchmark" in sys.argv[2:]
benchmark_configuration = json.loads((root / "control.json").read_text()) if benchmark else None
lock = threading.Lock()
active = 0


def control():
    if benchmark:
        return benchmark_configuration
    path = root / "control.json"
    return json.loads(path.read_text()) if path.exists() else {}


def event(kind, **fields):
    if benchmark:
        return
    with lock:
        with (root / "events.jsonl").open("a") as output:
            output.write(json.dumps({"kind": kind, **fields}) + "\n")


def send(message, newline=True):
    with lock:
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", **message}) + ("\n" if newline else ""))
        sys.stdout.flush()


def definitions():
    def tool(name, properties, required):
        return {
            "name": name,
            "description": "Controlled " + name,
            "inputSchema": {"type": "object", "properties": properties,
                            "required": required, "additionalProperties": False},
            "outputSchema": {"type": "object", "properties": {"text": {"type": "string"}},
                             "required": ["text"], "additionalProperties": False},
            # Deliberately untrusted: tests must select policy themselves.
            "annotations": {"readOnlyHint": True, "idempotentHint": True},
        }
    string = {"type": "string", "minLength": 1}
    tools = [
        tool("echo", {"text": string, "hold": {"type": "boolean"},
                      "delay_ms": {"type": "integer", "minimum": 0}}, ["text"]),
        tool("read_file", {"path": string}, ["path"]),
        tool("write_file", {"path": string, "content": {"type": "string"},
                            "hold": {"type": "boolean"}}, ["path", "content"]),
    ]
    config = control()
    for index in range(len(tools), config.get("catalog_size", len(tools))):
        extra = json.loads(json.dumps(tools[0]))
        extra["name"] = f"echo_{index}"
        tools.append(extra)
    if config.get("schema_revision"):
        tools[0]["inputSchema"]["required"].append("new_argument")
        tools[0]["inputSchema"]["properties"]["new_argument"] = string
    if config.get("external_schema"):
        tools[0]["inputSchema"]["properties"]["text"] = {"$ref": "https://schemas.invalid/secret"}
    if config.get("bundled_schema"):
        tools[0]["inputSchema"]["$defs"] = {"word": string}
        tools[0]["inputSchema"]["properties"]["text"] = {"$ref": "#/$defs/word"}
    if config.get("removed_tool"):
        tools = tools[1:]
    return tools


def call(message):
    global active
    request_id = message["id"]
    params = message["params"]
    name, args = params["name"], params["arguments"]
    config = control()
    with lock:
        active += 1
        count = active
    event("call", name=name, id=request_id, active=count)
    text = args.get("text", "")
    if name in ("read_file", "write_file"):
        path = (root / args["path"]).resolve()
        assert path.is_relative_to(root)
        if name == "write_file":
            path.write_text(args["content"])
            text = "written"
            event("effect", name=name)
        else:
            text = path.read_text()
    mode = config.get("call_mode", "normal")
    if mode == "disconnect":
        os._exit(0)
    while args.get("hold") and not (root / "release").exists():
        time.sleep(0.005)
    if args.get("delay_ms", 0):
        time.sleep(args["delay_ms"] / 1000)
    result = {"content": [{"type": "text", "text": text}],
              "structuredContent": {"text": text}, "_meta": {"fixture": "retained"}}
    if mode == "reported_error":
        result = {"content": [{"type": "text", "text": "controlled failure"}], "isError": True}
    elif mode == "invalid_output":
        result["structuredContent"]["text"] = 42
    elif mode == "missing_content":
        del result["content"]
    elif mode == "large":
        result["content"][0]["text"] = "x" * 65536
    event("complete", name=name, id=request_id)
    with lock:
        active -= 1
    if mode == "rpc_error":
        send({"id": request_id, "error": {"code": -32602, "message": "controlled rejection", "data": {"reason": "test"}}})
    else:
        send({"id": request_id, "result": result}, newline=mode != "truncated")
    if mode == "truncated":
        os._exit(0)


event("started", pid=os.getpid())
if not benchmark and control().get("spawn_descendant"):
    # A descendant that stays in the server's process group, as a wrapper's interpreter would.
    descendant = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL)
    event("descendant", pid=descendant.pid)
for line in sys.stdin:
    message = json.loads(line)
    method = message["method"]
    if method == "initialize":
        config = control()
        event("initializing")
        while config.get("hold_initialization") and not (root / "release").exists():
            time.sleep(0.005)
        send({"id": message["id"], "result": {
            "protocolVersion": config.get("protocol", message["params"]["protocolVersion"]),
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": "controlled-filesystem", "version": config.get("server_version", "1")},
        }})
    elif method == "notifications/initialized":
        event("initialized")
    elif method == "tools/list":
        event("list")
        config = control()
        tools = definitions()
        result = {"tools": tools}
        mode = config.get("list_mode")
        if mode == "paginate":
            offset = int(message.get("params", {}).get("cursor") or 0)
            page_size = config.get("page_size", 1)
            result = {"tools": tools[offset:offset + page_size]}
            if offset + page_size < len(tools):
                result["nextCursor"] = str(offset + page_size)
        elif mode == "loop":
            result = {"tools": [], "nextCursor": "same"}
        elif mode == "duplicate":
            result["tools"].append(tools[0])
        if config.get("notify_on_list"):
            send({"method": "notifications/tools/list_changed"})
        send({"id": message["id"], "result": result})
    elif method == "tools/call":
        threading.Thread(target=call, args=(message,), daemon=True).start()
    elif method == "notifications/cancelled":
        event("cancelled", id=message["params"]["requestId"])
    elif "id" in message:
        send({"id": message["id"], "error": {"code": -32601, "message": "method not found"}})
