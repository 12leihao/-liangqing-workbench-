"""
sync_entries.py - MQTT 数据持久化同步脚本

连接 MQTT broker，收集所有 retained messages，
合并到 entries.json 中，确保持久化存储。

用法: python sync_entries.py

配置来源（优先级从高到低）：
1. 环境变量: MQTT_HOST / MQTT_PORT / MQTT_USER / MQTT_PASS
2. 同目录 mqtt_config.json: {"host": "...", "port": 8883, "username": "...", "password": "..."}
3. 默认公共 broker（broker.emqx.io:8883，匿名）

迁移到 EMQX Cloud Serverless 时：
- 在脚本同目录创建 mqtt_config.json（该文件不要部署、不要提交到公共仓库），
  或在 GitHub Actions Secrets 中配置上述环境变量。
"""

import json
import os
import sys
import time
import ssl
import paho.mqtt.client as mqtt


def load_mqtt_config():
    """加载 MQTT 连接配置：环境变量 > mqtt_config.json

    不再静默回落到公共 broker。两处都没配到 host 时直接报错退出（exit 2）：
    公共 broker 的 retained 会被清理，静默回落会连到一个空 broker、收集 0 条、
    保留旧数据并以 0 退出，把"同步失败"伪装成"同步成功"，
    导致 GitHub Actions 一片绿却漏采所有新投稿。
    """
    host = os.environ.get("MQTT_HOST", "")
    port = os.environ.get("MQTT_PORT", "")
    user = os.environ.get("MQTT_USER", "")
    password = os.environ.get("MQTT_PASS", "")
    source = "环境变量"

    if not host:
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mqtt_config.json")
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                host = cfg.get("host", "")
                port = cfg.get("port", "")
                user = cfg.get("username", "")
                password = cfg.get("password", "")
                source = "mqtt_config.json"
            except Exception as e:
                print(f"Warning: failed to read {cfg_path}: {e}")

    if not host:
        print("ERROR: 未找到 MQTT broker 配置，拒绝回落到公共 broker。")
        print("  需要以下任一来源：")
        print("    1) 环境变量 MQTT_HOST / MQTT_PORT / MQTT_USER / MQTT_PASS")
        print("       GitHub Actions: Settings → Secrets and variables → Actions")
        print("    2) 脚本同目录 mqtt_config.json（本地运行用）")
        sys.exit(2)

    return {
        "host": host,
        "port": int(port or 8883),
        "username": user,
        "password": password,
        "source": source,
    }


MQTT_CONF = load_mqtt_config()
MQTT_HOST = MQTT_CONF["host"]
MQTT_PORT = MQTT_CONF["port"]
MQTT_USER = MQTT_CONF["username"]
MQTT_PASS = MQTT_CONF["password"]
MQTT_CONF_SOURCE = MQTT_CONF["source"]
MQTT_KEEPALIVE = 30

ENTRY_TOPIC = "liangqing-art-workbench/v2/entries"
ANNO_TOPIC = "liangqing-art-workbench/v2/annotations"
ACCOUNT_TOPIC = "liangqing-art-workbench/v2/accounts"
CAT_RENAME_TOPIC = "liangqing-art-workbench/v2/category_renames"
PERSONAL_PREFIX = "liangqing-art-workbench/v2/personal"
SUGGESTION_TOPIC = "liangqing-art-workbench/v2/suggestions"

ENTRIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "entries.json")

WAIT_TIMEOUT = 10  # 等待 retained messages 的秒数

# ===== 数据收集 =====
collected = {
    "entries": {},      # { entryId: data }
    "annotations": {},  # { entryId: { annoId: data } }
    "accounts": {},     # { username: data }
    "catRenames": {},
    "personal": {},     # { username: { entryId: data } }
    "suggestions": {}   # { sugId: data }
}

message_received = False
last_message_time = 0
connect_rc = None  # 记录连接结果码，用于区分"鉴权失败"与"确实没数据"


def on_connect(client, userdata, flags, rc):
    global connect_rc
    connect_rc = rc
    if rc != 0:
        # rc=4/5 通常是用户名口令错误或未授权。必须显式报错：
        # 否则脚本会以 0 条数据继续、保留旧 entries.json 并以 0 退出，
        # 让 GitHub Actions 显示成功，掩盖凭据失效。
        print(f"ERROR: MQTT 连接失败 rc={rc}（4=用户名/口令无效，5=未授权）")
        return
    print("MQTT connected, subscribing to topics...")
    client.subscribe(ENTRY_TOPIC + "/#", qos=0)
    client.subscribe(ANNO_TOPIC + "/#", qos=0)
    client.subscribe(CAT_RENAME_TOPIC, qos=0)
    client.subscribe(SUGGESTION_TOPIC + "/#", qos=0)
    # 注：账户（accounts/#）与个人库（personal/#）不再同步 ——
    # 账户体系已改为浏览器本地；entries.json 可能随公开仓库发布，绝不带敏感数据


def on_message(client, userdata, msg):
    global message_received, last_message_time
    message_received = True
    last_message_time = time.time()

    topic = msg.topic
    payload = msg.payload

    try:
        if topic.startswith(ENTRY_TOPIC + "/"):
            entry_id = topic.split("/")[-1]
            if len(payload) == 0:
                collected["entries"].pop(entry_id, None)
            else:
                collected["entries"][entry_id] = json.loads(payload.decode("utf-8"))

        elif topic.startswith(ANNO_TOPIC + "/"):
            parts = topic.split("/")
            anno_id = parts[-1]
            entry_id = parts[-2]
            if len(payload) == 0:
                if entry_id in collected["annotations"]:
                    collected["annotations"][entry_id].pop(anno_id, None)
            else:
                if entry_id not in collected["annotations"]:
                    collected["annotations"][entry_id] = {}
                collected["annotations"][entry_id][anno_id] = json.loads(payload.decode("utf-8"))

        elif topic.startswith(ACCOUNT_TOPIC + "/"):
            username = topic.split("/")[-1]
            if len(payload) == 0:
                collected["accounts"].pop(username, None)
            else:
                collected["accounts"][username] = json.loads(payload.decode("utf-8"))

        elif topic == CAT_RENAME_TOPIC:
            if len(payload) == 0:
                collected["catRenames"] = {}
            else:
                collected["catRenames"] = json.loads(payload.decode("utf-8"))

        elif topic.startswith(PERSONAL_PREFIX + "/"):
            parts = topic.split("/")
            # liangqing-art-workbench/v2/personal/{username}/entries/{entryId}
            entry_id = parts[-1]
            username = parts[-3]
            if len(payload) == 0:
                if username in collected["personal"]:
                    collected["personal"][username].pop(entry_id, None)
            else:
                if username not in collected["personal"]:
                    collected["personal"][username] = {}
                collected["personal"][username][entry_id] = json.loads(payload.decode("utf-8"))

        elif topic.startswith(SUGGESTION_TOPIC + "/"):
            sug_id = topic.split("/")[-1]
            if len(payload) == 0:
                collected["suggestions"].pop(sug_id, None)
            else:
                collected["suggestions"][sug_id] = json.loads(payload.decode("utf-8"))

    except Exception as e:
        print(f"Error processing message on {topic}: {e}")


def on_disconnect(client, userdata, rc):
    print(f"MQTT disconnected (rc={rc})")


def load_existing():
    """Load existing entries.json"""
    try:
        with open(ENTRIES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"entries": []}


def merge_data(existing):
    """Merge MQTT collected data into existing entries.json"""
    # Keep existing static entries
    result = {
        "entries": existing.get("entries", []),
    }

    # Merge community entries: start with existing, update/add from MQTT
    existing_community = {}
    for entry in existing.get("communityEntries", []):
        existing_community[str(entry.get("id", ""))] = entry

    for entry_id, data in collected["entries"].items():
        existing_community[entry_id] = {
            "id": entry_id,
            "title": data.get("title", ""),
            "excerpt": data.get("content", ""),
            "summary": data.get("note", ""),
            "source": data.get("source", ""),
            "category": data.get("category", "其他"),
            "author": data.get("author", "匿名"),
            "date": data.get("date", ""),
            "createdAt": data.get("createdAt", ""),
            "updatedAt": data.get("updatedAt", ""),
            "tags": data.get("tags", []),
            "isCommunity": True
        }

    result["communityEntries"] = list(existing_community.values())

    # 批注：合并已有数据与 MQTT 收集数据（MQTT 清理时不丢已持久化的批注）
    merged_annos = existing.get("annotations", {})
    for entry_id, annos in collected["annotations"].items():
        if entry_id not in merged_annos:
            merged_annos[entry_id] = {}
        merged_annos[entry_id].update(annos)
    # 删除操作：MQTT 中已清空的主题，从持久化数据中同步删除
    result["annotations"] = merged_annos

    # 分类重命名：MQTT 有数据时覆盖（单一 retained 消息，全量语义）
    if collected["catRenames"]:
        result["catRenames"] = collected["catRenames"]
    else:
        result["catRenames"] = existing.get("catRenames", {})

    # 账户不再收集（账户体系已改为浏览器本地）；entries.json 中保持为空
    result["accounts"] = {}

    # 个人库数据已不再从 MQTT 收集（隐私数据仅存用户浏览器）
    result["personal"] = existing.get("personal", {})

    # 建议箱：合并已有数据（防止 broker 清空导致丢失）
    merged_sugs = existing.get("suggestions", {})
    for sug_id, data in collected["suggestions"].items():
        merged_sugs[sug_id] = data
    result["suggestions"] = merged_sugs

    return result


def main():
    print(f"=== MQTT Sync Script ===")
    print(f"Broker: {MQTT_HOST}:{MQTT_PORT} (config source: {MQTT_CONF_SOURCE})")
    print(f"Output: {ENTRIES_FILE}")
    print()

    # Create MQTT client
    client = mqtt.Client(
        client_id="liangqing_sync_" + str(int(time.time())),
        clean_session=True
    )

    # 认证（配置了用户名时启用，EMQX Cloud 等私有部署需要）
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    # Set TLS for secure connection
    context = ssl.create_default_context()
    client.tls_set_context(context)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Connect
    try:
        client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
    except Exception as e:
        print(f"Failed to connect: {e}")
        sys.exit(1)

    # Start network loop
    client.loop_start()

    # Wait for retained messages
    print(f"Waiting {WAIT_TIMEOUT}s for retained messages...")
    start_time = time.time()
    while time.time() - start_time < WAIT_TIMEOUT:
        if message_received and time.time() - last_message_time > 3:
            # No new messages for 3 seconds, assume all retained messages received
            print("All retained messages received (no new messages for 3s)")
            break
        time.sleep(0.5)

    elapsed = time.time() - start_time
    print(f"Waited {elapsed:.1f}s")

    # Stop MQTT
    client.loop_stop()
    client.disconnect()

    # 连接结果硬校验：鉴权失败或从未连上时绝不继续写文件
    if connect_rc is None:
        print("ERROR: 未收到 CONNACK，连接可能超时或被 broker 拒绝。")
        print("  现有 entries.json 未修改，脚本以非 0 退出以让 CI 失败可见。")
        sys.exit(1)
    if connect_rc != 0:
        print(f"ERROR: MQTT 鉴权/连接失败 rc={connect_rc}，放弃本轮同步。")
        print("  请检查 MQTT_USER / MQTT_PASS 是否有效。")
        sys.exit(1)

    # Print collected stats
    print()
    print("=== Collected from MQTT ===")
    print(f"  Community entries: {len(collected['entries'])}")
    print(f"  Annotations: {sum(len(v) for v in collected['annotations'].values())}")
    print(f"  Accounts: {len(collected['accounts'])}")
    print(f"  Cat renames: {len(collected['catRenames'])}")
    print(f"  Personal entries: {sum(len(v) for v in collected['personal'].values())}")
    print(f"  Suggestions: {len(collected['suggestions'])}")

    # Merge with existing data
    existing = load_existing()
    print()
    print(f"=== Existing entries.json ===")
    print(f"  Static entries: {len(existing.get('entries', []))}")
    print(f"  Community entries: {len(existing.get('communityEntries', []))}")

    merged = merge_data(existing)

    print()
    print(f"=== Merged result ===")
    print(f"  Static entries: {len(merged['entries'])}")
    print(f"  Community entries: {len(merged['communityEntries'])}")
    print(f"  Annotations: {sum(len(v) for v in merged.get('annotations', {}).values())}")

    # Write to file
    with open(ENTRIES_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print()
    print(f"✓ entries.json updated: {ENTRIES_FILE}")


if __name__ == "__main__":
    main()
