# SPDX-License-Identifier: GPL-3.0-or-later
"""Local web UI for mesh-mqtt-proxy.

Runs inside the proxy process so it can share the proxy's Bluetooth connection.
(A Meshtastic node accepts only one BLE connection at a time, so a separate web
app could not talk to the node while the proxy is running.)

Only the Python standard library is used, to stay light on a Raspberry Pi 2.

    GET  /                  the single-page UI (web/index.html)
    GET  /api/status        connection state, broker, counters, node and Pi info
    GET  /api/logs          log lines (?after=<id>&level=INFO|WARNING|ERROR)
    GET  /api/nodes         nodes the connected node has heard, with distance
    GET  /api/settings      node settings the UI can edit (passwords are never sent)
    POST /api/settings      change settings; the node reboots to apply them
    POST /api/reconnect     drop and re-establish the Bluetooth link

Security: HTTP Basic auth (required unless bound to loopback), a required
X-Requested-With header on every POST (blocks cross-site form posts), a Host
check when no password is set (blocks DNS rebinding), and no secret is ever
sent back to the browser.
"""
from __future__ import annotations

import base64
import collections
import hmac
import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

log = logging.getLogger("mesh-mqtt-proxy.web")

INDEX_PATH = Path(__file__).resolve().parent / "web" / "index.html"
MAX_BODY = 64 * 1024
LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


# --------------------------------------------------------------------------
# Web settings
# --------------------------------------------------------------------------
@dataclass
class WebSettings:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8080
    username: str = "admin"
    password: str = ""
    log_lines: int = 3000
    latitude: Optional[float] = None     # fallback origin for distances if the node has no position
    longitude: Optional[float] = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "WebSettings":
        return cls(
            enabled=bool(raw.get("enabled", False)),
            host=str(raw.get("host", "127.0.0.1")),
            port=int(raw.get("port", 8080)),
            username=str(raw.get("username", "admin")),
            password=str(raw.get("password", "")),
            log_lines=int(raw.get("log_lines", 3000)),
            latitude=(float(raw["latitude"]) if raw.get("latitude") is not None else None),
            longitude=(float(raw["longitude"]) if raw.get("longitude") is not None else None),
        )

    @property
    def loopback_only(self) -> bool:
        return self.host in ("127.0.0.1", "::1", "localhost")


# --------------------------------------------------------------------------
# In-memory log buffer
# --------------------------------------------------------------------------
class LogBuffer(logging.Handler):
    """Keeps the most recent log records so the UI can show them."""

    def __init__(self, maxlen: int = 3000):
        super().__init__(level=logging.DEBUG)
        self._lines: collections.deque[dict[str, Any]] = collections.deque(maxlen=maxlen)
        self._next_id = 1
        self._mu = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001
            msg = str(record.msg)
        with self._mu:
            self._lines.append({
                "id": self._next_id,
                "t": record.created,
                "level": record.levelname,
                "logger": record.name,
                "msg": msg,
            })
            self._next_id += 1

    def since(self, after: int = 0, min_level: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        with self._mu:
            out = [x for x in self._lines
                   if x["id"] > after and LEVELS.get(x["level"], 0) >= min_level]
        return out[-limit:]


# --------------------------------------------------------------------------
# Settings model: what the UI may read and change
# --------------------------------------------------------------------------
class ApiError(Exception):
    def __init__(self, status: int, message: str, **extra: Any):
        super().__init__(message)
        self.status = status
        self.message = message
        self.extra = extra


MQTT_BOOLS = ("enabled", "proxy_to_client_enabled", "map_reporting_enabled",
              "encryption_enabled", "json_enabled", "tls_enabled")
MQTT_STRS = {"address": 63, "username": 63, "root": 31}       # field -> max bytes
MQTT_SECRETS = ("password",)                                  # write-only
MAP_BOOLS = ("should_report_location",)
VALID_PRECISION = [0] + list(range(10, 20)) + [32]
MIN_PUBLISH_INTERVAL = 3600
MAX_PUBLISH_INTERVAL = 7 * 24 * 3600

PRECISION_TEXT = {
    0: "location not shared", 10: "23.3 km", 11: "11.7 km", 12: "5.8 km",
    13: "2.9 km", 14: "1.5 km", 15: "729 m", 16: "364 m", 17: "182 m",
    18: "91 m", 19: "45 m", 32: "exact",
}


def _pb():
    from meshtastic.protobuf import channel_pb2, config_pb2

    return channel_pb2, config_pb2


def _enum_name(enum_type: Any, value: int) -> str:
    try:
        return enum_type.Name(value)
    except ValueError:
        return str(value)


def _get(obj: Any, path: str) -> Any:
    for part in path.split("."):
        obj = getattr(obj, part)
    return obj


def _set(obj: Any, path: str, value: Any) -> None:
    *parents, leaf = path.split(".")
    for part in parents:
        obj = getattr(obj, part)
    setattr(obj, leaf, value)


def _channel_key_info(settings: Any) -> str:
    n = len(settings.psk)
    if n == 0:
        return "none (unencrypted)"
    if n == 1:
        return "default public key" if settings.psk == b"\x01" else "simple key"
    return f"{n * 8}-bit custom key"


def read_settings(iface: Any) -> dict[str, Any]:
    """Current editable node settings as plain JSON (no secrets)."""
    channel_pb2, config_pb2 = _pb()
    node = iface.localNode
    mq = node.moduleConfig.mqtt
    lora = node.localConfig.lora
    rs = mq.map_report_settings

    channels = []
    for i, ch in enumerate(node.channels or []):
        if ch.role == channel_pb2.Channel.Role.DISABLED:
            continue
        channels.append({
            "index": i,
            "role": _enum_name(channel_pb2.Channel.Role, ch.role),
            "name": ch.settings.name,
            "key": _channel_key_info(ch.settings),
            "uplink_enabled": ch.settings.uplink_enabled,
            "downlink_enabled": ch.settings.downlink_enabled,
            "position_precision": ch.settings.module_settings.position_precision,
        })

    return {
        "mqtt": {
            **{k: getattr(mq, k) for k in MQTT_BOOLS},
            **{k: getattr(mq, k) for k in MQTT_STRS},
            "password_set": bool(mq.password),
            "map_report_settings": {
                "publish_interval_secs": rs.publish_interval_secs,
                "position_precision": rs.position_precision,
                "should_report_location": rs.should_report_location,
            },
        },
        "lora": {
            "config_ok_to_mqtt": lora.config_ok_to_mqtt,
            "region": _enum_name(config_pb2.Config.LoRaConfig.RegionCode, lora.region),
            "modem_preset": (_enum_name(config_pb2.Config.LoRaConfig.ModemPreset, lora.modem_preset)
                             if lora.use_preset else "custom"),
        },
        "channels": channels,
        "owner": _owner(iface),
        "precision_text": {str(k): v for k, v in PRECISION_TEXT.items()},
    }


def _owner(iface: Any) -> dict[str, Any]:
    user = None
    try:
        user = iface.getMyUser()
    except Exception:  # noqa: BLE001
        pass
    user = user or {}
    return {"long_name": user.get("longName", ""), "short_name": user.get("shortName", ""),
            "known": bool(user)}


# ---- validation ----------------------------------------------------------
def _want_bool(v: Any, what: str) -> bool:
    if not isinstance(v, bool):
        raise ApiError(400, f"{what} must be true or false")
    return v


def _want_precision(v: Any, what: str) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v not in VALID_PRECISION:
        raise ApiError(400, f"{what} must be 0, 10-19 or 32")
    return v


def _want_str(v: Any, what: str, max_bytes: int) -> str:
    if not isinstance(v, str):
        raise ApiError(400, f"{what} must be text")
    v = v.strip()
    if len(v.encode()) > max_bytes:
        raise ApiError(400, f"{what} is too long (max {max_bytes} bytes)")
    if any(ord(c) < 32 for c in v):
        raise ApiError(400, f"{what} contains control characters")
    return v


@dataclass
class Plan:
    changes: list[dict[str, Any]]
    risks: list[str]
    warnings: list[str]
    sections: set[str]            # "mqtt", "lora"
    channels: set[int]
    owner: Optional[dict[str, str]]
    # (section, path, value) tuples to apply to the local protobufs
    sets: list[tuple[str, str, Any]]
    channel_sets: list[tuple[int, str, Any]]


def plan_changes(iface: Any, payload: dict[str, Any]) -> Plan:
    """Validate a settings payload and work out what would change.

    Nothing is modified here.  Unknown keys are rejected rather than ignored,
    so a typo can never silently do nothing.
    """
    channel_pb2, _ = _pb()
    node = iface.localNode
    mq = node.moduleConfig.mqtt
    lora = node.localConfig.lora
    plan = Plan([], [], [], set(), set(), None, [], [])

    def note(section: str, label: str, path: str, old: Any, new: Any, secret: bool = False) -> None:
        plan.changes.append({
            "section": section, "label": label,
            "old": "(hidden)" if secret else old, "new": "(hidden)" if secret else new,
        })

    allowed_top = {"mqtt", "lora", "channels", "owner", "confirm"}
    extra = set(payload) - allowed_top
    if extra:
        raise ApiError(400, f"unknown field(s): {', '.join(sorted(extra))}")

    # ---- MQTT ------------------------------------------------------------
    m = payload.get("mqtt") or {}
    if not isinstance(m, dict):
        raise ApiError(400, "mqtt must be an object")
    known = set(MQTT_BOOLS) | set(MQTT_STRS) | set(MQTT_SECRETS) | {"map_report_settings"}
    if set(m) - known:
        raise ApiError(400, f"unknown mqtt field(s): {', '.join(sorted(set(m) - known))}")
    for k, v in m.items():
        if k in MQTT_BOOLS:
            new = _want_bool(v, f"mqtt.{k}")
        elif k in MQTT_STRS:
            new = _want_str(v, f"mqtt.{k}", MQTT_STRS[k])
        elif k in MQTT_SECRETS:
            new = _want_str(v, f"mqtt.{k}", 63)
            if new == "":
                continue  # blank = keep the current password
        else:
            continue
        old = getattr(mq, k)
        if new != old:
            note("MQTT", k, f"mqtt.{k}", old, new, secret=k in MQTT_SECRETS)
            plan.sets.append(("mqtt", k, new))
            plan.sections.add("mqtt")

    rs_in = m.get("map_report_settings") or {}
    if not isinstance(rs_in, dict):
        raise ApiError(400, "mqtt.map_report_settings must be an object")
    rs_known = {"publish_interval_secs", "position_precision", "should_report_location"}
    if set(rs_in) - rs_known:
        raise ApiError(400, f"unknown map_report_settings field(s): "
                            f"{', '.join(sorted(set(rs_in) - rs_known))}")
    rs = mq.map_report_settings
    for k, v in rs_in.items():
        if k == "publish_interval_secs":
            if isinstance(v, bool) or not isinstance(v, int) or not (
                    MIN_PUBLISH_INTERVAL <= v <= MAX_PUBLISH_INTERVAL):
                raise ApiError(400, f"map report interval must be {MIN_PUBLISH_INTERVAL}"
                                    f"-{MAX_PUBLISH_INTERVAL} seconds")
            new = v
        elif k == "position_precision":
            new = _want_precision(v, "map report precision")
        else:
            new = _want_bool(v, f"map_report_settings.{k}")
        old = getattr(rs, k)
        if new != old:
            note("Map report", k, f"mqtt.map_report_settings.{k}", old, new)
            plan.sets.append(("mqtt", f"map_report_settings.{k}", new))
            plan.sections.add("mqtt")

    # ---- LoRa (only the MQTT consent flag) -------------------------------
    lo = payload.get("lora") or {}
    if not isinstance(lo, dict) or set(lo) - {"config_ok_to_mqtt"}:
        raise ApiError(400, "only lora.config_ok_to_mqtt can be changed here")
    if "config_ok_to_mqtt" in lo:
        new = _want_bool(lo["config_ok_to_mqtt"], "lora.config_ok_to_mqtt")
        if new != lora.config_ok_to_mqtt:
            note("LoRa", "config_ok_to_mqtt", "lora.config_ok_to_mqtt", lora.config_ok_to_mqtt, new)
            plan.sets.append(("lora", "config_ok_to_mqtt", new))
            plan.sections.add("lora")

    # ---- channels --------------------------------------------------------
    chans = payload.get("channels") or {}
    if not isinstance(chans, dict):
        raise ApiError(400, "channels must be an object keyed by channel index")
    for key, cin in chans.items():
        try:
            idx = int(key)
            ch = node.channels[idx]
        except (ValueError, IndexError, TypeError):
            raise ApiError(400, f"no such channel: {key}") from None
        if ch.role == channel_pb2.Channel.Role.DISABLED:
            raise ApiError(400, f"channel {idx} is disabled")
        if not isinstance(cin, dict) or set(cin) - {"uplink_enabled", "downlink_enabled",
                                                    "position_precision"}:
            raise ApiError(400, f"channel {idx}: only uplink, downlink and position precision "
                                f"can be changed here")
        for k, v in cin.items():
            if k == "position_precision":
                new = _want_precision(v, f"channel {idx} position precision")
                old = ch.settings.module_settings.position_precision
                path = "module_settings.position_precision"
            else:
                new = _want_bool(v, f"channel {idx} {k}")
                old = getattr(ch.settings, k)
                path = k
            if new != old:
                label = ch.settings.name or f"channel {idx}"
                note(f"Channel {idx} ({label})", k, path, old, new)
                plan.channel_sets.append((idx, path, new))
                plan.channels.add(idx)

    # ---- owner names -----------------------------------------------------
    ow = payload.get("owner") or {}
    if not isinstance(ow, dict) or set(ow) - {"long_name", "short_name"}:
        raise ApiError(400, "owner may contain long_name and short_name")
    if ow:
        cur = _owner(iface)
        new_owner: dict[str, str] = {}
        if "long_name" in ow:
            v = _want_str(ow["long_name"], "long name", 39)
            if not v:
                raise ApiError(400, "long name cannot be empty")
            if v != cur["long_name"]:
                new_owner["long_name"] = v
                note("Device", "long_name", "owner", cur["long_name"], v)
        if "short_name" in ow:
            v = _want_str(ow["short_name"], "short name", 4)
            if not v:
                raise ApiError(400, "short name cannot be empty")
            if v != cur["short_name"]:
                new_owner["short_name"] = v
                note("Device", "short_name", "owner", cur["short_name"], v)
        plan.owner = new_owner or None

    _assess(iface, plan, payload)
    return plan


def _effective(node: Any, plan: Plan) -> tuple[dict[str, Any], dict[int, dict[str, Any]]]:
    """Node settings as they would be after the plan is applied."""
    mq = node.moduleConfig.mqtt
    eff = {
        "enabled": mq.enabled, "proxy": mq.proxy_to_client_enabled,
        "map": mq.map_reporting_enabled,
        "precision": mq.map_report_settings.position_precision,
        "locate": mq.map_report_settings.should_report_location,
        "ok": node.localConfig.lora.config_ok_to_mqtt,
    }
    for section, path, val in plan.sets:
        key = {"enabled": "enabled", "proxy_to_client_enabled": "proxy",
               "map_reporting_enabled": "map",
               "map_report_settings.position_precision": "precision",
               "map_report_settings.should_report_location": "locate",
               "config_ok_to_mqtt": "ok"}.get(path)
        if key:
            eff[key] = val
    chans: dict[int, dict[str, Any]] = {}
    for i, ch in enumerate(node.channels or []):
        chans[i] = {
            "role": ch.role, "name": ch.settings.name,
            "uplink": ch.settings.uplink_enabled,
            "precision": ch.settings.module_settings.position_precision,
        }
    for idx, path, val in plan.channel_sets:
        chans[idx]["uplink" if path == "uplink_enabled" else
               "precision" if path.endswith("precision") else "downlink"] = val
    return eff, chans


def _assess(iface: Any, plan: Plan, payload: dict[str, Any]) -> None:
    """Fill plan.risks (need explicit confirmation) and plan.warnings (advice)."""
    channel_pb2, _ = _pb()
    node = iface.localNode
    eff, chans = _effective(node, plan)
    cur_eff, cur_chans = _effective(node, Plan([], [], [], set(), set(), None, [], []))

    if cur_eff["proxy"] and not eff["proxy"]:
        plan.risks.append("Turning off 'MQTT client proxy' means this Pi has nothing to relay: "
                          "the node will stop publishing to MQTT (and off the map) until it is on again.")
    if cur_eff["enabled"] and not eff["enabled"]:
        plan.risks.append("Turning off MQTT disables all MQTT output from the node, including map reports.")
    for idx, c in chans.items():
        was = cur_chans[idx]
        if c["uplink"] and not was["uplink"]:
            if c["role"] != channel_pb2.Channel.Role.PRIMARY:
                plan.risks.append(
                    f"Uplink on channel {idx} ({c['name'] or 'unnamed'}) is not the primary channel. "
                    f"Everything the node hears on it will be published to the MQTT server. "
                    f"Do not do this for a private channel.")
            elif c["precision"] == 32:
                plan.risks.append(
                    f"Channel {idx} shares exact positions and you are enabling uplink on it, "
                    f"so exact positions will be published to the MQTT server.")
        elif c["uplink"] and c["precision"] == 32 and was["precision"] != 32:
            plan.risks.append(
                f"Channel {idx} has uplink on and you are making its position exact, "
                f"so exact positions will be published to the MQTT server.")

    if eff["map"]:
        if not eff["enabled"] or not eff["proxy"]:
            plan.warnings.append("Map reporting is on, but MQTT or the client proxy is off, so nothing will be sent.")
        if not eff["locate"]:
            plan.warnings.append("Map reporting is on but 'share location' (should_report_location) is off; "
                                 "the node will probably not send map reports.")
        if not eff["ok"]:
            plan.warnings.append("'OK to MQTT' is off; meshmap.net lists it as a requirement.")
        if eff["precision"] and not 10 <= eff["precision"] <= 16:
            plan.warnings.append("meshmap.net accepts map precision between 10 and 16 bits (23.3 km to 364 m); "
                                 "finer values may be rejected or coarsened.")
        if eff["precision"] == 0:
            plan.warnings.append("Map precision 0 means the location is not shared, so you will not appear on the map.")


def apply_plan(bridge: Any, iface: Any, plan: Plan) -> None:
    """Write the planned changes to the node.  The node reboots to apply them."""
    node = iface.localNode
    _ensure_session(iface, node)
    try:
        if plan.owner:
            node.setOwner(**plan.owner)
        for section, path, val in plan.sets:
            target = node.moduleConfig.mqtt if section == "mqtt" else node.localConfig.lora
            _set(target, path, val)
        for idx, path, val in plan.channel_sets:
            _set(node.channels[idx].settings, path, val)

        node.beginSettingsTransaction()
        for section in sorted(plan.sections):
            node.writeConfig(section)
        for idx in sorted(plan.channels):
            node.writeChannel(idx)
        node.commitSettingsTransaction()
    except BaseException:
        # Local copy may now differ from the node; reload it from the node.
        bridge.request_reconnect()
        raise
    # The node reboots to apply the change; the BLE link drops and the bridge reconnects.
    bridge.request_reconnect()


def _ensure_session(iface: Any, node: Any) -> None:
    """Get the admin session key before writing (the node requires it)."""
    from meshtastic.util import to_node_num

    try:
        node.ensureSessionKey()
        num = to_node_num(node.nodeNum)
        for _ in range(50):
            if iface._getOrCreateByNum(num).get("adminSessionPassKey") is not None:
                return
            time.sleep(0.1)
        log.debug("no admin session key after 5s; writing anyway")
    except Exception:  # noqa: BLE001
        log.debug("session key request failed", exc_info=True)


# --------------------------------------------------------------------------
# Node list
# --------------------------------------------------------------------------
ONLINE_SECS = 2 * 3600          # the Meshtastic apps call a node "online" if heard in the last 2 h
EARTH_RADIUS_M = 6371008.8


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial compass bearing from point 1 to point 2, 0 = north, clockwise."""
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def node_position(node: dict[str, Any]) -> Optional[tuple[float, float, Optional[float]]]:
    """(lat, lon, altitude) of a node dict, or None if it has no usable position."""
    p = node.get("position") or {}
    lat, lon = p.get("latitude"), p.get("longitude")
    if lat is None and p.get("latitudeI") is not None:
        lat = p["latitudeI"] * 1e-7
    if lon is None and p.get("longitudeI") is not None:
        lon = p["longitudeI"] * 1e-7
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        return None
    alt = p.get("altitude")
    return float(lat), float(lon), (float(alt) if isinstance(alt, (int, float)) else None)


def _num(v: Any) -> Optional[float]:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _snapshot(d: Optional[dict]) -> list[tuple[Any, Any]]:
    """Items of a dict that the library's thread may be adding to while we read it."""
    if not d:
        return []
    for _ in range(20):
        try:
            return list(d.copy().items())        # dict.copy() is one atomic step under the GIL
        except RuntimeError:
            continue
    return []


def build_node_list(iface: Any, origin: Optional[tuple[float, float]] = None) -> dict[str, Any]:
    """Snapshot of iface.nodesByNum as plain JSON, with distance from our own node.

    The distance origin is the connected node's own position; if it has none we
    fall back to [web] latitude/longitude from the config.
    """
    now = time.time()
    raw = _snapshot(getattr(iface, "nodesByNum", None))
    my_num = getattr(getattr(iface, "myInfo", None), "my_node_num", None)

    me_pos = None
    for num, n in raw:
        if num == my_num:
            me_pos = node_position(n)
    origin_src = None
    if me_pos:
        origin, origin_src = (me_pos[0], me_pos[1]), "node"
    elif origin:
        origin_src = "config"

    nodes = []
    for num, n in raw:
        user = n.get("user") or {}
        dm = n.get("deviceMetrics") or {}
        pos = node_position(n)
        last = _num(n.get("lastHeard")) or None
        row: dict[str, Any] = {
            "num": num,
            "id": user.get("id") or f"!{num:08x}",
            "long_name": user.get("longName") or "",
            "short_name": user.get("shortName") or "",
            "hw": user.get("hwModel") or "",
            "role": user.get("role") or "CLIENT",
            "last_heard": last,
            "snr": _num(n.get("snr")),
            "hops": n.get("hopsAway") if isinstance(n.get("hopsAway"), int) else None,
            "via_mqtt": bool(n.get("viaMqtt")),
            "favorite": bool(n.get("isFavorite")),
            "ignored": bool(n.get("isIgnored")),
            "encrypted": bool(user.get("publicKey")),
            "battery": _num(dm.get("batteryLevel")),
            "voltage": _num(dm.get("voltage")),
            "ch_util": _num(dm.get("channelUtilization")),
            "air_util": _num(dm.get("airUtilTx")),
            "uptime": _num(dm.get("uptimeSeconds")),
            "lat": pos[0] if pos else None,
            "lon": pos[1] if pos else None,
            "alt": pos[2] if pos else None,
            "distance_m": None,
            "bearing": None,
            "is_me": num == my_num,
        }
        if pos and origin and num != my_num:
            row["distance_m"] = round(haversine_m(origin[0], origin[1], pos[0], pos[1]))
            row["bearing"] = round(bearing_deg(origin[0], origin[1], pos[0], pos[1]))
        nodes.append(row)

    return {
        "now": now,
        "me": my_num,
        "origin": ({"lat": origin[0], "lon": origin[1], "source": origin_src} if origin else None),
        "total": len(nodes),
        "online": sum(1 for x in nodes if x["last_heard"] and now - x["last_heard"] <= ONLINE_SECS),
        "nodes": nodes,
    }


# --------------------------------------------------------------------------
# API implementation (independent of HTTP, so it can be unit-tested)
# --------------------------------------------------------------------------
def _read_first(path: str) -> Optional[str]:
    try:
        with open(path) as fh:
            return fh.read().strip()
    except OSError:
        return None


def system_info() -> dict[str, Any]:
    info: dict[str, Any] = {}
    la = _read_first("/proc/loadavg")
    if la:
        info["load"] = [float(x) for x in la.split()[:3]]
    mem = _read_first("/proc/meminfo")
    if mem:
        kv = {ln.split(":")[0]: int(re.findall(r"\d+", ln)[0]) for ln in mem.splitlines()
              if re.findall(r"\d+", ln)}
        if "MemTotal" in kv and "MemAvailable" in kv:
            info["mem_total_mb"] = kv["MemTotal"] // 1024
            info["mem_used_mb"] = (kv["MemTotal"] - kv["MemAvailable"]) // 1024
    temp = _read_first("/sys/class/thermal/thermal_zone0/temp")
    if temp and temp.lstrip("-").isdigit():
        info["cpu_temp_c"] = round(int(temp) / 1000, 1)
    up = _read_first("/proc/uptime")
    if up:
        info["host_uptime_s"] = int(float(up.split()[0]))
    try:
        info["hostname"] = os.uname().nodename
    except AttributeError:
        pass
    return info


def _node_summary(iface: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        num = getattr(getattr(iface, "myInfo", None), "my_node_num", 0)
        if num:
            out["id"] = f"!{num:08x}"
        md = getattr(iface, "metadata", None)
        if md is not None:
            out["firmware"] = md.firmware_version
            from meshtastic.protobuf import mesh_pb2

            out["hardware"] = _enum_name(mesh_pb2.HardwareModel, md.hw_model)
        out.update({k: v for k, v in _owner(iface).items() if k != "known"})
    except Exception:  # noqa: BLE001
        log.debug("node summary failed", exc_info=True)
    return out


class Api:
    def __init__(self, bridge: Any, logs: LogBuffer, origin: Optional[tuple[float, float]] = None):
        self.bridge = bridge
        self.logs = logs
        self.origin = origin                     # configured fallback (lat, lon)
        self._nodes_cache: Optional[dict[str, Any]] = None

    def status(self) -> dict[str, Any]:
        b = self.bridge
        now = time.time()
        iface = b.current_interface()
        broker = None
        if b.broker is not None:
            broker = {"host": b.broker.host, "port": b.broker.port,
                      "tls": b.broker.tls, "root": b.broker.root}
        return {
            "now": now,
            "service_uptime_s": int(now - b.started_at),
            "state": b.state,
            "state_since": b.state_since,
            "retry_at": b.retry_at,
            "last_error": b.last_error,
            "address": b.s.address,
            "downlink": b.s.downlink,
            "broker": broker,
            "broker_connected": b.mqtt_connected(),
            "stats": dict(b.stats),
            "by_kind": dict(b.kinds),
            "last_map_report": b.last_map_report,
            "recent": list(b.recent)[-20:][::-1],
            "node": _node_summary(iface) if iface is not None else None,
            "system": system_info(),
        }

    def nodes(self) -> dict[str, Any]:
        """The connected node's node list, with distance and bearing from it."""
        iface = self.bridge.current_interface()
        if iface is None:
            if self._nodes_cache is not None:          # keep showing the last list while it reboots
                return {**self._nodes_cache, "stale": True, "stale_since": self._nodes_cache["now"]}
            raise ApiError(503, f"Not connected to the node right now ({self.bridge.state}).")
        if not getattr(self.bridge, "want_nodes", True):
            return {"enabled": False, "nodes": [], "total": 0, "online": 0, "now": time.time()}
        out = build_node_list(iface, self.origin)
        out["enabled"] = True
        self._nodes_cache = out
        return out

    def settings(self) -> dict[str, Any]:
        iface = self._iface()
        with self.bridge.admin_lock:
            return read_settings(iface)

    def apply(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ApiError(400, "expected a JSON object")
        with self.bridge.admin_lock:
            iface = self._iface()
            plan = plan_changes(iface, payload)
            if not plan.changes:
                return {"ok": True, "changed": False, "message": "Nothing to change.",
                        "changes": [], "warnings": plan.warnings}
            if plan.risks and payload.get("confirm") is not True:
                raise ApiError(409, "confirmation required", risks=plan.risks,
                               changes=plan.changes, warnings=plan.warnings)
            log.info("web UI: applying %d change(s): %s", len(plan.changes),
                     "; ".join(f"{c['section']} {c['label']}: {c['old']} -> {c['new']}"
                               for c in plan.changes))
            try:
                apply_plan(self.bridge, iface, plan)
            except ApiError:
                raise
            except Exception as e:  # noqa: BLE001
                log.error("web UI: writing settings failed: %s: %s", type(e).__name__, e)
                raise ApiError(502, f"could not write to the node: {type(e).__name__}: {e}") from e
        return {"ok": True, "changed": True, "changes": plan.changes, "warnings": plan.warnings,
                "message": "Sent to the node. It reboots to apply the change, then the proxy reconnects "
                           "(about 20-40 seconds)."}

    def reconnect(self) -> dict[str, Any]:
        log.info("web UI: reconnect requested")
        self.bridge.request_reconnect()
        return {"ok": True}

    def get_logs(self, query: dict[str, list[str]]) -> dict[str, Any]:
        try:
            after = int(query.get("after", ["0"])[0])
            limit = max(1, min(int(query.get("limit", ["500"])[0]), 2000))
        except ValueError:
            raise ApiError(400, "bad query") from None
        level = LEVELS.get(query.get("level", ["DEBUG"])[0].upper(), 0)
        lines = self.logs.since(after, level, limit)
        return {"lines": lines, "last_id": lines[-1]["id"] if lines else after}

    def _iface(self) -> Any:
        iface = self.bridge.current_interface()
        if iface is None:
            raise ApiError(503, f"Not connected to the node right now ({self.bridge.state}). "
                                f"Settings are available once the Bluetooth link is up.")
        return iface


# --------------------------------------------------------------------------
# HTTP layer
# --------------------------------------------------------------------------
def make_handler(api: Api, cfg: WebSettings):
    expected = None
    if cfg.password:
        expected = base64.b64encode(f"{cfg.username}:{cfg.password}".encode()).decode()

    class Handler(BaseHTTPRequestHandler):
        server_version = "mesh-mqtt-proxy"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
            log.debug("http %s", fmt % args)

        # -- helpers --
        def _send(self, status: int, body: bytes, ctype: str, extra: Optional[dict[str, str]] = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy",
                             "default-src 'none'; script-src 'unsafe-inline'; "
                             "style-src 'unsafe-inline'; connect-src 'self'; img-src data:; "
                             "base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, obj: Any) -> None:
            self._send(status, json.dumps(obj).encode(), "application/json")

        def _authorized(self) -> bool:
            if expected is not None:
                h = self.headers.get("Authorization", "")
                if h.startswith("Basic ") and hmac.compare_digest(h[6:].strip(), expected):
                    return True
                time.sleep(1.0)  # slow down password guessing
                self.close_connection = True  # the request body (if any) was not read
                self._send(401, b"Authentication required", "text/plain",
                           {"WWW-Authenticate": 'Basic realm="mesh-mqtt-proxy", charset="UTF-8"'})
                return False
            # No password: only loopback is allowed, and the Host header must be local
            # (defeats DNS-rebinding from a web page in the same browser).
            host = (self.headers.get("Host") or "").rsplit(":", 1)[0].strip("[]").lower()
            if host not in ("localhost", "127.0.0.1", "::1"):
                self.close_connection = True
                self._send(403, b"Set [web] password to use this from another address", "text/plain")
                return False
            return True

        def _guard(self) -> bool:
            return self._authorized()

        def _dispatch(self, fn) -> None:
            try:
                self._json(200, fn())
            except ApiError as e:
                self._json(e.status, {"error": e.message, **e.extra})
            except Exception as e:  # noqa: BLE001
                log.error("web UI error: %s: %s", type(e).__name__, e, exc_info=True)
                self._json(500, {"error": "internal error, see the log"})

        # -- verbs --
        def do_GET(self) -> None:  # noqa: N802
            if not self._guard():
                return
            u = urlparse(self.path)
            if u.path in ("/", "/index.html"):
                try:
                    self._send(200, INDEX_PATH.read_bytes(), "text/html; charset=utf-8")
                except OSError:
                    self._send(500, b"web/index.html is missing", "text/plain")
            elif u.path == "/api/status":
                self._dispatch(api.status)
            elif u.path == "/api/settings":
                self._dispatch(api.settings)
            elif u.path == "/api/nodes":
                self._dispatch(api.nodes)
            elif u.path == "/api/logs":
                q = parse_qs(u.query)
                self._dispatch(lambda: api.get_logs(q))
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self) -> None:  # noqa: N802
            if not self._guard():
                return
            if self.headers.get("X-Requested-With") != "mesh-ui" or \
                    not (self.headers.get("Content-Type") or "").startswith("application/json"):
                self.close_connection = True
                self._json(400, {"error": "missing X-Requested-With / JSON content type"})
                return
            u = urlparse(self.path)
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = -1
            if not 0 <= n <= MAX_BODY:
                self.close_connection = True
                self._json(413, {"error": "bad body size"})
                return
            try:
                payload = json.loads(self.rfile.read(n) or b"{}")
            except ValueError:
                self._json(400, {"error": "invalid JSON"})
                return
            if u.path == "/api/settings":
                self._dispatch(lambda: api.apply(payload))
            elif u.path == "/api/reconnect":
                self._dispatch(api.reconnect)
            else:
                self._send(404, b"not found", "text/plain")

    return Handler


class WebServer:
    def __init__(self, httpd: ThreadingHTTPServer, thread: threading.Thread, handler: logging.Handler):
        self.httpd = httpd
        self.thread = thread
        self._handler = handler

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def shutdown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        logging.getLogger().removeHandler(self._handler)


def start(bridge: Any, raw_settings: dict[str, Any]) -> Optional[WebServer]:
    """Start the UI in a background thread.  Returns None if it must not run."""
    cfg = WebSettings.from_dict(raw_settings)
    if not cfg.enabled:
        return None
    if not cfg.loopback_only and not cfg.password:
        log.error("web UI NOT started: [web] host=%s is reachable from the network, so "
                  "[web] password must be set (or use host = \"127.0.0.1\")", cfg.host)
        return None

    logs = LogBuffer(cfg.log_lines)
    logging.getLogger().addHandler(logs)
    origin = (cfg.latitude, cfg.longitude) if cfg.latitude is not None and cfg.longitude is not None else None
    httpd = ThreadingHTTPServer((cfg.host, cfg.port), make_handler(Api(bridge, logs, origin), cfg))
    httpd.daemon_threads = True
    t = threading.Thread(target=httpd.serve_forever, name="web-ui", daemon=True)
    t.start()
    log.info("web UI listening on http://%s:%d/ (%s)", cfg.host, httpd.server_address[1],
             "password protected" if cfg.password else "no password, loopback only")
    return WebServer(httpd, t, logs)
