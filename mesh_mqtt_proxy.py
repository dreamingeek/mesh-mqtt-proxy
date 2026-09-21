#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""MQTT client proxy for a Meshtastic node, over Bluetooth LE.

Some Meshtastic boards (nRF52 ones such as the RAK4631) have no WiFi or
Ethernet, so they cannot reach an MQTT broker on their own.  The firmware
solves this with "MQTT client proxy": the node hands every MQTT message it
wants to publish (including Map Reports) to a connected client, and expects
that client to publish it for it.  Phone apps normally do this; this script
does it from a Linux box (e.g. a Raspberry Pi) instead.

    node --(BLE)--> this script --(TCP/TLS)--> MQTT broker      (uplink)
    node <-(BLE)--- this script <-(TCP/TLS)--- MQTT broker      (downlink, optional)

Broker address, credentials, root topic and TLS are read from the node's own
MQTT module config, so configure them on the node; the config file here only
needs the node's Bluetooth address.  Anything in the [broker] section
overrides the node's values.
"""
from __future__ import annotations

import argparse
import collections
import functools
import logging
import queue
import re
import signal
import ssl
import sys
import threading
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt
from pubsub import pub

log = logging.getLogger("mesh-mqtt-proxy")

# Defaults the firmware itself uses when the node's MQTT address is blank.
DEFAULT_BROKER = "mqtt.meshtastic.org"
DEFAULT_USER = "meshdev"
DEFAULT_PASS = "large4cats"
DEFAULT_ROOT = "msh"

# Channel name the firmware uses for a channel with an empty name, per preset.
PRESET_NAMES = {
    "LONG_FAST": "LongFast",
    "LONG_SLOW": "LongSlow",
    "VERY_LONG_SLOW": "VLongSlow",
    "MEDIUM_SLOW": "MediumSlow",
    "MEDIUM_FAST": "MediumFast",
    "SHORT_SLOW": "ShortSlow",
    "SHORT_FAST": "ShortFast",
    "LONG_MODERATE": "LongMod",
    "SHORT_TURBO": "ShortTurbo",
    "LONG_TURBO": "LongTurbo",
}


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
@dataclass
class Settings:
    address: str = ""                       # BLE MAC address or advertised name
    load_nodes: Optional[bool] = None       # download the node list (None = only if the web UI is on)
    downlink: bool = False                  # forward broker -> node as well
    downlink_queue: int = 200               # max queued downlink messages
    heartbeat_interval: float = 60.0        # seconds between BLE keepalives
    connect_timeout: float = 90.0           # seconds to wait for BLE + config
    reconnect_min: float = 5.0
    reconnect_max: float = 120.0
    stats_interval: float = 300.0
    extra_subscriptions: list[str] = field(default_factory=list)
    broker: dict[str, Any] = field(default_factory=dict)
    web: dict[str, Any] = field(default_factory=dict)   # see mesh_web.WebSettings


def wants_nodes(s: Settings) -> bool:
    """The node list is only worth its (slow) BLE download when something shows it."""
    if s.load_nodes is not None:
        return s.load_nodes
    return bool(s.web.get("enabled"))


def load_settings(path: Path) -> Settings:
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    radio = raw.get("radio", {})
    bridge = raw.get("bridge", {})
    s = Settings(
        address=str(radio.get("address", "")),
        load_nodes=(bool(radio["load_nodes"]) if "load_nodes" in radio else None),
        connect_timeout=float(radio.get("connect_timeout", 90)),
        downlink=bool(bridge.get("downlink", False)),
        downlink_queue=int(bridge.get("downlink_queue", 200)),
        heartbeat_interval=float(bridge.get("heartbeat_interval", 60)),
        reconnect_min=float(bridge.get("reconnect_min", 5)),
        reconnect_max=float(bridge.get("reconnect_max", 120)),
        stats_interval=float(bridge.get("stats_interval", 300)),
        extra_subscriptions=list(bridge.get("extra_subscriptions", [])),
        broker=dict(raw.get("broker", {})),
        web=dict(raw.get("web", {})),
    )
    if not s.address:
        raise SystemExit(f"{path}: [radio] address is required (see --scan)")
    return s


# --------------------------------------------------------------------------
# Broker / topic resolution (pure functions, easy to test)
# --------------------------------------------------------------------------
@dataclass
class BrokerInfo:
    host: str
    port: int
    username: str
    password: str
    tls: bool
    root: str


def region_code(lora: Any) -> str:
    """Region name as used in default MQTT topics (e.g. 'US'), or '' if unset."""
    from meshtastic.protobuf import config_pb2

    name = config_pb2.Config.LoRaConfig.RegionCode.Name(lora.region)
    return "" if name == "UNSET" else name


def resolve_broker(node_mqtt: Any, override: dict[str, Any], region: str = "") -> BrokerInfo:
    """Combine the node's MQTT module config with optional local overrides.

    A blank root topic means the firmware's default, which is ``msh/<REGION>``.
    """
    host = str(override.get("host") or node_mqtt.address or DEFAULT_BROKER)
    parsed_port: Optional[int] = None
    if ":" in host and host.count(":") == 1:
        host, _, p = host.partition(":")
        if p.isdigit():
            parsed_port = int(p)
    tls = bool(override.get("tls", node_mqtt.tls_enabled))
    port = int(override.get("port") or parsed_port or (8883 if tls else 1883))
    is_default = host == DEFAULT_BROKER
    username = str(
        override.get("username") or node_mqtt.username
        or (DEFAULT_USER if is_default else "")
    )
    password = str(
        override.get("password") or node_mqtt.password
        or (DEFAULT_PASS if is_default else "")
    )
    default_root = f"{DEFAULT_ROOT}/{region}" if region else DEFAULT_ROOT
    root = str(override.get("root") or node_mqtt.root or default_root).strip("/")
    return BrokerInfo(host, port, username, password, tls, root)


def preset_channel_name(lora: Any) -> str:
    """Name the firmware gives an unnamed channel (derived from the preset)."""
    from meshtastic.protobuf import config_pb2

    if not lora.use_preset:
        return "Custom"
    enum_name = config_pb2.Config.LoRaConfig.ModemPreset.Name(lora.modem_preset)
    return PRESET_NAMES.get(enum_name, enum_name.title().replace("_", ""))


def downlink_topics(node: Any, broker: BrokerInfo, extra: list[str]) -> list[str]:
    """Topics the node wants to hear from the broker (channels with downlink on)."""
    from meshtastic.protobuf import channel_pb2

    topics: list[str] = []
    default_name = preset_channel_name(node.localConfig.lora)
    json_enabled = node.moduleConfig.mqtt.json_enabled
    for ch in node.channels or []:
        if ch.role == channel_pb2.Channel.Role.DISABLED:
            continue
        if not ch.settings.downlink_enabled:
            continue
        name = ch.settings.name or default_name
        topics.append(f"{broker.root}/2/e/{name}/#")
        if json_enabled:
            topics.append(f"{broker.root}/2/json/{name}/#")
    topics.extend(extra)
    # de-duplicate, keep order
    return list(dict.fromkeys(topics))


# --------------------------------------------------------------------------
# The bridge
# --------------------------------------------------------------------------
class SessionEnded(Exception):
    pass


_KIND_RE = re.compile(r"/2/(e|json|map|stat)(?:/|$)")


def topic_kind(topic: str) -> str:
    """Classify a Meshtastic MQTT topic: e (channel packets), json, map, stat, other."""
    m = _KIND_RE.search(topic)
    return m.group(1) if m else "other"


_MAC_RE = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")


@functools.cache
def direct_ble_interface_class():
    """BLEInterface that can still connect to a node that isn't advertising.

    The stock library only connects to nodes it sees *advertising* the Meshtastic
    service during a 10 s scan.  A node that BlueZ already holds a connection to
    (for example right after `bluetoothctl pair`, or a leftover session) stops
    advertising, so the scan misses it even though it is right there.  When the
    scan fails and we were given a MAC address, connect to that address directly.
    """
    from meshtastic.ble_interface import BLEInterface

    class DirectBLEInterface(BLEInterface):
        def find_device(self, address):
            try:
                return super().find_device(address)
            except BLEInterface.BLEError as e:
                if e.kind != BLEInterface.BLEError.DEVICE_NOT_FOUND or not _MAC_RE.match(address or ""):
                    raise
                log.warning(
                    "%s was not seen advertising (already connected or bonded?); "
                    "connecting directly by address", address,
                )
                return SimpleNamespace(address=address, name=address)

    return DirectBLEInterface


def default_interface_factory(address: str, timeout: float, load_nodes: bool = False):
    # By default fetch config/channels only and skip the (slow over BLE) node DB;
    # the web UI's Nodes tab needs it, so it is loaded when that is enabled.
    return direct_ble_interface_class()(address, noNodes=not load_nodes, timeout=int(timeout))


class Bridge:
    def __init__(
        self,
        settings: Settings,
        interface_factory: Optional[Callable[[str, float], Any]] = None,
    ):
        self.s = settings
        self.want_nodes = wants_nodes(settings)
        self._factory = interface_factory or functools.partial(
            default_interface_factory, load_nodes=wants_nodes(settings))
        self._iface: Any = None
        self._mqtt: Optional[mqtt.Client] = None
        self._down_q: queue.Queue[tuple[str, bytes]] = queue.Queue(
            maxsize=settings.downlink_queue
        )
        self._lost = threading.Event()
        self._stop = threading.Event()
        self.stats = {"up": 0, "up_dropped": 0, "down": 0, "down_dropped": 0}
        self.kinds: collections.Counter[str] = collections.Counter()  # uplink by topic kind
        self._seen_kinds: set[str] = set()
        self.last_map_report: Optional[float] = None  # wall-clock time
        self.on_session_ready: Optional[Callable[[], None]] = None  # test hook

        # State for the web UI (mesh_web.py); harmless when the UI is off.
        self.started_at = time.time()
        self.state = "starting"            # starting | connecting | connected | waiting | stopped
        self.state_since = time.time()
        self.last_error = ""
        self.retry_at: Optional[float] = None
        self.broker: Optional[BrokerInfo] = None
        self.recent: collections.deque[dict[str, Any]] = collections.deque(maxlen=50)
        self.admin_lock = threading.RLock()  # serialises config changes from the UI
        self._fast_retry = False

        pub.subscribe(self._on_proxy, "meshtastic.mqttclientproxymessage")
        pub.subscribe(self._on_lost, "meshtastic.connection.lost")

    # ---- pubsub callbacks (run on the meshtastic library's threads) --------
    def _on_proxy(self, proxymessage, interface):
        """Node -> broker."""
        if self._iface is not None and interface is not self._iface:
            return
        which = proxymessage.WhichOneof("payload_variant")
        payload = proxymessage.data if which == "data" else proxymessage.text.encode()
        client = self._mqtt
        if client is None or not client.is_connected():
            self.stats["up_dropped"] += 1
            log.warning("broker not connected; dropped uplink %s", proxymessage.topic)
            return
        client.publish(proxymessage.topic, payload, qos=0, retain=proxymessage.retained)
        self.stats["up"] += 1
        kind = topic_kind(proxymessage.topic)
        self.kinds[kind] += 1
        self.recent.append({"t": time.time(), "topic": proxymessage.topic,
                            "kind": kind, "bytes": len(payload)})
        if kind == "map":
            self.last_map_report = time.time()
            log.info("published MAP REPORT to %s (%d bytes)", proxymessage.topic, len(payload))
        elif kind not in self._seen_kinds:
            self._seen_kinds.add(kind)
            log.info("first '%s' uplink message: %s", kind, proxymessage.topic)
        else:
            log.debug("uplink %s (%d bytes)", proxymessage.topic, len(payload))

    def _set_state(self, state: str) -> None:
        self.state = state
        self.state_since = time.time()

    def current_interface(self) -> Any:
        """The live node connection, or None while (re)connecting."""
        iface = self._iface
        if self.state == "connected" and iface is not None and self._healthy(iface):
            return iface
        return None

    def mqtt_connected(self) -> bool:
        client = self._mqtt
        return bool(client is not None and client.is_connected())

    def request_reconnect(self) -> None:
        """Drop the BLE session and reconnect soon (used after a config change)."""
        self._fast_retry = True
        self._lost.set()
        if self.state == "connected":
            self._set_state("waiting")

    def stats_text(self) -> str:
        last = ("none yet" if self.last_map_report is None
                else time.strftime("%H:%M:%S", time.localtime(self.last_map_report)))
        return f"stats: {self.stats} by_kind={dict(self.kinds)} last_map_report={last}"

    def _on_lost(self, interface):
        if interface is self._iface:
            log.warning("BLE connection lost")
            self._lost.set()

    # ---- paho callbacks (run on paho's thread) ---------------------------
    def _make_mqtt(self, node_num: int, broker: BrokerInfo, topics: list[str]) -> mqtt.Client:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"mesh-mqtt-proxy-{node_num:08x}",
        )
        if broker.username:
            client.username_pw_set(broker.username, broker.password)
        if broker.tls:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.reconnect_delay_set(min_delay=1, max_delay=60)

        def on_connect(c, _ud, _flags, reason_code, _props):
            if reason_code.is_failure:
                log.error("broker refused connection: %s", reason_code)
                return
            log.info("connected to broker %s:%d", broker.host, broker.port)
            if topics:
                c.subscribe([(t, 0) for t in topics])
                log.info("subscribed: %s", ", ".join(topics))

        def on_disconnect(_c, _ud, _flags, reason_code, _props):
            if reason_code.value == 0:  # we asked for it (session teardown)
                log.debug("broker connection closed")
            else:
                log.warning("broker disconnected (%s); paho will retry", reason_code)

        def on_message(_c, _ud, msg):
            if not self.s.downlink:
                return
            try:
                self._down_q.put_nowait((msg.topic, bytes(msg.payload)))
            except queue.Full:
                self.stats["down_dropped"] += 1

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        return client

    # ---- one BLE session ---------------------------------------------------
    def _healthy(self, iface: Any) -> bool:
        if self._lost.is_set():
            return False
        if not iface.isConnected.is_set():
            return False
        if getattr(iface, "client", True) is None:
            return False
        if getattr(iface, "_want_receive", True) is False:
            return False
        return True

    def _run_session(self) -> None:
        self._lost.clear()
        while not self._down_q.empty():
            self._down_q.get_nowait()

        self._set_state("connecting")
        self.retry_at = None
        log.info("connecting to node %s over BLE ...", self.s.address)
        iface = self._factory(self.s.address, self.s.connect_timeout)
        self._iface = iface
        try:
            node = iface.localNode
            node_mqtt = node.moduleConfig.mqtt
            broker = resolve_broker(node_mqtt, self.s.broker, region_code(node.localConfig.lora))
            self.broker = broker
            self._log_node_state(node, node_mqtt, broker)

            topics = downlink_topics(node, broker, self.s.extra_subscriptions) \
                if self.s.downlink else []
            num = getattr(getattr(iface, "myInfo", None), "my_node_num", 0) or 0
            client = self._make_mqtt(num, broker, topics)
            self._mqtt = client
            client.connect_async(broker.host, broker.port, keepalive=60)
            client.loop_start()

            self._set_state("connected")
            self.last_error = ""
            if self.on_session_ready:
                self.on_session_ready()

            next_beat = time.monotonic() + self.s.heartbeat_interval
            next_stats = time.monotonic() + self.s.stats_interval
            while not self._stop.is_set():
                if not self._healthy(iface):
                    raise SessionEnded("BLE link is down")
                try:
                    topic, payload = self._down_q.get(timeout=1.0)
                except queue.Empty:
                    pass
                else:
                    iface.sendMqttClientProxyMessage(topic, payload)
                    self.stats["down"] += 1
                now = time.monotonic()
                if now >= next_beat:
                    iface.sendHeartbeat()  # raises if the link is dead
                    next_beat = now + self.s.heartbeat_interval
                if now >= next_stats:
                    log.info("%s", self.stats_text())
                    next_stats = now + self.s.stats_interval
        finally:
            self._teardown(iface)

    def _log_node_state(self, node: Any, node_mqtt: Any, broker: BrokerInfo) -> None:
        log.info(
            "node MQTT config: enabled=%s proxy_to_client=%s map_reporting=%s",
            node_mqtt.enabled, node_mqtt.proxy_to_client_enabled,
            node_mqtt.map_reporting_enabled,
        )
        if not node_mqtt.enabled:
            log.warning("MQTT is disabled on the node; it will not emit anything to proxy")
        if not node_mqtt.proxy_to_client_enabled:
            log.warning("'MQTT client proxy' is off on the node; enable it (proxy_to_client_enabled)")
        if node_mqtt.map_reporting_enabled:
            rs = node_mqtt.map_report_settings
            log.info(
                "map reporting on: every %ds, position precision %d bits, should_report_location=%s",
                rs.publish_interval_secs, rs.position_precision, rs.should_report_location,
            )
            if not rs.should_report_location:
                log.warning(
                    "map reporting is enabled but should_report_location is false; the node "
                    "will probably not send map reports. Set it with: meshtastic --ble <MAC> "
                    "--set mqtt.map_report_settings.should_report_location true "
                    "(saving the MQTT screen in a phone app can reset it)"
                )
        else:
            log.warning("map reporting is off on the node")
        log.info("broker: %s:%d tls=%s root=%s", broker.host, broker.port, broker.tls, broker.root)

    def _teardown(self, iface: Any) -> None:
        client, self._mqtt = self._mqtt, None
        if client is not None:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:  # noqa: BLE001
                log.debug("mqtt teardown error", exc_info=True)

        def _close():
            try:
                iface.close()
            except Exception:  # noqa: BLE001
                log.debug("iface close error", exc_info=True)

        t = threading.Thread(target=_close, daemon=True)
        t.start()
        t.join(15)
        if t.is_alive():
            log.warning("BLE close is hanging; abandoning it")
        self._iface = None

    # ---- top-level loop ------------------------------------------------------
    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        delay = self.s.reconnect_min
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                try:
                    self._run_session()
                except Exception as e:  # noqa: BLE001
                    self.last_error = f"{type(e).__name__}: {e}"
                    log.error("session ended: %s: %s", type(e).__name__, e)
                    log.debug("details", exc_info=True)
                if self._stop.is_set():
                    break
                # Back off, but start over if that session was healthy for a while.
                if self._fast_retry:
                    # We asked for this (config change makes the node reboot, which
                    # takes it a few seconds), so do not treat it as a failure.
                    self._fast_retry = False
                    delay = max(self.s.reconnect_min, 8.0)
                elif time.monotonic() - started > 300:
                    delay = self.s.reconnect_min
                else:
                    delay = min(delay * 2, self.s.reconnect_max)
                log.info("reconnecting in %.0fs", delay)
                self._set_state("waiting")
                self.retry_at = time.time() + delay
                self._stop.wait(delay)
        finally:
            self._set_state("stopped")
            pub.unsubscribe(self._on_proxy, "meshtastic.mqttclientproxymessage")
            pub.unsubscribe(self._on_lost, "meshtastic.connection.lost")
            log.info("stopped; final stats: %s", self.stats)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def cmd_scan() -> int:
    from meshtastic.ble_interface import BLEInterface

    devices = BLEInterface.scan()
    if not devices:
        print("No Meshtastic BLE devices found. Is the node powered and in range? "
              "A node that is already connected (to a phone, or to this Pi via "
              "bluetoothctl) stops advertising and will not show up here; "
              "try `bluetoothctl disconnect <MAC>` and scan again.")
        return 1
    for d in devices:
        print(f"{d.address}  {d.name}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("-c", "--config", type=Path, default=Path("config.toml"))
    ap.add_argument("--scan", action="store_true", help="list nearby Meshtastic BLE nodes and exit")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout,
    )
    if not args.verbose:
        for noisy in ("meshtastic", "bleak"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    if args.scan:
        return cmd_scan()

    settings = load_settings(args.config)
    bridge = Bridge(settings)
    web = None
    if settings.web.get("enabled"):
        import mesh_web  # local module; only needed when the UI is on

        web = mesh_web.start(bridge, settings.web)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: bridge.stop())
    try:
        bridge.run()
    finally:
        if web is not None:
            web.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
