# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests for the web UI backend (mesh_web).  No Bluetooth or MQTT broker needed:
a fake node built from real Meshtastic protobufs records what would be written.

Run:  python -m unittest discover -s tests -v
"""
import base64
import json
import logging
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import mesh_mqtt_proxy as mmp  # noqa: E402
import mesh_web  # noqa: E402
from meshtastic.protobuf import channel_pb2, config_pb2, localonly_pb2  # noqa: E402

SECRET = "hunter2-do-not-leak"


class FakeNode:
    nodeNum = 0xDEADBEEF

    def __init__(self):
        cfg = localonly_pb2.LocalModuleConfig()
        cfg.mqtt.enabled = True
        cfg.mqtt.proxy_to_client_enabled = True
        cfg.mqtt.map_reporting_enabled = True
        cfg.mqtt.root = "msh/US/CA"
        cfg.mqtt.password = SECRET
        cfg.mqtt.map_report_settings.publish_interval_secs = 3600
        cfg.mqtt.map_report_settings.position_precision = 16
        cfg.mqtt.map_report_settings.should_report_location = True
        lc = localonly_pb2.LocalConfig()
        lc.lora.use_preset = True
        lc.lora.region = config_pb2.Config.LoRaConfig.RegionCode.US
        lc.lora.config_ok_to_mqtt = True
        self.moduleConfig, self.localConfig = cfg, lc
        self.channels = []
        for i, (name, role) in enumerate((("", "PRIMARY"), ("family", "SECONDARY"))):
            ch = channel_pb2.Channel(index=i, role=getattr(channel_pb2.Channel.Role, role))
            ch.settings.name = name
            ch.settings.psk = b"\x01" if i == 0 else bytes(32)
            ch.settings.uplink_enabled = i == 0
            ch.settings.downlink_enabled = i == 0
            ch.settings.module_settings.position_precision = 16 if i == 0 else 32
            self.channels.append(ch)
        self.calls = []

    def ensureSessionKey(self): self.calls.append("session")
    def beginSettingsTransaction(self): self.calls.append("begin")
    def commitSettingsTransaction(self): self.calls.append("commit")
    def writeConfig(self, name): self.calls.append(("config", name))
    def writeChannel(self, idx): self.calls.append(("channel", idx))
    def setOwner(self, **kw): self.calls.append(("owner", kw))


class FakeIface:
    def __init__(self):
        self.localNode = FakeNode()
        self.myInfo = SimpleNamespace(my_node_num=0xDEADBEEF)
        self.metadata = None
        self.isConnected = threading.Event()
        self.isConnected.set()
        self.client = object()
        self._want_receive = True
        now = time.time()
        self.nodesByNum = {
            0xDEADBEEF: {"num": 0xDEADBEEF, "user": {"id": "!deadbeef", "longName": "Roof Node", "shortName": "ROOF",
                                                    "hwModel": "RAK4631"},
                         "position": {"latitude": 0.0, "longitude": 0.0001, "latitudeI": 0, "longitudeI": 1000,
                                      "altitude": 300},
                         "lastHeard": int(now), "deviceMetrics": {"batteryLevel": 101, "voltage": 4.07}},
            0x0A: {"num": 0x0A, "user": {"id": "!0000000a", "longName": "East One", "shortName": "EST1",
                                        "hwModel": "RAK4631", "publicKey": "abc="},
                   "position": {"latitude": 0.0, "longitude": 1.0001}, "snr": 6.5, "hopsAway": 2,
                   "lastHeard": int(now - 300), "isFavorite": True,
                   "deviceMetrics": {"batteryLevel": 72, "voltage": 3.91}},
            0x0B: {"num": 0x0B, "user": {"id": "!0000000b", "longName": "North One", "shortName": "NTH1"},
                   "position": {"latitudeI": 10000000, "longitudeI": 1000}, "hopsAway": 0, "viaMqtt": True,
                   "lastHeard": int(now - 5 * 3600)},                 # heard 5 h ago: not "online"
            0x0C: {"num": 0x0C, "user": {"id": "!0000000c", "longName": "No Position"}, "lastHeard": int(now - 60)},
            0x0D: {"num": 0x0D, "position": {"latitude": 0.0, "longitude": 0.0}},   # 0,0 = "no fix"
        }

    def _getOrCreateByNum(self, num):
        return {"adminSessionPassKey": b"k"}

    def getMyUser(self):
        return {"longName": "Roof Node", "shortName": "ROOF"}


def make_bridge(connected=True):
    b = mmp.Bridge(mmp.Settings(address="AA:BB:CC:DD:EE:FF"))
    b.iface = FakeIface()
    b.want_nodes = True
    if connected:
        b._iface = b.iface
        b._set_state("connected")
    return b


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.bridge = make_bridge()
        self.addCleanup(self.bridge.stop)
        self.addCleanup(lambda: mmp.pub.unsubAll("meshtastic.mqttclientproxymessage"))
        self.api = mesh_web.Api(self.bridge, mesh_web.LogBuffer())
        self.node = self.bridge.iface.localNode

    def apply(self, payload):
        return self.api.apply(payload)

    def err(self, payload):
        with self.assertRaises(mesh_web.ApiError) as cm:
            self.apply(payload)
        return cm.exception

    def test_read_has_no_secret_and_lists_channels(self):
        s = self.api.settings()
        self.assertNotIn(SECRET, json.dumps(s))
        self.assertTrue(s["mqtt"]["password_set"])
        self.assertEqual([c["index"] for c in s["channels"]], [0, 1])
        self.assertEqual(s["channels"][1]["position_precision"], 32)
        self.assertEqual(s["lora"]["region"], "US")
        self.assertEqual(s["owner"]["long_name"], "Roof Node")
        self.assertEqual(s["channels"][0]["key"], "default public key")

    def test_disabled_channels_are_hidden(self):
        self.node.channels.append(channel_pb2.Channel(index=2))  # role DISABLED
        self.assertEqual(len(self.api.settings()["channels"]), 2)

    def test_change_map_precision_writes_in_a_transaction(self):
        r = self.apply({"mqtt": {"map_report_settings": {"position_precision": 15}}})
        self.assertTrue(r["changed"])
        self.assertEqual(self.node.moduleConfig.mqtt.map_report_settings.position_precision, 15)
        self.assertEqual(self.node.calls, ["session", "begin", ("config", "mqtt"), "commit"])
        self.assertTrue(self.bridge._lost.is_set())     # reconnect requested after reboot
        self.assertTrue(self.bridge._fast_retry)

    def test_no_change_writes_nothing(self):
        r = self.apply({"mqtt": {"root": "msh/US/CA", "enabled": True}})
        self.assertFalse(r["changed"])
        self.assertEqual(self.node.calls, [])
        self.assertFalse(self.bridge._lost.is_set())

    def test_channel_and_lora_and_mqtt_together(self):
        self.apply({"mqtt": {"root": "msh/US"}, "lora": {"config_ok_to_mqtt": False},
                    "channels": {"0": {"position_precision": 13}}})
        self.assertEqual(self.node.calls, ["session", "begin", ("config", "lora"), ("config", "mqtt"),
                                           ("channel", 0), "commit"])
        self.assertEqual(self.node.channels[0].settings.module_settings.position_precision, 13)
        self.assertFalse(self.node.localConfig.lora.config_ok_to_mqtt)

    def test_owner_names(self):
        self.apply({"owner": {"long_name": "Attic Node", "short_name": "ATTC"}})
        self.assertIn(("owner", {"long_name": "Attic Node", "short_name": "ATTC"}), self.node.calls)

    def test_risky_changes_need_confirmation(self):
        e = self.err({"mqtt": {"proxy_to_client_enabled": False}})
        self.assertEqual(e.status, 409)
        self.assertTrue(e.extra["risks"])
        self.assertEqual(self.node.calls, [])                     # nothing written yet
        self.assertTrue(self.node.moduleConfig.mqtt.proxy_to_client_enabled)
        r = self.apply({"mqtt": {"proxy_to_client_enabled": False}, "confirm": True})
        self.assertTrue(r["changed"])
        self.assertFalse(self.node.moduleConfig.mqtt.proxy_to_client_enabled)

    def test_uplink_on_private_channel_needs_confirmation(self):
        e = self.err({"channels": {"1": {"uplink_enabled": True}}})
        self.assertEqual(e.status, 409)
        self.assertIn("not the primary channel", e.extra["risks"][0])
        self.assertFalse(self.node.channels[1].settings.uplink_enabled)

    def test_exact_precision_on_an_uplinked_channel_needs_confirmation(self):
        # primary already uplinks; making it exact would publish exact positions
        e = self.err({"channels": {"0": {"position_precision": 32}}})
        self.assertEqual(e.status, 409)
        # ...but exact precision on the private channel (no uplink) is fine
        r = self.apply({"channels": {"1": {"position_precision": 32}}})
        self.assertFalse(r["changed"])                      # already 32
        # enabling uplink on primary while exact is also flagged
        self.node.channels[0].settings.uplink_enabled = False
        self.node.channels[0].settings.module_settings.position_precision = 32
        e = self.err({"channels": {"0": {"uplink_enabled": True}}})
        self.assertEqual(e.status, 409)

    def test_turning_uplink_off_is_not_risky(self):
        r = self.apply({"channels": {"0": {"uplink_enabled": False}}})
        self.assertTrue(r["changed"])

    def test_warnings(self):
        r = self.apply({"mqtt": {"map_report_settings": {"should_report_location": False}}})
        self.assertTrue(any("share" in w.lower() or "should_report_location" in w for w in r["warnings"]))
        self.bridge._lost.clear()                       # pretend the node came back
        self.bridge._set_state("connected")
        r = self.apply({"mqtt": {"map_report_settings": {"position_precision": 19}}})
        self.assertTrue(any("10 and 16" in w for w in r["warnings"]))

    def test_validation(self):
        bad = [
            {"nope": 1},
            {"mqtt": {"nope": 1}},
            {"mqtt": {"enabled": "yes"}},
            {"mqtt": {"root": "x" * 40}},
            {"mqtt": {"root": "a\nb"}},
            {"mqtt": {"map_report_settings": {"position_precision": 20}}},
            {"mqtt": {"map_report_settings": {"position_precision": True}}},
            {"mqtt": {"map_report_settings": {"publish_interval_secs": 60}}},
            {"mqtt": {"map_report_settings": {"nope": 1}}},
            {"lora": {"region": "EU_868"}},
            {"channels": {"9": {"uplink_enabled": True}}},
            {"channels": {"x": {"uplink_enabled": True}}},
            {"channels": {"0": {"name": "renamed"}}},
            {"channels": {"0": {"uplink_enabled": 1}}},
            {"owner": {"long_name": "   "}},
            {"owner": {"short_name": "TOOLONG"}},
            {"owner": {"is_licensed": True}},
        ]
        for payload in bad:
            with self.subTest(payload=payload):
                self.assertEqual(self.err(payload).status, 400)
        self.assertEqual(self.node.calls, [])

    def test_disabled_channel_cannot_be_edited(self):
        self.node.channels.append(channel_pb2.Channel(index=2))
        self.assertEqual(self.err({"channels": {"2": {"uplink_enabled": True}}}).status, 400)

    def test_blank_password_keeps_current_and_new_one_is_hidden_in_result(self):
        r = self.apply({"mqtt": {"password": ""}})
        self.assertFalse(r["changed"])
        r = self.apply({"mqtt": {"password": "newsecret"}})
        self.assertTrue(r["changed"])
        self.assertEqual(self.node.moduleConfig.mqtt.password, "newsecret")
        self.assertNotIn("newsecret", json.dumps(r))

    def test_not_connected(self):
        b = make_bridge(connected=False)
        self.addCleanup(b.stop)
        api = mesh_web.Api(b, mesh_web.LogBuffer())
        for fn in (api.settings, lambda: api.apply({"mqtt": {"root": "x"}})):
            with self.assertRaises(mesh_web.ApiError) as cm:
                fn()
            self.assertEqual(cm.exception.status, 503)

    def test_no_second_write_until_reconnected(self):
        self.apply({"mqtt": {"root": "msh/US"}})
        self.assertEqual(self.bridge.state, "waiting")
        self.assertEqual(self.err({"mqtt": {"root": "msh/TX"}}).status, 503)

    def test_failed_write_forces_reload(self):
        def boom(_name):
            raise RuntimeError("ble died")
        self.node.writeConfig = boom
        e = self.err({"mqtt": {"root": "msh/US"}})
        self.assertEqual(e.status, 502)
        self.assertTrue(self.bridge._lost.is_set())

    def test_status_shape(self):
        self.bridge.stats["up"] = 3
        self.bridge.recent.append({"t": 1.0, "topic": "msh/US/2/map/", "kind": "map", "bytes": 99})
        st = self.api.status()
        self.assertEqual(st["state"], "connected")
        self.assertEqual(st["stats"]["up"], 3)
        self.assertEqual(st["recent"][0]["kind"], "map")
        self.assertEqual(st["node"]["id"], "!deadbeef")
        self.assertEqual(st["node"]["long_name"], "Roof Node")
        self.assertIn("system", st)
        json.dumps(st)  # must be serialisable


class NodeListTests(unittest.TestCase):
    def setUp(self):
        self.bridge = make_bridge()
        self.addCleanup(self.bridge.stop)
        self.addCleanup(lambda: mmp.pub.unsubAll("meshtastic.mqttclientproxymessage"))
        self.api = mesh_web.Api(self.bridge, mesh_web.LogBuffer())

    def by_id(self, res):
        return {n["id"]: n for n in res["nodes"]}

    def test_math(self):
        self.assertAlmostEqual(mesh_web.haversine_m(0, 0, 0, 1), 111195, delta=5)
        self.assertAlmostEqual(mesh_web.haversine_m(10, 20, 10, 20), 0)
        self.assertAlmostEqual(mesh_web.bearing_deg(0, 0, 1, 0), 0, delta=0.01)      # north
        self.assertAlmostEqual(mesh_web.bearing_deg(0, 0, 0, 1), 90, delta=0.01)     # east
        self.assertAlmostEqual(mesh_web.bearing_deg(0, 0, -1, 0), 180, delta=0.01)   # south
        self.assertAlmostEqual(mesh_web.bearing_deg(0, 0, 0, -1), 270, delta=0.01)   # west

    def test_distance_from_our_own_node(self):
        res = self.api.nodes()
        n = self.by_id(res)
        self.assertEqual(res["origin"]["source"], "node")
        self.assertAlmostEqual(n["!0000000a"]["distance_m"], 111195, delta=10)
        self.assertEqual(n["!0000000a"]["bearing"], 90)
        self.assertAlmostEqual(n["!0000000b"]["distance_m"], 111195, delta=10)     # from latitudeI
        self.assertIn(n["!0000000b"]["bearing"], (0, 359, 360))
        self.assertIsNone(n["!deadbeef"]["distance_m"])                            # ourselves
        self.assertTrue(n["!deadbeef"]["is_me"])

    def test_fields_and_missing_data(self):
        res = self.api.nodes()
        n = self.by_id(res)
        east = n["!0000000a"]
        self.assertEqual((east["long_name"], east["short_name"], east["hw"]), ("East One", "EST1", "RAK4631"))
        self.assertEqual((east["snr"], east["hops"], east["battery"], east["favorite"], east["encrypted"]),
                         (6.5, 2, 72.0, True, True))
        north = n["!0000000b"]
        self.assertEqual((north["hops"], north["via_mqtt"], north["snr"], north["encrypted"]), (0, True, None, False))
        nopos = n["!0000000c"]
        self.assertIsNone(nopos["lat"])
        self.assertIsNone(nopos["distance_m"])
        self.assertEqual(nopos["role"], "CLIENT")
        ghost = n["!0000000d"]                     # no user info at all; 0,0 position ignored
        self.assertIsNone(ghost["lat"])
        self.assertEqual(ghost["long_name"], "")
        self.assertEqual(res["total"], 5)
        self.assertEqual(res["online"], 3)          # own node, East One, No Position; not North One (5 h)
        json.dumps(res)

    def test_config_origin_used_when_our_node_has_no_position(self):
        del self.bridge.iface.nodesByNum[0xDEADBEEF]["position"]
        res = self.api.nodes()
        self.assertIsNone(res["origin"])
        self.assertIsNone(self.by_id(res)["!0000000a"]["distance_m"])
        api = mesh_web.Api(self.bridge, mesh_web.LogBuffer(), origin=(0.0, 0.0))
        res = api.nodes()
        self.assertEqual(res["origin"]["source"], "config")
        self.assertAlmostEqual(self.by_id(res)["!0000000a"]["distance_m"], 111195 * 1.0001, delta=50)

    def test_node_list_not_loaded(self):
        self.bridge.want_nodes = False
        res = self.api.nodes()
        self.assertFalse(res["enabled"])
        self.assertEqual(res["nodes"], [])

    def test_last_list_kept_while_reconnecting(self):
        self.api.nodes()
        self.bridge.request_reconnect()
        res = self.api.nodes()
        self.assertTrue(res["stale"])
        self.assertEqual(res["total"], 5)

    def test_not_connected_and_nothing_cached(self):
        b = make_bridge(connected=False)
        self.addCleanup(b.stop)
        with self.assertRaises(mesh_web.ApiError) as cm:
            mesh_web.Api(b, mesh_web.LogBuffer()).nodes()
        self.assertEqual(cm.exception.status, 503)

    def test_survives_nodes_changing_underneath(self):
        stop = threading.Event()

        def churn():
            i = 1000
            while not stop.is_set():
                self.bridge.iface.nodesByNum[i] = {"num": i, "lastHeard": 1}
                i += 1
                if i > 4000:
                    for k in range(1000, 4000):
                        self.bridge.iface.nodesByNum.pop(k, None)
                    i = 1000
        t = threading.Thread(target=churn, daemon=True)
        t.start()
        try:
            for _ in range(200):
                self.api.nodes()
        finally:
            stop.set()
            t.join()

    def test_web_settings_parse_origin(self):
        w = mesh_web.WebSettings.from_dict({"latitude": 36.1, "longitude": -95.9})
        self.assertEqual((w.latitude, w.longitude), (36.1, -95.9))
        self.assertIsNone(mesh_web.WebSettings.from_dict({}).latitude)


class LogBufferTests(unittest.TestCase):
    def test_since_and_levels(self):
        buf = mesh_web.LogBuffer(maxlen=3)
        lg = logging.getLogger("t.logbuf")
        lg.propagate = False
        lg.setLevel(logging.DEBUG)
        lg.addHandler(buf)
        lg.info("a %s", 1)
        lg.warning("b")
        lg.error("c")
        lg.info("d")                                    # pushes "a" out (maxlen 3)
        got = buf.since(0)
        self.assertEqual([x["msg"] for x in got], ["b", "c", "d"])
        self.assertEqual([x["msg"] for x in buf.since(got[0]["id"])], ["c", "d"])
        self.assertEqual([x["msg"] for x in buf.since(0, min_level=30)], ["b", "c"])


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bridge = make_bridge()
        cfg = {"enabled": True, "host": "127.0.0.1", "port": 0, "username": "admin", "password": "pw"}
        cls.web = mesh_web.start(cls.bridge, cfg)
        cls.base = f"http://127.0.0.1:{cls.web.port}"
        cls.auth = "Basic " + base64.b64encode(b"admin:pw").decode()
        logging.getLogger("mesh-mqtt-proxy").warning("hello from the test")

    @classmethod
    def tearDownClass(cls):
        cls.web.shutdown()
        cls.bridge.stop()
        mmp.pub.unsubAll("meshtastic.mqttclientproxymessage")

    def req(self, path, method="GET", body=None, headers=None, auth=True):
        h = dict(headers or {})
        if auth:
            h["Authorization"] = self.auth
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            h.setdefault("Content-Type", "application/json")
        r = urllib.request.Request(self.base + path, data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, resp.read(), resp.headers
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers

    def test_requires_auth(self):
        self.assertEqual(self.req("/api/status", auth=False)[0], 401)
        self.assertEqual(self.req("/api/status", headers={"Authorization": "Basic " + base64.b64encode(b"admin:no").decode()}, auth=False)[0], 401)
        code, body, _ = self.req("/api/status")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["state"], "connected")

    def test_index_served_with_security_headers(self):
        code, body, hdr = self.req("/")
        self.assertEqual(code, 200)
        self.assertIn(b"Mesh MQTT Bridge", body)
        self.assertEqual(hdr["X-Frame-Options"], "DENY")
        self.assertIn("default-src 'none'", hdr["Content-Security-Policy"])

    def test_logs(self):
        code, body, _ = self.req("/api/logs?level=WARNING")
        self.assertEqual(code, 200)
        data = json.loads(body)
        self.assertTrue(any("hello from the test" in x["msg"] for x in data["lines"]))
        code, body, _ = self.req(f"/api/logs?after={data['last_id']}")
        self.assertEqual(json.loads(body)["lines"], [])
        self.assertEqual(self.req("/api/logs?after=abc")[0], 400)

    def test_post_needs_csrf_header_and_json(self):
        body = {"mqtt": {"root": "msh/US/CA"}}
        self.assertEqual(self.req("/api/settings", "POST", body)[0], 400)
        self.assertEqual(self.req("/api/settings", "POST", body, {"X-Requested-With": "mesh-ui", "Content-Type": "text/plain"})[0], 400)
        code, resp, _ = self.req("/api/settings", "POST", body, {"X-Requested-With": "mesh-ui"})
        self.assertEqual(code, 200)
        self.assertFalse(json.loads(resp)["changed"])

    def test_error_statuses_pass_through(self):
        h = {"X-Requested-With": "mesh-ui"}
        code, resp, _ = self.req("/api/settings", "POST", {"mqtt": {"proxy_to_client_enabled": False}}, h)
        self.assertEqual(code, 409)
        self.assertTrue(json.loads(resp)["risks"])
        self.assertEqual(self.req("/api/settings", "POST", {"bogus": 1}, h)[0], 400)
        self.assertEqual(self.req("/api/nothing")[0], 404)

    def test_nodes_endpoint(self):
        code, body, _ = self.req("/api/nodes")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["total"], 5)
        self.assertEqual(self.req("/api/nodes", auth=False)[0], 401)

    def test_settings_get_has_no_secret(self):
        code, body, _ = self.req("/api/settings")
        self.assertEqual(code, 200)
        self.assertNotIn(SECRET.encode(), body)


class StartRulesTests(unittest.TestCase):
    def test_network_bind_without_password_is_refused(self):
        b = make_bridge()
        self.addCleanup(b.stop)
        self.addCleanup(lambda: mmp.pub.unsubAll("meshtastic.mqttclientproxymessage"))
        self.assertIsNone(mesh_web.start(b, {"enabled": True, "host": "0.0.0.0", "port": 0}))

    def test_disabled_by_default(self):
        self.assertIsNone(mesh_web.start(None, {}))

    def test_loopback_without_password_checks_host_header(self):
        b = make_bridge()
        self.addCleanup(b.stop)
        self.addCleanup(lambda: mmp.pub.unsubAll("meshtastic.mqttclientproxymessage"))
        web = mesh_web.start(b, {"enabled": True, "host": "127.0.0.1", "port": 0})
        self.addCleanup(web.shutdown)
        url = f"http://127.0.0.1:{web.port}/api/status"
        with urllib.request.urlopen(url, timeout=10) as r:
            self.assertEqual(r.status, 200)
        rebind = urllib.request.Request(url, headers={"Host": "evil.example.com"})
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(rebind, timeout=10)
        self.assertEqual(cm.exception.code, 403)


if __name__ == "__main__":
    unittest.main()
