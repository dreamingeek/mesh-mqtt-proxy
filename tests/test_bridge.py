# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests for mesh_mqtt_proxy against a real local Mosquitto broker and a fake radio.

Needs the `mosquitto` binary (apt install mosquitto).  No Bluetooth hardware is
involved: FakeInterface stands in for meshtastic's BLEInterface and we inject
proxy messages the same way the meshtastic library does (via pypubsub).

Run:  python -m unittest discover -s tests -v
"""
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

import paho.mqtt.client as mqtt
from pubsub import pub

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import mesh_mqtt_proxy as mmp  # noqa: E402
from meshtastic.protobuf import channel_pb2, config_pb2, mesh_pb2, module_config_pb2  # noqa: E402

PORT = 18830


def wait_for(cond, timeout=10.0, what="condition"):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


class FakeInterface:
    def __init__(self, downlink_channels=("LongFast",), map_reporting=True):
        cfg = module_config_pb2.ModuleConfig()
        cfg.mqtt.enabled = True
        cfg.mqtt.proxy_to_client_enabled = True
        cfg.mqtt.address = f"127.0.0.1:{PORT}"
        cfg.mqtt.root = "msh/US"
        cfg.mqtt.map_reporting_enabled = map_reporting
        cfg.mqtt.map_report_settings.publish_interval_secs = 3600
        cfg.mqtt.map_report_settings.position_precision = 14
        lc = config_pb2.Config()
        lc.lora.use_preset = True
        lc.lora.modem_preset = config_pb2.Config.LoRaConfig.ModemPreset.LONG_FAST
        chans = []
        for i, name in enumerate(("", "Private")):
            ch = channel_pb2.Channel(index=i)
            ch.role = channel_pb2.Channel.Role.PRIMARY if i == 0 else channel_pb2.Channel.Role.SECONDARY
            ch.settings.name = name
            ch.settings.downlink_enabled = (name or "LongFast") in downlink_channels
            chans.append(ch)
        self.localNode = SimpleNamespace(moduleConfig=cfg, localConfig=lc, channels=chans)
        self.myInfo = SimpleNamespace(my_node_num=0xDEADBEEF)
        self.isConnected = threading.Event()
        self.isConnected.set()
        self.client = object()
        self._want_receive = True
        self.sent = []
        self.heartbeats = 0
        self.closed = False

    def sendMqttClientProxyMessage(self, topic, data):
        self.sent.append((topic, data))

    def sendHeartbeat(self):
        self.heartbeats += 1

    def close(self):
        self.closed = True


class BridgeTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        conf = Path(cls.tmp.name) / "mosquitto.conf"
        conf.write_text(f"listener {PORT} 127.0.0.1\nallow_anonymous true\n")
        cls.broker = subprocess.Popen(["mosquitto", "-c", str(conf)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        end = time.monotonic() + 10
        while time.monotonic() < end:
            try:
                socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError("mosquitto did not start")

    @classmethod
    def tearDownClass(cls):
        cls.broker.terminate()
        cls.broker.wait(5)
        cls.tmp.cleanup()

    def start_bridge(self, downlink=False, **fake_kwargs):
        self.fakes = []

        def factory(_addr, _timeout):
            f = FakeInterface(**fake_kwargs)
            self.fakes.append(f)
            return f

        s = mmp.Settings(address="AA:BB:CC:DD:EE:FF", downlink=downlink,
                         heartbeat_interval=0.5, reconnect_min=0.2, reconnect_max=0.5)
        self.bridge = mmp.Bridge(s, interface_factory=factory)
        self.thread = threading.Thread(target=self.bridge.run, daemon=True)
        self.thread.start()
        wait_for(lambda: self.bridge._mqtt is not None and self.bridge._mqtt.is_connected(),
                 what="bridge to connect to broker")
        return self.bridge

    def make_listener(self, topic_filter):
        got = []
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"test-{time.time_ns()}")
        c.on_message = lambda _c, _u, m: got.append((m.topic, bytes(m.payload), m.retain))
        c.connect("127.0.0.1", PORT)
        c.subscribe(topic_filter)
        c.loop_start()
        time.sleep(0.3)  # let the SUBACK land
        self.addCleanup(lambda: (c.loop_stop(), c.disconnect()))
        return c, got

    def tearDown(self):
        if getattr(self, "bridge", None):
            self.bridge.stop()
            self.thread.join(20)
            self.assertFalse(self.thread.is_alive(), "bridge thread did not stop")


class UplinkTests(BridgeTestBase):
    def test_map_report_is_published(self):
        _, got = self.make_listener("msh/#")
        self.start_bridge()
        msg = mesh_pb2.MqttClientProxyMessage(topic="msh/US/2/map/", data=b"\x0a\x01\x02map")
        pub.sendMessage("meshtastic.mqttclientproxymessage",
                        proxymessage=msg, interface=self.fakes[0])
        wait_for(lambda: got, what="map report at broker")
        self.assertEqual(got[0][:2], ("msh/US/2/map/", b"\x0a\x01\x02map"))
        self.assertEqual(self.bridge.stats["up"], 1)
        self.assertEqual(dict(self.bridge.kinds), {"map": 1})
        self.assertIsNotNone(self.bridge.last_map_report)
        self.assertIn("by_kind={'map': 1}", self.bridge.stats_text())

    def test_text_payload_and_retain(self):
        _, got = self.make_listener("msh/#")
        self.start_bridge()
        msg = mesh_pb2.MqttClientProxyMessage(topic="msh/US/2/stat/!deadbeef",
                                              text="online", retained=True)
        pub.sendMessage("meshtastic.mqttclientproxymessage",
                        proxymessage=msg, interface=self.fakes[0])
        wait_for(lambda: got, what="text message at broker")
        self.assertEqual(got[0][1], b"online")
        # Retained flag is stored by the broker: a *new* subscriber sees it.
        _, late = self.make_listener("msh/US/2/stat/#")
        wait_for(lambda: late, what="retained message for late subscriber")
        self.assertTrue(late[0][2])

    def test_messages_from_other_interfaces_ignored(self):
        _, got = self.make_listener("msh/#")
        self.start_bridge()
        msg = mesh_pb2.MqttClientProxyMessage(topic="msh/US/2/map/", data=b"x")
        pub.sendMessage("meshtastic.mqttclientproxymessage",
                        proxymessage=msg, interface=FakeInterface())
        time.sleep(0.5)
        self.assertEqual(got, [])

    def test_uplink_does_not_forward_downlink_when_disabled(self):
        pub_client, _ = self.make_listener("nothing/#")
        self.start_bridge(downlink=False)
        pub_client.publish("msh/US/2/e/LongFast/!abcd", b"hello")
        time.sleep(0.7)
        self.assertEqual(self.fakes[0].sent, [])


class DownlinkTests(BridgeTestBase):
    def test_downlink_only_for_enabled_channels(self):
        pub_client, _ = self.make_listener("nothing/#")
        self.start_bridge(downlink=True, downlink_channels=("LongFast",))
        # Primary channel has an empty name -> firmware calls it "LongFast".
        pub_client.publish("msh/US/2/e/LongFast/!abcd", b"for-primary")
        # "Private" has downlink disabled in this fake, so it must not be subscribed.
        pub_client.publish("msh/US/2/e/Private/!abcd", b"for-private")
        wait_for(lambda: self.fakes[0].sent, what="downlink to radio")
        time.sleep(0.5)
        self.assertEqual(self.fakes[0].sent, [("msh/US/2/e/LongFast/!abcd", b"for-primary")])


class ReconnectTests(BridgeTestBase):
    def test_reconnects_after_ble_loss(self):
        self.start_bridge()
        first = self.fakes[0]
        pub.sendMessage("meshtastic.connection.lost", interface=first)
        wait_for(lambda: len(self.fakes) >= 2, what="second BLE session")
        self.assertTrue(first.closed)
        wait_for(lambda: self.bridge._mqtt is not None and self.bridge._mqtt.is_connected(),
                 what="broker reconnect")

    def test_health_check_catches_silent_death(self):
        # The meshtastic BLE read thread can die without emitting connection.lost.
        self.start_bridge()
        self.fakes[0]._want_receive = False
        wait_for(lambda: len(self.fakes) >= 2, what="session restart after silent BLE death")

    def test_heartbeats_are_sent(self):
        self.start_bridge()
        wait_for(lambda: self.fakes[0].heartbeats >= 1, what="heartbeat")


class PureFunctionTests(unittest.TestCase):
    def test_topic_kind(self):
        self.assertEqual(mmp.topic_kind("msh/US/2/map/"), "map")
        self.assertEqual(mmp.topic_kind("msh/US/CA/2/map"), "map")
        self.assertEqual(mmp.topic_kind("msh/US/CA/2/e/LongFast/!abcd1234"), "e")
        self.assertEqual(mmp.topic_kind("msh/US/2/json/mqtt/"), "json")
        self.assertEqual(mmp.topic_kind("msh/US/2/stat/!abcd1234"), "stat")
        self.assertEqual(mmp.topic_kind("something/else"), "other")

    def test_resolve_defaults(self):
        node = module_config_pb2.ModuleConfig().mqtt
        b = mmp.resolve_broker(node, {})
        self.assertEqual((b.host, b.port, b.username, b.password, b.tls, b.root),
                         ("mqtt.meshtastic.org", 1883, "meshdev", "large4cats", False, "msh"))

    def test_blank_root_uses_region(self):
        node = module_config_pb2.ModuleConfig().mqtt
        lora = config_pb2.Config().lora
        lora.region = config_pb2.Config.LoRaConfig.RegionCode.US
        self.assertEqual(mmp.region_code(lora), "US")
        self.assertEqual(mmp.resolve_broker(node, {}, "US").root, "msh/US")
        node.root = "custom/root"
        self.assertEqual(mmp.resolve_broker(node, {}, "US").root, "custom/root")
        self.assertEqual(mmp.region_code(config_pb2.Config().lora), "")

    def test_resolve_custom_tls_and_port(self):
        node = module_config_pb2.ModuleConfig().mqtt
        node.address = "broker.example.com"
        node.username, node.password = "u", "p"
        node.tls_enabled = True
        node.root = "/msh/US/"
        b = mmp.resolve_broker(node, {})
        self.assertEqual((b.host, b.port, b.username, b.tls, b.root),
                         ("broker.example.com", 8883, "u", True, "msh/US"))
        self.assertEqual(mmp.resolve_broker(node, {"port": 9999}).port, 9999)

    def test_resolve_host_with_port_and_override(self):
        node = module_config_pb2.ModuleConfig().mqtt
        node.address = "10.0.0.5:1884"
        self.assertEqual(mmp.resolve_broker(node, {}).port, 1884)
        self.assertEqual(mmp.resolve_broker(node, {"host": "other"}).host, "other")
        # Custom broker with blank creds must not get the public-server creds.
        self.assertEqual(mmp.resolve_broker(node, {}).username, "")

    def test_topics_json_and_extra(self):
        f = FakeInterface(downlink_channels=("LongFast", "Private"))
        f.localNode.moduleConfig.mqtt.json_enabled = True
        b = mmp.resolve_broker(f.localNode.moduleConfig.mqtt, {})
        t = mmp.downlink_topics(f.localNode, b, ["msh/US/2/e/PKI/#"])
        self.assertEqual(t, [
            "msh/US/2/e/LongFast/#", "msh/US/2/json/LongFast/#",
            "msh/US/2/e/Private/#", "msh/US/2/json/Private/#",
            "msh/US/2/e/PKI/#",
        ])


class NodeStateWarningTests(unittest.TestCase):
    def _log_state(self, consent):
        f = FakeInterface()
        f.localNode.moduleConfig.mqtt.map_report_settings.should_report_location = consent
        bridge = mmp.Bridge(mmp.Settings(address="AA:BB:CC:DD:EE:FF"), interface_factory=lambda *_: f)
        self.addCleanup(lambda: (pub.unsubscribe(bridge._on_proxy, "meshtastic.mqttclientproxymessage"),
                                 pub.unsubscribe(bridge._on_lost, "meshtastic.connection.lost")))
        mqtt_cfg = f.localNode.moduleConfig.mqtt
        with self.assertLogs("mesh-mqtt-proxy", level="INFO") as cm:
            bridge._log_node_state(f.localNode, mqtt_cfg, mmp.resolve_broker(mqtt_cfg, {}))
        return "\n".join(cm.output)

    def test_warns_when_consent_flag_is_off(self):
        self.assertIn("should_report_location is false", self._log_state(False))

    def test_no_warning_when_consent_flag_is_on(self):
        out = self._log_state(True)
        self.assertNotIn("should_report_location is false", out)
        self.assertIn("should_report_location=True", out)


class DirectConnectTests(unittest.TestCase):
    """A node that is already connected does not advertise, so the scan misses it."""

    def setUp(self):
        from meshtastic.ble_interface import BLEInterface
        self.BLE = BLEInterface
        self.cls = mmp.direct_ble_interface_class()
        self.obj = object.__new__(self.cls)  # skip __init__: no Bluetooth needed

    def test_falls_back_to_address_when_scan_finds_nothing(self):
        from unittest import mock
        with mock.patch.object(self.BLE, "scan", return_value=[]):
            dev = self.obj.find_device("AA:BB:CC:DD:EE:FF")
        self.assertEqual(dev.address, "AA:BB:CC:DD:EE:FF")

    def test_name_instead_of_mac_still_fails(self):
        from unittest import mock
        with mock.patch.object(self.BLE, "scan", return_value=[]):
            with self.assertRaises(self.BLE.BLEError):
                self.obj.find_device("Meshtastic_a1b2")

    def test_uses_scan_result_when_advertising(self):
        from unittest import mock
        found = SimpleNamespace(address="AA:BB:CC:DD:EE:FF", name="Meshtastic_eeff")
        with mock.patch.object(self.BLE, "scan", return_value=[found]):
            self.assertIs(self.obj.find_device("AA:BB:CC:DD:EE:FF"), found)


if __name__ == "__main__":
    unittest.main()


class NodeLoadingTests(unittest.TestCase):
    def test_wants_nodes_follows_web_setting_unless_overridden(self):
        self.assertFalse(mmp.wants_nodes(mmp.Settings(address="x")))
        self.assertTrue(mmp.wants_nodes(mmp.Settings(address="x", web={"enabled": True})))
        self.assertFalse(mmp.wants_nodes(mmp.Settings(address="x", web={"enabled": True}, load_nodes=False)))
        self.assertTrue(mmp.wants_nodes(mmp.Settings(address="x", load_nodes=True)))

    def test_default_factory_passes_no_nodes_flag(self):
        seen = {}

        class Fake:
            def __init__(self, address, **kw):
                seen.update(kw)
        with mock.patch.object(mmp, "direct_ble_interface_class", return_value=Fake):
            mmp.default_interface_factory("AA:BB:CC:DD:EE:FF", 30)
            self.assertTrue(seen["noNodes"])
            mmp.default_interface_factory("AA:BB:CC:DD:EE:FF", 30, load_nodes=True)
            self.assertFalse(seen["noNodes"])

    def test_bridge_binds_the_setting_to_its_factory(self):
        seen = {}
        with mock.patch.object(mmp, "direct_ble_interface_class",
                               return_value=lambda a, **kw: seen.update(kw)):
            b = mmp.Bridge(mmp.Settings(address="AA:BB:CC:DD:EE:FF", web={"enabled": True}))
            self.addCleanup(b.stop)
            self.addCleanup(lambda: pub.unsubAll("meshtastic.mqttclientproxymessage"))
            b._factory("AA:BB:CC:DD:EE:FF", 30)
        self.assertFalse(seen["noNodes"])
