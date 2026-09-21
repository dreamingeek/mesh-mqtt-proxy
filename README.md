# mesh-mqtt-proxy

Lets a Meshtastic node with no network hardware (e.g. RAK4631) publish to MQTT,
including **Map Reports**, by using a Linux box such as a Raspberry Pi as the
node's network connection over Bluetooth LE. It is the same job the phone apps
do under "MQTT client proxy".

```
RAK4631 --BLE--> Raspberry Pi (this script) --> MQTT broker
```

The node builds every MQTT message itself and hands it to the script, which
publishes it unchanged. Broker address, credentials, root topic and TLS are read
from the node's own MQTT settings, so there is nothing to keep in sync.

## 1. Configure the node

Do this once with the phone app, the web client or the CLI:

- **MQTT module**: enabled, **MQTT Client Proxy** on (`proxy_to_client_enabled`),
  and the server/credentials/root you want. Leaving the address blank uses the
  public server (`mqtt.meshtastic.org`).
- **Map reporting**: on, with the publish interval (minimum and default 3600 s)
  and position precision you are comfortable making public. The report is
  unencrypted.
- **Bluetooth**: enabled. A node without a screen normally uses the fixed pairing
  PIN `123456` unless you changed it.
- Firmware 2.3.2 or newer. Note firmware issue #8860 on the Meshtastic GitHub, where
  2.7.15 on nRF52 boards has been reported to force the proxy setting on. That
  is what you want here, but check the setting stuck.

Per the Meshtastic docs, the primary channel (and any channel used for MQTT) also needs
**Uplink** and **Downlink** enabled on the node. Note that Uplink makes the node
publish packets it hears on that channel, not just its own map report. The
script's own `downlink = false` still keeps broker traffic from being pushed back
to the node.

Using the CLI from the Pi (stop the service first, since the node accepts only one
Bluetooth connection: `sudo systemctl stop mesh-mqtt-proxy`):

```bash
M="sudo -u meshproxy /opt/mesh-mqtt-proxy/venv/bin/meshtastic --ble <MAC>"
$M --set mqtt.enabled true \
   --set mqtt.proxy_to_client_enabled true \
   --set mqtt.map_reporting_enabled true \
   --set mqtt.map_report_settings.publish_interval_secs 3600 \
   --set mqtt.map_report_settings.position_precision 14 \
   --set mqtt.map_report_settings.should_report_location true
$M --ch-index 0 --ch-set uplink_enabled true --ch-set downlink_enabled true
$M --set lora.config_ok_to_mqtt true    # "OK to MQTT"; meshmap.net lists this as a requirement
$M --get mqtt          # confirm the values stuck
```

`--get mqtt` only prints fields that are not false, so if `should_report_location`
is missing from the output, it is off and the node will probably not send map
reports. Saving the MQTT screen in a phone app may reset it, so after any app
change, re-check with `--get mqtt` (the startup log also warns).

A node with no GPS module needs a position for map reports; set a fixed one with
`--setlat`, `--setlon` and `--setalt`.

## 2. Set up the Pi

```bash
sudo apt install -y bluez python3-venv mosquitto-clients
sudo useradd --system --create-home --home-dir /opt/mesh-mqtt-proxy --groups bluetooth meshproxy
sudo -u meshproxy python3 -m venv /opt/mesh-mqtt-proxy/venv
sudo cp -r mesh_mqtt_proxy.py mesh_web.py web requirements.txt /opt/mesh-mqtt-proxy/
sudo -u meshproxy /opt/mesh-mqtt-proxy/venv/bin/pip install -r /opt/mesh-mqtt-proxy/requirements.txt
```

**Pair the node once.** The library does not do the pairing for you, and it needs
to happen before the service runs. Close the phone app first, since the node
accepts only one BLE connection at a time.

```bash
bluetoothctl
[bluetooth]# agent on
[bluetooth]# default-agent
[bluetooth]# scan on          # wait for your node, e.g. Meshtastic_a1b2
[bluetooth]# pair AA:BB:CC:DD:EE:FF     # enter the PIN (123456)
[bluetooth]# trust AA:BB:CC:DD:EE:FF
[bluetooth]# quit
```

Bonds are stored system-wide by `bluetoothd`, so the service user does not have to
be the one that paired.

Then find the address the script will use, and write the config:

```bash
/opt/mesh-mqtt-proxy/venv/bin/python /opt/mesh-mqtt-proxy/mesh_mqtt_proxy.py --scan
sudo cp config.example.toml /etc/mesh-mqtt-proxy.toml   # set [radio] address
```

## 3. Try it in the foreground

```bash
sudo -u meshproxy /opt/mesh-mqtt-proxy/venv/bin/python \
    /opt/mesh-mqtt-proxy/mesh_mqtt_proxy.py -c /etc/mesh-mqtt-proxy.toml -v
```

Look for: the node's MQTT config being logged, `connected to broker`, and about an
hour later (or whenever the node sends its first report) `published MAP REPORT`.
To watch the broker directly (adjust the region in the topic):

```bash
mosquitto_sub -h mqtt.meshtastic.org -u meshdev -P large4cats -t 'msh/US/2/map/#' -v
```

## 4. Run it as a service

```bash
sudo cp mesh-mqtt-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mesh-mqtt-proxy
journalctl -u mesh-mqtt-proxy -f
```

## 5. Web interface (optional)

A small local web app, in the spirit of client.meshtastic.com, that runs inside the
same service:

- **Dashboard**: Bluetooth link and MQTT broker state, last map report, traffic
  counters, recent relayed messages, node name/ID/firmware, and Pi load, memory
  and temperature.
- **Nodes**: every node your node has heard, like the Meshtastic apps: name and
  short-name badge, hops away (or via MQTT), last heard, **distance and direction
  from your node**, SNR, battery/voltage, hardware model and key status. Search,
  sort (last heard, distance, name, SNR, hops), an "online only" filter (heard in
  the last 2 hours), miles or kilometres, and click a row for role, position,
  uptime, channel utilisation and an OpenStreetMap link.
- **MQTT & Map**: MQTT on/off, client proxy, server, credentials, root topic,
  encryption/JSON/TLS, map reporting, "share location", map precision, interval
  and "OK to MQTT".
- **Channels**: uplink, downlink and position precision per channel.
- **Device**: owner names (region and preset are shown but read only).
- **Logs**: live log with level filter, search, pause and download.

Why inside the service: the node accepts only **one** Bluetooth connection, so a
separate web app could not talk to it while the proxy runs. The UI reads and
writes through the proxy's own connection instead, and the proxy never has to be
stopped. (The reverse is also true: while the service runs, the `meshtastic` CLI
cannot connect. Use the web UI, or `sudo systemctl stop mesh-mqtt-proxy` first.)

Turn it on in `/etc/mesh-mqtt-proxy.toml`:

```toml
[web]
enabled  = true
host     = "0.0.0.0"        # or "127.0.0.1" for this machine only
port     = 8080
username = "admin"
password = "choose-something-long"
```

```bash
sudo chown root:meshproxy /etc/mesh-mqtt-proxy.toml && sudo chmod 640 /etc/mesh-mqtt-proxy.toml
sudo cp mesh_mqtt_proxy.py mesh_web.py /opt/mesh-mqtt-proxy/ && sudo cp -r web /opt/mesh-mqtt-proxy/
sudo systemctl restart mesh-mqtt-proxy
```

Then browse to `http://<pi-hostname-or-ip>:8080/`.

How changes work: **Save to node** writes the settings and the node reboots to apply
them (about 30 seconds). The Bluetooth link drops and the proxy reconnects on its
own; the header pill shows the state and the dashboard tells you when it is back.
Only changed settings are written. Some changes ask for confirmation first: turning
off MQTT or the client proxy, enabling uplink on a non-primary (for example private
family) channel, and making an uplinked channel share exact positions. Live hints
also flag map settings that will not work (share location off, precision outside
meshmap.net's 10-16 bits, "OK to MQTT" off). The MQTT password is write-only: it is
never sent to the browser.

Security notes:

- The UI can change your node, so it needs the password whenever it is reachable
  from the network; it will not start on a non-loopback address without one.
- Basic auth over plain HTTP sends the password unencrypted on your LAN. For
  anything beyond a trusted LAN, put it behind your reverse proxy with TLS (bind
  to `127.0.0.1` and proxy to it, or keep the password and let the proxy add TLS).
  Keep the password set even behind a proxy.
- Changes from the browser need a custom header, which stops other web pages from
  posting to it, and no page is framed or cached.
- Logs show what the service logs (topics, counts), not passwords or keys.

The Nodes tab needs the node list, which the proxy would otherwise skip because it
is slow over Bluetooth. With the web UI enabled it is downloaded on each connect
(`load_nodes` under `[radio]` overrides this; if a big node list makes connecting
time out, raise `connect_timeout`). The list then stays current as the node
reports updates. Distances are measured from your node's own position; a node with
no GPS or fixed position gives no origin, so set `latitude`/`longitude` under
`[web]` as a fallback. Nodes that share no position show "-". The list is what
your node has heard, including positions other people chose to share with the mesh,
and it is only shown to whoever can log in to the UI.

Limits: the log view holds the last few thousand lines since the service started
(use `journalctl -u mesh-mqtt-proxy` for older history). Settings are editable only
while the Bluetooth link is up. With the node list off (`load_nodes = false`) the
current owner names show as unknown; you can still set new ones.

## Checking that the node reached the map

1. On the Pi: `journalctl -u mesh-mqtt-proxy | grep -E "MAP REPORT|stats"`. A
   `published MAP REPORT` line means the report left the Pi; the `up` counter in
   the `stats` lines counts everything sent to the broker.
2. On a map that reads the public server:
   - meshmap.net: search by node name or ID. It refreshes about every minute and
     drops a node whose position has not updated for 8 hours.
   - meshtastic.liamcottle.net: `?node_id=<decimal node number>` in the URL, e.g.
     `python3 -c "print(int('a1b2c3d4', 16))"` for node `!a1b2c3d4`.
3. meshmap.net lists these requirements: recent firmware, the default primary
   channel settings, "OK to MQTT" on, a position (GPS, phone or fixed), and location
   precision between 364 m and 23.3 km.

## Behavior worth knowing

- **Uplink only by default.** Map Reports are uplink. Set `downlink = true` only to
  bring broker traffic back to the node; on the public server that can be a lot
  of traffic for a BLE link, so the queue is bounded and drops when full.
- **Reconnects.** The script reconnects with backoff when the BLE link drops. It
  also sends a keepalive every 60 s and watches the library's read thread, because
  the library's BLE reader can stop without announcing a disconnect.
- **Range.** Bluetooth range is roughly 10 to 30 m through structure. Put the Pi
  near the node.
- **Downlink topics** are derived from the channels that have downlink enabled
  on the node. Unnamed channels use the preset name (e.g. `LongFast`); if yours does
  not match, add topics via `extra_subscriptions`.

## Troubleshooting

**`No Meshtastic BLE peripheral with identifier or address '...' found`** even though
the node shows as connected. The Meshtastic library only connects to nodes it sees
*advertising* during a 10 s scan, and a node that already has a connection stops
advertising. This commonly happens right after `bluetoothctl pair`, which can leave
the link open. The script now falls back to connecting straight to the MAC address
when the scan finds nothing (you will see a `connecting directly by address`
warning). If that still fails:

1. Check who holds the connection: `bluetoothctl info <MAC>` (look at `Connected:`).
2. If it is the Pi, drop it with `bluetoothctl disconnect <MAC>`. Pairing and trust
   stay in place.
3. If it is a phone or another computer, close that app; the node accepts one
   connection at a time.
4. Run `mesh_mqtt_proxy.py --scan` to confirm the node is now visible.

## Tests

```bash
pip install -r requirements.txt
sudo apt install mosquitto      # tests start their own private instance on port 18830
python -m unittest discover -s tests -v
```

The tests use a real Mosquitto broker and a fake radio, so they check the MQTT
side, reconnect logic and topic handling, but not real Bluetooth. The web tests
use a fake node built from the real Meshtastic protobufs and check validation, the
confirmation rules, the write sequence (begin transaction, write, commit) and the
HTTP security. Real-radio behaviour still needs to be tried on your hardware.

## License and disclaimer

Licensed under the GNU General Public License v3.0 or later (see `LICENSE`). It
builds on the Meshtastic Python library, which is GPL-3.0, and this keeps the
licenses compatible.

This is an independent community project. It is not affiliated with or endorsed by
the Meshtastic project; "Meshtastic" is a trademark of Meshtastic LLC. The web UI
writes settings to your node and the software comes with no warranty, so check the
changes it lists before you save. The MQTT server credentials in the example config
(`meshdev` / `large4cats`) are the shared, published ones for the public Meshtastic
server, not private secrets. Please follow the public server's usage guidelines.
