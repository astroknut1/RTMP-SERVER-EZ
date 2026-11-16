"""
EZ RTMP SERVER (Windows, pure Python, nothing to download)
====================================================================
Run:  start.bat   (or: python server.py)

  camera / OBS / app  --RTMP-->  this server  --HTTP-FLV-->  your browser
  Player page    : http://localhost:8080
  Settings page  : http://localhost:8080/settings
"""
import http.server
import json
import os
import queue
import re
import socket
import struct
import sys
import threading
import time
import webbrowser
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(HERE, "settings.json")
REC_DIR = os.path.join(HERE, "recordings")
WEB_PORT = 8080
MASK = 0xFFFFFFFF

DEFAULTS = {
    "title": "My Stream",
    "rtmp_port": 1935,
    "stream_key": "stream",   # empty = accept any key
    "record": False,
    "low_latency": True,
}
SETTINGS = dict(DEFAULTS)


# =====================================================================
# Settings
# =====================================================================
def load_settings():
    s = dict(DEFAULTS)
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        s.update({k: v for k, v in saved.items() if k in DEFAULTS})
    except (OSError, ValueError):
        pass
    return s


def save_settings(s):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)


def validate(data):
    """Returns (clean_settings, error_message)."""
    s = dict(DEFAULTS)
    try:
        s["title"] = str(data.get("title", s["title"])).strip()[:60] or DEFAULTS["title"]
        s["rtmp_port"] = int(data.get("rtmp_port", s["rtmp_port"]))
        s["stream_key"] = str(data.get("stream_key", s["stream_key"])).strip()
        s["record"] = bool(data.get("record", False))
        s["low_latency"] = bool(data.get("low_latency", True))
    except (TypeError, ValueError):
        return None, "Port must be a number."
    if not 1 <= s["rtmp_port"] <= 65535:
        return None, "Port must be between 1 and 65535."
    if s["rtmp_port"] == WEB_PORT:
        return None, f"Port {WEB_PORT} is used by the web page. Pick another."
    if not re.fullmatch(r"[A-Za-z0-9_-]*", s["stream_key"]):
        return None, "Stream key: letters, numbers, - and _ only."
    return s, None


def key_ok(app, name):
    key = SETTINGS.get("stream_key", "")
    if not key:
        return True
    name = name.split("?")[0]
    full = (app.strip("/") + "/" + name).strip("/")
    return name == key or full == key or full.endswith("/" + key)


# =====================================================================
# AMF0 (the little data format RTMP commands use)
# =====================================================================
def amf_read(b, i):
    t = b[i]
    i += 1
    if t == 0x00:
        return struct.unpack(">d", b[i:i + 8])[0], i + 8
    if t == 0x01:
        return b[i] != 0, i + 1
    if t == 0x02:
        n = struct.unpack(">H", b[i:i + 2])[0]
        i += 2
        return b[i:i + n].decode("utf-8", "replace"), i + n
    if t in (0x03, 0x08):
        if t == 0x08:
            i += 4
        obj = {}
        while True:
            n = struct.unpack(">H", b[i:i + 2])[0]
            i += 2
            if n == 0 and b[i] == 0x09:
                return obj, i + 1
            key = b[i:i + n].decode("utf-8", "replace")
            i += n
            obj[key], i = amf_read(b, i)
    if t in (0x05, 0x06):
        return None, i
    if t == 0x0A:
        n = struct.unpack(">I", b[i:i + 4])[0]
        i += 4
        arr = []
        for _ in range(n):
            v, i = amf_read(b, i)
            arr.append(v)
        return arr, i
    if t == 0x0B:
        return struct.unpack(">d", b[i:i + 8])[0], i + 10
    if t == 0x0C:
        n = struct.unpack(">I", b[i:i + 4])[0]
        i += 4
        return b[i:i + n].decode("utf-8", "replace"), i + n
    raise ValueError("unknown AMF type")


def amf_all(b):
    out, i = [], 0
    while i < len(b):
        try:
            v, i = amf_read(b, i)
        except (ValueError, IndexError, struct.error):
            break
        out.append(v)
    return out


def amf_w(v):
    if v is None:
        return b"\x05"
    if isinstance(v, bool):
        return b"\x01" + (b"\x01" if v else b"\x00")
    if isinstance(v, (int, float)):
        return b"\x00" + struct.pack(">d", float(v))
    if isinstance(v, str):
        e = v.encode("utf-8")
        return b"\x02" + struct.pack(">H", len(e)) + e
    if isinstance(v, dict):
        out = b"\x03"
        for k, x in v.items():
            ke = k.encode("utf-8")
            out += struct.pack(">H", len(ke)) + ke + amf_w(x)
        return out + b"\x00\x00\x09"
    raise TypeError("cannot encode")


# =====================================================================
# FLV helpers (RTMP audio/video payloads are already FLV tag bodies)
# =====================================================================
def flv_header(flags):
    return b"FLV\x01" + bytes([flags]) + struct.pack(">I", 9) + b"\x00\x00\x00\x00"


def flv_tag(kind, ts, body):
    n = len(body)
    ts &= MASK
    head = (bytes([kind]) + n.to_bytes(3, "big") + (ts & 0xFFFFFF).to_bytes(3, "big")
            + bytes([(ts >> 24) & 0xFF]) + b"\x00\x00\x00")
    return head + body + struct.pack(">I", n + 11)


# =====================================================================
# Hub: holds the live stream and hands it to viewers
# =====================================================================
class Viewer:
    def __init__(self):
        self.q = queue.Queue(maxsize=4000)
        self.dead = False

    def kill(self):
        self.dead = True
        try:
            self.q.put_nowait(None)
        except queue.Full:
            pass


class Hub:
    def __init__(self):
        self.lock = threading.Lock()
        self.viewers = set()
        self.live = False
        self.publisher = None
        self._clear()

    def _clear(self):
        self.addr = ""
        self.meta = self.vseq = self.aseq = None
        self.gop = []
        self.started = 0.0
        self.win_t = time.time()
        self.win_b = 0
        self.kbps = 0
        self.rec = None
        self.rec_name = ""

    # ---- recording ----
    def _rec_open(self):
        try:
            os.makedirs(REC_DIR, exist_ok=True)
            self.rec_name = datetime.now().strftime("stream-%Y%m%d-%H%M%S.flv")
            self.rec = open(os.path.join(REC_DIR, self.rec_name), "wb")
            self.rec.write(flv_header(0x05))
        except OSError:
            self.rec = None

    def _rec_write(self, tag):
        if self.rec:
            try:
                self.rec.write(flv_tag(*tag))
            except OSError:
                self.rec = None

    def _rec_close(self):
        if self.rec:
            try:
                self.rec.close()
            except OSError:
                pass
        self.rec = None

    def _kill_viewers(self):
        for v in list(self.viewers):
            v.kill()
        self.viewers.clear()

    def _fan(self, tag):
        for v in list(self.viewers):
            try:
                v.q.put_nowait(tag)
            except queue.Full:
                v.kill()
                self.viewers.discard(v)

    # ---- publisher side ----
    def start(self, conn, addr):
        with self.lock:
            old = self.publisher
            self._rec_close()
            self._kill_viewers()
            self._clear()
            self.live = True
            self.publisher = conn
            self.addr = addr
            self.started = time.time()
            if SETTINGS.get("record"):
                self._rec_open()
        if old is not None and old is not conn:
            old.close()
        print(f"[stream] started from {addr}")

    def stop(self, conn):
        with self.lock:
            if self.publisher is not conn:
                return
            self.live = False
            self.publisher = None
            self._rec_close()
            self._kill_viewers()
            self._clear()
        print("[stream] stopped")

    def set_meta(self, body):
        with self.lock:
            if self.live:
                self.meta = (18, 0, body)
                self._rec_write(self.meta)
                self._fan(self.meta)

    def push(self, kind, ts, body):
        if len(body) < 2:
            return
        with self.lock:
            if not self.live:
                return
            now = time.time()
            self.win_b += len(body)
            if now - self.win_t >= 2:
                self.kbps = int(self.win_b * 8 / 1000 / (now - self.win_t))
                self.win_t, self.win_b = now, 0

            tag = (kind, ts, body)
            if kind == 9:  # video
                frame, codec = body[0] >> 4, body[0] & 15
                if codec == 7 and body[1] == 0:          # H.264 config
                    tag = (9, 0, body)
                    self.vseq = tag
                elif frame == 1:                          # keyframe: new GOP
                    self.gop = [tag]
                elif self.gop:
                    self.gop.append(tag)
            else:  # audio
                if (body[0] >> 4) == 10 and body[1] == 0:  # AAC config
                    tag = (8, 0, body)
                    self.aseq = tag
                elif self.gop:
                    self.gop.append(tag)
            if len(self.gop) > 3000:
                self.gop = []
            self._rec_write(tag)
            self._fan(tag)

    # ---- viewer side ----
    def subscribe(self):
        with self.lock:
            if not (self.live and self.vseq and self.gop):
                return None
            if not self.aseq and time.time() - self.started < 2.0:
                return None  # give the audio config a moment to arrive
            v = Viewer()
            init = [t for t in (self.meta, self.vseq, self.aseq) if t] + list(self.gop)
            flags = 0x01 | (0x04 if self.aseq else 0)
            base = self.gop[0][1]
            self.viewers.add(v)
            return v, init, flags, base

    def unsubscribe(self, v):
        with self.lock:
            self.viewers.discard(v)

    def status(self):
        with self.lock:
            return {
                "live": self.live,
                "viewers": len(self.viewers),
                "kbps": self.kbps if self.live else 0,
                "uptime": int(time.time() - self.started) if self.live else 0,
                "from": self.addr,
                "recording": self.rec_name if self.rec else "",
            }


hub = Hub()


# =====================================================================
# RTMP connection (one per camera / client)
# =====================================================================
class Conn:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.rf = sock.makefile("rb")
        self.in_chunk = 128
        self.out_chunk = 128
        self.chunks = {}
        self.bytes_in = 0
        self.last_ack = 0
        self.ack_window = 2500000
        self.app = ""
        self.publishing = False
        self.send_lock = threading.Lock()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def read(self, n):
        d = self.rf.read(n)
        if d is None or len(d) < n:
            raise EOFError
        self.bytes_in += n
        return d

    # ---- sending ----
    def send(self, csid, mtype, sid, payload):
        out = bytearray([csid & 0x3F])
        out += (0).to_bytes(3, "big") + len(payload).to_bytes(3, "big")
        out.append(mtype)
        out += sid.to_bytes(4, "little")
        for i in range(0, len(payload), self.out_chunk):
            if i:
                out.append(0xC0 | (csid & 0x3F))
            out += payload[i:i + self.out_chunk]
        with self.send_lock:
            self.sock.sendall(bytes(out))

    def send_cmd(self, csid, sid, values):
        self.send(csid, 20, sid, b"".join(amf_w(v) for v in values))

    def status_msg(self, sid, level, code, desc):
        self.send_cmd(5, sid, ["onStatus", 0.0, None,
                               {"level": level, "code": code, "description": desc}])

    # ---- handshake ----
    def handshake(self):
        if self.read(1)[0] != 3:
            raise ValueError("not RTMP")
        c1 = self.read(1536)
        s1 = struct.pack(">II", 0, 0) + os.urandom(1528)
        self.sock.sendall(b"\x03" + s1 + c1)
        self.read(1536)

    # ---- chunk reader ----
    def read_message(self):
        b = self.read(1)[0]
        fmt, csid = b >> 6, b & 0x3F
        if csid == 0:
            csid = self.read(1)[0] + 64
        elif csid == 1:
            d = self.read(2)
            csid = d[0] + (d[1] << 8) + 64
        st = self.chunks.get(csid)
        if st is None:
            st = self.chunks[csid] = {"ts": 0, "delta": 0, "len": 0, "type": 0, "sid": 0,
                                      "buf": bytearray(), "rem": 0, "ext": False}

        if fmt == 3:
            if st["ext"]:
                self.read(4)
            if st["rem"] == 0:  # new message reusing the previous header
                st["ts"] = (st["ts"] + st["delta"]) & MASK
                st["rem"] = st["len"]
                st["buf"] = bytearray()
        else:
            size = (11, 7, 3)[fmt]
            h = self.read(size)
            val = int.from_bytes(h[0:3], "big")
            st["ext"] = val == 0xFFFFFF
            if st["ext"]:
                val = int.from_bytes(self.read(4), "big")
            if fmt <= 1:
                st["len"] = int.from_bytes(h[3:6], "big")
                st["type"] = h[6]
            if fmt == 0:
                st["sid"] = int.from_bytes(h[7:11], "little")
                st["ts"] = val
                st["delta"] = val
            else:
                st["delta"] = val
                st["ts"] = (st["ts"] + val) & MASK
            st["rem"] = st["len"]
            st["buf"] = bytearray()

        n = min(self.in_chunk, st["rem"])
        if n:
            st["buf"] += self.read(n)
            st["rem"] -= n
        if st["rem"] == 0:
            payload = bytes(st["buf"])
            st["buf"] = bytearray()
            return st["type"], st["ts"], st["sid"], payload
        return None

    # ---- message handling ----
    def handle(self, t, ts, sid, p):
        if t == 1 and len(p) >= 4:
            self.in_chunk = max(1, struct.unpack(">I", p[:4])[0] & 0x7FFFFFFF)
        elif t == 5 and len(p) >= 4:
            self.ack_window = max(1, struct.unpack(">I", p[:4])[0])
        elif t in (20, 17):
            self.on_command(amf_all(p[1:] if t == 17 else p), sid)
        elif t in (18, 15):
            if self.publishing:
                if t == 15:
                    p = p[1:]
                pre = amf_w("@setDataFrame")
                if p.startswith(pre):
                    p = p[len(pre):]
                if p.startswith(amf_w("onMetaData")):
                    hub.set_meta(p)
        elif t in (8, 9):
            if self.publishing:
                hub.push(t, ts, p)

    def on_command(self, args, sid):
        if not args or not isinstance(args[0], str):
            return
        name = args[0]
        txn = args[1] if len(args) > 1 and isinstance(args[1], float) else 0.0

        if name == "connect":
            obj = args[2] if len(args) > 2 and isinstance(args[2], dict) else {}
            self.app = str(obj.get("app", "")).split("?")[0]
            self.send(2, 5, 0, struct.pack(">I", 5000000))
            self.send(2, 6, 0, struct.pack(">IB", 5000000, 2))
            self.send(2, 4, 0, struct.pack(">HI", 0, 0))
            self.send_cmd(3, 0, [
                "_result", txn,
                {"fmsVer": "FMS/3,0,1,123", "capabilities": 31.0},
                {"level": "status", "code": "NetConnection.Connect.Success",
                 "description": "Connection succeeded.", "objectEncoding": 0.0}])
        elif name == "createStream":
            self.send_cmd(3, 0, ["_result", txn, None, 1.0])
        elif name == "publish":
            pub = str(args[3]) if len(args) > 3 else ""
            if not key_ok(self.app, pub):
                print(f"[stream] rejected: wrong stream key '{pub}' from {self.addr[0]}")
                self.status_msg(sid, "error", "NetStream.Publish.BadName", "Bad stream key")
                raise EOFError
            hub.start(self, self.addr[0])
            self.publishing = True
            self.send(2, 4, 0, struct.pack(">HI", 0, sid))
            self.status_msg(sid, "status", "NetStream.Publish.Start", "Publishing")
        elif name in ("deleteStream", "closeStream"):
            raise EOFError
        elif name == "play":
            self.status_msg(sid, "error", "NetStream.Play.StreamNotFound", "Watch in the web page")
            raise EOFError
        elif txn > 0:  # releaseStream, FCPublish, _checkbw, ...
            self.send_cmd(3, 0, ["_result", txn, None, None])

    def run(self):
        try:
            self.sock.settimeout(30)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.handshake()
            while True:
                m = self.read_message()
                if m:
                    self.handle(*m)
                if self.bytes_in - self.last_ack >= self.ack_window:
                    self.last_ack = self.bytes_in
                    self.send(2, 3, 0, struct.pack(">I", self.bytes_in & MASK))
        except Exception:  # noqa: BLE001  (disconnects, timeouts, junk data)
            pass
        finally:
            if self.publishing:
                hub.stop(self)
            self.close()


class RtmpServer:
    def __init__(self):
        self.sock = None
        self.port = None
        self.error = ""

    def start(self, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if os.name != "nt":
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", port))
            s.listen(16)
            s.settimeout(1.0)
        except OSError:
            self.error = f"Port {port} is already in use by another program."
            return False
        old, self.sock, self.port, self.error = self.sock, s, port, ""
        if old:
            try:
                old.close()
            except OSError:
                pass
        threading.Thread(target=self._accept, args=(s,), daemon=True).start()
        return True

    def _accept(self, s):
        while True:
            try:
                c, a = s.accept()
            except socket.timeout:
                if self.sock is not s:
                    return
                continue
            except OSError:
                return
            threading.Thread(target=Conn(c, a).run, daemon=True).start()


rtmp = RtmpServer()


# =====================================================================
# Web server (player, settings, API, live.flv)
# =====================================================================
def lan_ip():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"


LAN_IP = lan_ip()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def is_local(self):
        return self.client_address[0] in ("127.0.0.1", "::1", "::ffff:127.0.0.1", LAN_IP)

    def reply(self, code, body, ctype="application/json", cors=False):
        if isinstance(body, (dict, list)):
            body = json.dumps(body)
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if cors:  # lets player.html on other PCs read the status
            self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def page(self, name):
        with open(os.path.join(HERE, name), "rb") as f:
            self.reply(200, f.read(), "text/html")

    def serve_flv(self):
        sub = None
        for _ in range(40):
            sub = hub.subscribe()
            if sub or not hub.live:
                break
            time.sleep(0.1)
        if not sub:
            return self.reply(404, {"error": "no stream"}, cors=True)
        v, init, flags, base = sub
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

        def out(tag):
            kind, ts, body = tag
            if ts:
                ts = (ts - base) & MASK
                if ts > 0x7FFFFFFF:
                    ts = 0
            return flv_tag(kind, ts, body)

        try:
            self.send_response(200)
            self.send_header("Content-Type", "video/x-flv")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(flv_header(flags) + b"".join(out(t) for t in init))
            while not v.dead:
                try:
                    tag = v.q.get(timeout=5)
                except queue.Empty:
                    continue
                if tag is None:
                    break
                self.wfile.write(out(tag))
        except OSError:
            pass
        finally:
            hub.unsubscribe(v)
            self.close_connection = True

    def do_GET(self):
        p = self.path.split("?")[0]
        if p in ("/", "/index.html"):
            self.page("index.html")
        elif p == "/player.html":
            self.page("player.html")
        elif p in ("/settings", "/settings.html"):
            if self.is_local():
                self.page("settings.html")
            else:
                self.reply(403, "Settings can only be opened on the server PC.", "text/plain")
        elif p == "/live.flv":
            self.serve_flv()
        elif p == "/api/public":
            self.reply(200, {
                "title": SETTINGS["title"], "rtmp_port": SETTINGS["rtmp_port"],
                "stream_key": SETTINGS["stream_key"], "low_latency": SETTINGS["low_latency"],
                "lan_ip": LAN_IP, "local": self.is_local()}, cors=True)
        elif p == "/api/status":
            st = hub.status()
            st["error"] = rtmp.error
            self.reply(200, st, cors=True)
        elif p == "/api/settings":
            if self.is_local():
                self.reply(200, SETTINGS)
            else:
                self.reply(403, {"error": "Local only"})
        else:
            self.reply(404, "Not found", "text/plain")

    def do_POST(self):
        if self.path.split("?")[0] != "/api/settings":
            return self.reply(404, {"error": "Not found"})
        if not self.is_local():
            return self.reply(403, {"error": "Local only"})
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return self.reply(400, {"error": "Bad data"})
        s, err = validate(data)
        if err:
            return self.reply(400, {"error": err})
        if s["rtmp_port"] != rtmp.port and not rtmp.start(s["rtmp_port"]):
            return self.reply(400, {"error": rtmp.error or "Could not use that port."})
        SETTINGS.clear()
        SETTINGS.update(s)
        save_settings(s)
        self.reply(200, {"ok": True})


def main():
    SETTINGS.update(load_settings())
    save_settings(SETTINGS)

    if not rtmp.start(SETTINGS["rtmp_port"]):
        print(f"\nWARNING: {rtmp.error}")
        print("The web page still works - open Settings and pick another port.\n")

    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", WEB_PORT), Handler)
    port = SETTINGS["rtmp_port"]
    key = SETTINGS["stream_key"] or "stream"
    url = f"rtmp://{LAN_IP}{'' if port == 1935 else ':' + str(port)}/live/{key}"

    print()
    print("=" * 56)
    print(" RTMP SERVER RUNNING  (no downloads, pure Python)")
    print("=" * 56)
    print(" Stream to this URL (OBS, a camera, or any RTMP app):")
    print(f"   {url}")
    print()
    print(f" Watch    : http://localhost:{WEB_PORT}")
    print(f" Other PCs: open http://{LAN_IP}:{WEB_PORT}  (or use player.html)")
    print(f" Settings : http://localhost:{WEB_PORT}/settings")
    print(" Close this window to stop.")
    print("=" * 56)

    webbrowser.open(f"http://localhost:{WEB_PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print("\nError:", e)
        input("Press Enter to close...")
        sys.exit(1)
