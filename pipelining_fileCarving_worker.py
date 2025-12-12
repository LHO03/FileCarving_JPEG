#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import json
import struct
import tempfile
from pathlib import Path

JSON_LEN_FMT = "!I"
JSON_LEN_SIZE = 4

BIN_LEN_FMT = "!Q"
BIN_LEN_SIZE = 8


class FileCarvingWorkerParallel:
    def __init__(self, host: str, port: int, stream_block_size=4 * 1024 * 1024):
        self.host = host
        self.port = port
        self.stream_block_size = stream_block_size
        self.sock = None

        self.worker_id = None
        self.hostname = socket.gethostname()

        self.local_out_dir = Path("worker_recovered")
        self.local_out_dir.mkdir(exist_ok=True)

    # ---------------------------
    # Networking helpers
    # ---------------------------
    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(min(65536, n - len(buf)))
            if not chunk:
                return b""
            buf.extend(chunk)
        return bytes(buf)

    def send_json(self, obj: dict) -> None:
        payload = json.dumps(obj).encode("utf-8")
        self.sock.sendall(struct.pack(JSON_LEN_FMT, len(payload)))
        self.sock.sendall(payload)

    def recv_json(self):
        size_b = self._recv_exact(JSON_LEN_SIZE)
        if not size_b:
            return None
        size = struct.unpack(JSON_LEN_FMT, size_b)[0]
        payload = self._recv_exact(size)
        if not payload:
            return None
        return json.loads(payload.decode("utf-8"))

    def recv_binary_stream_to_file(self, out_path: Path) -> int:
        size_b = self._recv_exact(BIN_LEN_SIZE)
        if not size_b:
            return -1
        total = struct.unpack(BIN_LEN_FMT, size_b)[0]

        out_path.parent.mkdir(parents=True, exist_ok=True)

        remaining = total
        with open(out_path, "wb") as f:
            while remaining > 0:
                chunk = self.sock.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                f.write(chunk)
                remaining -= len(chunk)
        return total

    def send_binary_stream_from_file(self, file_path: Path) -> int:
        total = file_path.stat().st_size
        self.sock.sendall(struct.pack(BIN_LEN_FMT, total))

        remaining = total
        with open(file_path, "rb") as f:
            while remaining > 0:
                data = f.read(min(self.stream_block_size, remaining))
                if not data:
                    raise IOError("Unexpected EOF while sending file")
                self.sock.sendall(data)
                remaining -= len(data)
        return total

    # ---------------------------
    # JPEG carve (file-based scan)
    # ---------------------------
    def carve_jpeg_from_file(self, chunk_path: Path, base_offset: int,
                             scan_block=8 * 1024 * 1024, tail_keep=2 * 1024 * 1024):
        SOI = b"\xff\xd8"
        EOI = b"\xff\xd9"

        found = []
        file_idx = 0

        with open(chunk_path, "rb") as f:
            carry = b""
            carry_pos = 0

            while True:
                data = f.read(scan_block)
                if not data:
                    break

                buf = carry + data
                buf_start_pos = carry_pos

                # 다음 carry 준비
                if len(buf) > tail_keep:
                    next_carry = buf[-tail_keep:]
                    next_carry_pos = buf_start_pos + (len(buf) - tail_keep)
                else:
                    next_carry = buf
                    next_carry_pos = buf_start_pos

                idx = 0
                while True:
                    s = buf.find(SOI, idx)
                    if s < 0:
                        break
                    e = buf.find(EOI, s + 2)
                    if e < 0:
                        idx = s + 2
                        continue

                    jpg = buf[s:e + 2]
                    abs_off = base_offset + (buf_start_pos + s)

                    out_path = self.local_out_dir / f"{self.worker_id}_off{abs_off}_idx{file_idx}.jpg"
                    with open(out_path, "wb") as out:
                        out.write(jpg)

                    found.append({"offset": abs_off, "path": out_path, "size": out_path.stat().st_size})
                    file_idx += 1
                    idx = e + 2

                carry = next_carry
                carry_pos = next_carry_pos

        return found

    # ---------------------------
    # Main flow
    # ---------------------------
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        self.worker_id = f"worker_{self.sock.getsockname()[1]}"
        self.send_json({"worker_id": self.worker_id, "hostname": self.hostname, "status": "ready"})

    def run(self):
        while True:
            # 다음 task 요청
            self.send_json({"type": "request_task"})

            msg = self.recv_json()
            if msg is None:
                print("[워커] 서버 연결 종료")
                return

            if msg.get("type") == "stop":
                print("[워커] 작업 종료 지시(stop)")
                return

            if msg.get("type") != "task":
                print(f"[워커] unexpected msg: {msg}")
                return

            task_id = int(msg["task_id"])
            read_start = int(msg["read_start"])
            chunk_size = int(msg["chunk_size"])

            print(f"[워커] task {task_id} 수신 시작 (chunk={chunk_size:,} bytes)")

            # 청크를 temp 파일로 저장 (RAM 폭발 방지)
            with tempfile.TemporaryDirectory(prefix=f"{self.worker_id}_t{task_id}_") as td:
                td = Path(td)
                chunk_path = td / "chunk.bin"
                self.recv_binary_stream_to_file(chunk_path)

                # 카빙
                recovered = self.carve_jpeg_from_file(chunk_path, base_offset=read_start)

                # 결과 헤더 전송
                self.send_json({"type": "result", "task_id": task_id, "recovered_count": len(recovered)})

                # 파일 전송
                for item in recovered:
                    self.send_json({"type": "file", "offset": int(item["offset"]), "size": int(item["size"])})
                    self.send_binary_stream_from_file(Path(item["path"]))

            print(f"[워커] task {task_id} 완료 (복구 {len(recovered)}개)")

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description="병렬/동적분배 JPEG carving worker")
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--stream-block-mb", type=int, default=4)
    args = parser.parse_args()

    w = FileCarvingWorkerParallel(args.host, args.port, stream_block_size=args.stream_block_mb * 1024 * 1024)
    try:
        w.connect()
        w.run()
    finally:
        w.close()


if __name__ == "__main__":
    main()
