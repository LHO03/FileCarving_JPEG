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


class FileCarvingWorker:
    def __init__(self, master_host: str, master_port: int, stream_block_size=4 * 1024 * 1024):
        self.master_host = master_host
        self.master_port = master_port
        self.stream_block_size = stream_block_size
        self.socket = None

        self.worker_id = None
        self.hostname = socket.gethostname()

        # 워커 로컬 결과 저장(보내기 전 임시 저장)
        self.local_out_dir = Path("worker_recovered")
        self.local_out_dir.mkdir(exist_ok=True)

    # ----------------------------
    # Networking helpers
    # ----------------------------
    def _recv_exact(self, size: int) -> bytes:
        buf = bytearray()
        while len(buf) < size:
            chunk = self.socket.recv(min(65536, size - len(buf)))
            if not chunk:
                return b""
            buf.extend(chunk)
        return bytes(buf)

    def send_json(self, obj: dict) -> None:
        payload = json.dumps(obj).encode("utf-8")
        self.socket.sendall(struct.pack(JSON_LEN_FMT, len(payload)))
        self.socket.sendall(payload)

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

        remaining = total
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "wb") as f:
            while remaining > 0:
                chunk = self.socket.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                f.write(chunk)
                remaining -= len(chunk)
        return total

    def send_binary_stream_from_file(self, file_path: Path) -> int:
        total = file_path.stat().st_size
        self.socket.sendall(struct.pack(BIN_LEN_FMT, total))

        with open(file_path, "rb") as f:
            remaining = total
            while remaining > 0:
                data = f.read(min(self.stream_block_size, remaining))
                if not data:
                    raise IOError("Unexpected EOF while sending file")
                self.socket.sendall(data)
                remaining -= len(data)
        return total

    # ----------------------------
    # JPEG carving (file-based, streaming scan)
    # ----------------------------
    def carve_jpeg_from_file(self, chunk_path: Path, base_offset: int, scan_block=8 * 1024 * 1024, tail_keep=2 * 1024 * 1024):
        """
        chunk_path: 수신한 청크 파일
        base_offset: 이 청크의 read_start (원본 DD에서의 절대 오프셋)
        scan_block: 스캔 블록 크기
        tail_keep: 블록 경계에서 패턴 놓치지 않게 뒤에 남겨둘 바이트 수
        """
        SOI = b"\xff\xd8"
        EOI = b"\xff\xd9"

        found = []
        file_idx = 0

        with open(chunk_path, "rb") as f:
            file_pos = 0
            carry = b""
            carry_pos = 0  # carry가 시작하는 청크 내 위치

            while True:
                data = f.read(scan_block)
                if not data:
                    break

                buf = carry + data
                buf_start_pos = carry_pos  # buf[0]이 해당하는 청크 내 위치
                # 다음 carry 준비: buf 끝에서 tail_keep만 남김
                if len(buf) > tail_keep:
                    next_carry = buf[-tail_keep:]
                    next_carry_pos = buf_start_pos + (len(buf) - tail_keep)
                else:
                    next_carry = buf
                    next_carry_pos = buf_start_pos

                # SOI 찾기
                idx = 0
                while True:
                    s = buf.find(SOI, idx)
                    if s < 0:
                        break
                    # EOI 찾기(해당 SOI 이후)
                    e = buf.find(EOI, s + 2)
                    if e < 0:
                        # EOI가 현재 buf에 없으면 다음 블록에서 이어서(간단화: 현재는 스킵)
                        # 완전한 복구율을 높이려면 "진행 중 이미지" 상태를 유지해야 하지만,
                        # 과제/실습 수준에서는 일반적으로 이 정도면 충분함.
                        idx = s + 2
                        continue

                    jpg_bytes = buf[s : e + 2]
                    abs_offset = base_offset + (buf_start_pos + s)

                    out_name = self.local_out_dir / f"worker_{self.worker_id}_off{abs_offset}_idx{file_idx}.jpg"
                    with open(out_name, "wb") as out:
                        out.write(jpg_bytes)

                    found.append({"offset": abs_offset, "path": out_name, "size": out_name.stat().st_size})
                    file_idx += 1

                    idx = e + 2

                carry = next_carry
                carry_pos = next_carry_pos
                file_pos += len(data)

        return found

    # ----------------------------
    # Main worker flow
    # ----------------------------
    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.master_host, self.master_port))

        self.worker_id = f"worker_{self.socket.getsockname()[1]}"
        hello = {"worker_id": self.worker_id, "hostname": self.hostname, "status": "ready"}
        self.send_json(hello)

    def run_once(self):
        """
        마스터가 1회 task 보내는 구조를 그대로 유지(연결 1번당 task 1개).
        """
        task = self.recv_json()
        if not task:
            return

        task_id = task["task_id"]
        read_start = int(task["read_start"])

        # 청크를 메모리에 올리지 않고 temp 파일로 수신
        with tempfile.TemporaryDirectory(prefix=f"worker_{self.worker_id}_") as td:
            td = Path(td)
            chunk_path = td / f"chunk_task{task_id}.bin"

            received = self.recv_binary_stream_to_file(chunk_path)
            if received <= 0:
                self.send_json({"task_id": task_id, "recovered_count": 0})
                return

            # 파일 기반 카빙
            recovered = self.carve_jpeg_from_file(chunk_path, base_offset=read_start)

            # 결과 전송
            self.send_json({"task_id": task_id, "recovered_count": len(recovered)})

            for item in recovered:
                meta = {"offset": int(item["offset"]), "size": int(item["size"])}
                self.send_json(meta)
                # 복구 파일도 스트리밍 전송
                self.send_binary_stream_from_file(Path(item["path"]))

    def close(self):
        try:
            self.socket.close()
        except Exception:
            pass


def main():
    import argparse

    parser = argparse.ArgumentParser(description="파일 카빙 워커 (스트리밍/8바이트 길이)")
    parser.add_argument("host", help="마스터 IP/호스트")
    parser.add_argument("port", type=int, help="마스터 포트")
    parser.add_argument("--block", "-b", type=int, default=4, help="전송 블록 크기(MB), 기본 4MB")
    args = parser.parse_args()

    worker = FileCarvingWorker(args.host, args.port, stream_block_size=args.block * 1024 * 1024)
    try:
        worker.connect()
        worker.run_once()
    finally:
        worker.close()


if __name__ == "__main__":
    main()
