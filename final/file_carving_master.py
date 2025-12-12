#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import json
import struct
import hashlib
import threading
from pathlib import Path

# JSON length: 4 bytes (충분)
JSON_LEN_FMT = "!I"
JSON_LEN_SIZE = 4

# Binary length: 8 bytes (30GB 같은 청크도 가능)
BIN_LEN_FMT = "!Q"
BIN_LEN_SIZE = 8


class FileCarvingMaster:
    def __init__(self, port=5000, overlap_size=1 * 1024 * 1024, stream_block_size=4 * 1024 * 1024):
        self.port = port
        self.overlap_size = overlap_size
        self.stream_block_size = stream_block_size

        self.workers = []
        self.dd_image_path = None
        self.image_size = 0

        self.results_dir = Path("recovered_files")
        self.results_dir.mkdir(exist_ok=True)

        self.file_hashes = set()
        self.lock = threading.Lock()
        self.recovered_files = []

    def load_dd_image(self, image_path: str) -> bool:
        p = Path(image_path)
        if not p.exists():
            print(f"[마스터] 오류: 이미지 파일을 찾을 수 없음: {image_path}")
            return False

        self.dd_image_path = p
        self.image_size = p.stat().st_size

        print(f"[마스터] DD 이미지 로드: {p}")
        print(f"[마스터] 이미지 크기: {self.image_size:,} bytes ({self.image_size / 1024 / 1024:.2f} MB)")
        return True

    # ----------------------------
    # Networking helpers
    # ----------------------------
    def _recv_exact(self, sock: socket.socket, size: int) -> bytes:
        buf = bytearray()
        while len(buf) < size:
            chunk = sock.recv(min(65536, size - len(buf)))
            if not chunk:
                return b""
            buf.extend(chunk)
        return bytes(buf)

    def send_json(self, sock: socket.socket, obj: dict) -> None:
        payload = json.dumps(obj).encode("utf-8")
        sock.sendall(struct.pack(JSON_LEN_FMT, len(payload)))
        sock.sendall(payload)

    def recv_json(self, sock: socket.socket):
        size_b = self._recv_exact(sock, JSON_LEN_SIZE)
        if not size_b:
            return None
        size = struct.unpack(JSON_LEN_FMT, size_b)[0]
        payload = self._recv_exact(sock, size)
        if not payload:
            return None
        return json.loads(payload.decode("utf-8"))

    def send_binary_stream_from_file(self, sock: socket.socket, file_obj, start: int, end: int) -> None:
        total = end - start
        if total < 0:
            raise ValueError("Invalid range")

        # 8-byte length
        sock.sendall(struct.pack(BIN_LEN_FMT, total))

        file_obj.seek(start)
        remaining = total
        while remaining > 0:
            to_read = min(self.stream_block_size, remaining)
            chunk = file_obj.read(to_read)
            if not chunk:
                raise IOError("Unexpected EOF while reading DD image")
            sock.sendall(chunk)
            remaining -= len(chunk)

    def recv_binary_stream_to_file(self, sock: socket.socket, out_path: Path) -> int:
        size_b = self._recv_exact(sock, BIN_LEN_SIZE)
        if not size_b:
            return -1
        total = struct.unpack(BIN_LEN_FMT, size_b)[0]

        remaining = total
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "wb") as f:
            while remaining > 0:
                chunk = sock.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                f.write(chunk)
                remaining -= len(chunk)

        return total

    # ----------------------------
    # Main server flow
    # ----------------------------
    def start_server(self) -> None:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", self.port))
        server_socket.listen(10)

        print(f"\n[마스터] 서버 시작 - 포트: {self.port}")
        print(f"[마스터] 다른 PC에서 연결하려면: python file_carving_worker.py <이 PC의 IP> {self.port}")
        print("[마스터] 워커 연결 대기 중... (30초 타임아웃)")
        server_socket.settimeout(30)

        try:
            while True:
                try:
                    client_socket, addr = server_socket.accept()
                    print(f"[마스터] 워커 연결됨: {addr}")
                    info = self.recv_json(client_socket)
                    print(f"[마스터] 워커 정보: {info}")

                    self.workers.append({"socket": client_socket, "address": addr, "info": info})
                except socket.timeout:
                    if self.workers:
                        print(f"\n[마스터] 총 {len(self.workers)}개 워커 연결 완료\n")
                        break
                    else:
                        print("[마스터] 워커 연결 없음. 종료합니다.")
                        return
        finally:
            server_socket.close()

        self.distribute_and_collect()
        self.print_summary()

    def distribute_and_collect(self) -> None:
        if not self.workers or not self.dd_image_path:
            return

        n = len(self.workers)
        base = self.image_size // n

        print("[마스터] 작업 분배 시작")
        print(f"  - 전체 크기: {self.image_size:,} bytes")
        print(f"  - 워커 수: {n}")
        print(f"  - 청크 크기: ~{base:,} bytes")
        print(f"  - 오버랩: {self.overlap_size:,} bytes")
        print(f"  - 전송: 스트리밍 / 길이필드: 8바이트(!Q)\n")

        with open(self.dd_image_path, "rb") as f:
            for i, w in enumerate(self.workers):
                sock = w["socket"]

                start_offset = i * base
                end_offset = (i + 1) * base if i < n - 1 else self.image_size

                read_start = 0 if i == 0 else max(0, start_offset - self.overlap_size // 2)
                read_end = self.image_size if i == n - 1 else min(self.image_size, end_offset + self.overlap_size // 2)
                chunk_size = read_end - read_start

                print(f"[마스터] 워커 {i} ({w['address'][0]})")
                print(f"  - 담당: {start_offset:,} ~ {end_offset:,}")
                print(f"  - 전송: {read_start:,} ~ {read_end:,} ({chunk_size:,} bytes)")

                task = {
                    "task_id": i,
                    "start_offset": start_offset,
                    "end_offset": end_offset,
                    "read_start": read_start,
                    "read_end": read_end,
                    "chunk_size": chunk_size,
                    "overlap_size": self.overlap_size,
                }

                try:
                    # 1) task info
                    self.send_json(sock, task)
                    # 2) chunk stream
                    self.send_binary_stream_from_file(sock, f, read_start, read_end)
                    print("  - 청크 전송 완료")

                    # 3) results
                    self.receive_results(sock, worker_id=i)

                except Exception as e:
                    print(f"  - 오류: {e}")
                finally:
                    try:
                        sock.close()
                    except Exception:
                        pass

                print("")

    def receive_results(self, sock: socket.socket, worker_id: int) -> None:
        result = self.recv_json(sock)
        if not result:
            print("  - 결과 수신 실패")
            return

        recovered_count = int(result.get("recovered_count", 0))
        print(f"  - 발견된 파일: {recovered_count}개")

        for _ in range(recovered_count):
            meta = self.recv_json(sock)
            if not meta:
                break

            offset = int(meta.get("offset", -1))
            size = int(meta.get("size", 0))

            # 워커가 이어서 바이너리 스트림(8바이트 길이 + 데이터)을 보냄
            tmp_path = self.results_dir / f"__tmp_worker{worker_id}_off{offset}.jpg"
            received = self.recv_binary_stream_to_file(sock, tmp_path)

            if received <= 0:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            # 중복 제거(해시)
            md5 = hashlib.md5()
            with open(tmp_path, "rb") as rf:
                for chunk in iter(lambda: rf.read(1024 * 1024), b""):
                    md5.update(chunk)
            file_hash = md5.hexdigest()

            with self.lock:
                if file_hash in self.file_hashes:
                    print(f"    - 중복 스킵: offset {offset}")
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue

                self.file_hashes.add(file_hash)

                final_name = f"recovered_{offset}_{file_hash[:8]}.jpg"
                final_path = self.results_dir / final_name
                tmp_path.replace(final_path)

                self.recovered_files.append(
                    {
                        "filename": final_name,
                        "size": received,
                        "offset": offset,
                        "hash": file_hash,
                        "worker_id": worker_id,
                    }
                )
                print(f"    + 저장: {final_name} ({received:,} bytes)")

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("파일 카빙 완료 - 결과 요약")
        print("=" * 60)
        print(f"총 복구 파일: {len(self.recovered_files)}개 (중복 제거됨)")
        if not self.recovered_files:
            return

        total = sum(x["size"] for x in self.recovered_files)
        print(f"총 복구 크기: {total:,} bytes ({total / 1024 / 1024:.2f} MB)")
        print(f"저장 위치: {self.results_dir.resolve()}")

        print("\n복구된 파일 목록:")
        for f in self.recovered_files:
            print(f"  - {f['filename']} ({f['size']:,} bytes, 워커 {f['worker_id']})")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="파일 카빙 마스터 서버 (스트리밍/8바이트 길이)")
    parser.add_argument("image", help="DD 이미지 파일 경로")
    parser.add_argument("--port", "-p", type=int, default=5000)
    parser.add_argument("--overlap", "-o", type=int, default=1, help="오버랩 크기(MB), 기본 1MB")
    parser.add_argument("--block", "-b", type=int, default=4, help="스트리밍 블록 크기(MB), 기본 4MB")
    parser.add_argument("--output", "-O", type=str, default="recovered_files")
    args = parser.parse_args()

    master = FileCarvingMaster(
        port=args.port,
        overlap_size=args.overlap * 1024 * 1024,
        stream_block_size=args.block * 1024 * 1024,
    )
    master.results_dir = Path(args.output)
    master.results_dir.mkdir(exist_ok=True)

    if master.load_dd_image(args.image):
        master.start_server()


if __name__ == "__main__":
    main()
