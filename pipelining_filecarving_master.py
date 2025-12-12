#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import json
import struct
import threading
import hashlib
from pathlib import Path
from collections import deque

JSON_LEN_FMT = "!I"
JSON_LEN_SIZE = 4

BIN_LEN_FMT = "!Q"   # 8-byte length (supports > 4GB)
BIN_LEN_SIZE = 8


class TaskQueue:
    def __init__(self):
        self.q = deque()
        self.lock = threading.Lock()

    def push(self, item):
        with self.lock:
            self.q.append(item)

    def pop(self):
        with self.lock:
            if not self.q:
                return None
            return self.q.popleft()

    def __len__(self):
        with self.lock:
            return len(self.q)


class FileCarvingMasterParallel:
    def __init__(self, port=5000, overlap_size=1 * 1024 * 1024,
                 stream_block_size=4 * 1024 * 1024, task_chunk_size=512 * 1024 * 1024):
        self.port = port
        self.overlap_size = overlap_size
        self.stream_block_size = stream_block_size
        self.task_chunk_size = task_chunk_size

        self.dd_image_path = None
        self.image_size = 0

        self.results_dir = Path("recovered_files")
        self.results_dir.mkdir(exist_ok=True)

        self.file_hashes = set()
        self.hash_lock = threading.Lock()

        self.tasks = TaskQueue()
        self.workers = []
        self.workers_lock = threading.Lock()

        self.recovered_files = []
        self.recovered_lock = threading.Lock()

        self.stop_event = threading.Event()

    # ---------------------------
    # IO / Networking helpers
    # ---------------------------
    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(min(65536, n - len(buf)))
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

        sock.sendall(struct.pack(BIN_LEN_FMT, total))

        file_obj.seek(start)
        remaining = total
        while remaining > 0:
            to_read = min(self.stream_block_size, remaining)
            data = file_obj.read(to_read)
            if not data:
                raise IOError("Unexpected EOF while reading image")
            sock.sendall(data)
            remaining -= len(data)

    def recv_binary_stream_to_file(self, sock: socket.socket, out_path: Path) -> int:
        size_b = self._recv_exact(sock, BIN_LEN_SIZE)
        if not size_b:
            return -1
        total = struct.unpack(BIN_LEN_FMT, size_b)[0]

        out_path.parent.mkdir(parents=True, exist_ok=True)

        remaining = total
        with open(out_path, "wb") as f:
            while remaining > 0:
                chunk = sock.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                f.write(chunk)
                remaining -= len(chunk)

        return total

    # ---------------------------
    # Master core
    # ---------------------------
    def load_dd_image(self, image_path: str) -> bool:
        p = Path(image_path)
        if not p.exists():
            print(f"[마스터] 오류: 이미지 파일을 찾을 수 없음: {image_path}")
            return False
        self.dd_image_path = p
        self.image_size = p.stat().st_size
        print(f"[마스터] DD 이미지 로드: {p}")
        print(f"[마스터] 이미지 크기: {self.image_size:,} bytes ({self.image_size/1024/1024:.2f} MB)")
        return True

    def build_tasks(self):
        # 이미지를 task_chunk_size 단위로 쪼개서 task queue에 넣음
        n = 0
        start = 0
        while start < self.image_size:
            end = min(self.image_size, start + self.task_chunk_size)

            # 오버랩 적용
            read_start = 0 if start == 0 else max(0, start - self.overlap_size // 2)
            read_end = self.image_size if end == self.image_size else min(self.image_size, end + self.overlap_size // 2)

            task = {
                "task_id": n,
                "start_offset": start,
                "end_offset": end,
                "read_start": read_start,
                "read_end": read_end,
                "chunk_size": read_end - read_start,
            }
            self.tasks.push(task)
            n += 1
            start = end

        print(f"[마스터] 작업 생성 완료: {n}개 (task_chunk_size={self.task_chunk_size:,} bytes, overlap={self.overlap_size:,})")

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", self.port))
        server_socket.listen(50)

        print(f"\n[마스터] 서버 시작 - 포트: {self.port}")
        print(f"[마스터] 워커 연결 대기 (30초 타임아웃)")

        server_socket.settimeout(30)

        try:
            while True:
                try:
                    client, addr = server_socket.accept()
                    info = self.recv_json(client)
                    print(f"[마스터] 워커 연결: {addr}, info={info}")
                    with self.workers_lock:
                        self.workers.append((client, addr, info))
                except socket.timeout:
                    break
        finally:
            server_socket.close()

        if not self.workers:
            print("[마스터] 워커가 연결되지 않아 종료합니다.")
            return

        print(f"[마스터] 워커 {len(self.workers)}개 연결 완료\n")

        # 병렬 처리: 워커마다 전용 스레드
        threads = []
        for idx, (sock, addr, info) in enumerate(self.workers):
            t = threading.Thread(target=self.worker_session, args=(idx, sock, addr, info), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        self.print_summary()

    def worker_session(self, worker_index: int, sock: socket.socket, addr, info):
        """
        워커 1개와 세션을 유지하면서:
          - 워커가 "request_task" 보내면,
          - task queue에서 pop 해서 task+데이터 스트리밍 전송
          - 결과 수신/저장
          - task 없으면 stop 전송
        """
        try:
            # Keep-alive (절전/네트워크 끊김 완화에 도움)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            with open(self.dd_image_path, "rb") as f:
                while True:
                    msg = self.recv_json(sock)
                    if msg is None:
                        print(f"[마스터][W{worker_index}] 연결 종료(수신 실패)")
                        return

                    if msg.get("type") != "request_task":
                        # 예기치 않은 메시지는 무시/종료
                        print(f"[마스터][W{worker_index}] unexpected msg: {msg}")
                        return

                    task = self.tasks.pop()
                    if task is None:
                        self.send_json(sock, {"type": "stop"})
                        print(f"[마스터][W{worker_index}] 더 이상 task 없음 → stop")
                        return

                    # task 전송
                    self.send_json(sock, {"type": "task", **task})
                    # chunk 데이터 스트리밍 전송
                    self.send_binary_stream_from_file(sock, f, task["read_start"], task["read_end"])

                    # 결과 수신
                    self.receive_results(sock, worker_index, task["task_id"])

        except Exception as e:
            print(f"[마스터][W{worker_index}] 오류: {e}")
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def receive_results(self, sock: socket.socket, worker_index: int, task_id: int):
        """
        워커가:
          - {"type":"result","task_id":...,"recovered_count":N}
          - N번 반복: meta(json) + file(binary stream)
        을 보낸다고 가정
        """
        header = self.recv_json(sock)
        if not header or header.get("type") != "result":
            print(f"[마스터][W{worker_index}] 결과 헤더 오류: {header}")
            return

        n = int(header.get("recovered_count", 0))
        print(f"[마스터][W{worker_index}] task {task_id} 결과: {n}개")

        for _ in range(n):
            meta = self.recv_json(sock)
            if not meta or meta.get("type") != "file":
                break

            offset = int(meta["offset"])
            # 워커가 이어서 파일 바이너리 스트림 전송
            tmp_path = self.results_dir / f"__tmp_w{worker_index}_t{task_id}_off{offset}.jpg"
            size = self.recv_binary_stream_to_file(sock, tmp_path)
            if size <= 0:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            # 중복 제거 (md5)
            md5 = hashlib.md5()
            with open(tmp_path, "rb") as rf:
                for chunk in iter(lambda: rf.read(1024 * 1024), b""):
                    md5.update(chunk)
            h = md5.hexdigest()

            with self.hash_lock:
                if h in self.file_hashes:
                    print(f"  - 중복 스킵: off={offset}")
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue
                self.file_hashes.add(h)

            final_name = f"recovered_{offset}_{h[:8]}.jpg"
            final_path = self.results_dir / final_name
            tmp_path.replace(final_path)

            with self.recovered_lock:
                self.recovered_files.append({
                    "filename": final_name,
                    "size": size,
                    "offset": offset,
                    "hash": h,
                    "worker": worker_index,
                    "task": task_id
                })

            print(f"  + 저장: {final_name} ({size:,} bytes)")

    def print_summary(self):
        print("\n" + "=" * 70)
        print("완료 요약")
        print("=" * 70)
        print(f"복구 파일 수: {len(self.recovered_files)}개 (중복 제거)")
        if not self.recovered_files:
            return
        total = sum(x["size"] for x in self.recovered_files)
        print(f"총 크기: {total:,} bytes ({total/1024/1024:.2f} MB)")
        print(f"저장 위치: {self.results_dir.resolve()}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="병렬/동적분배 JPEG carving master")
    parser.add_argument("image", help="DD 이미지 경로")
    parser.add_argument("--port", "-p", type=int, default=5000)
    parser.add_argument("--overlap-mb", type=int, default=1)
    parser.add_argument("--stream-block-mb", type=int, default=4)
    parser.add_argument("--task-chunk-mb", type=int, default=512, help="task 단위 청크 크기(MB)")
    parser.add_argument("--output", "-O", type=str, default="recovered_files")
    args = parser.parse_args()

    master = FileCarvingMasterParallel(
        port=args.port,
        overlap_size=args.overlap_mb * 1024 * 1024,
        stream_block_size=args.stream_block_mb * 1024 * 1024,
        task_chunk_size=args.task_chunk_mb * 1024 * 1024
    )
    master.results_dir = Path(args.output)
    master.results_dir.mkdir(exist_ok=True)

    if master.load_dd_image(args.image):
        master.build_tasks()
        master.start_server()


if __name__ == "__main__":
    main()
