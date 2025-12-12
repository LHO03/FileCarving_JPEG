#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
파일 카빙 분산 처리 시스템 - 워커 (진행률 표시 버전)
"""

import socket
import json
import struct
import tempfile
import time
import sys
from pathlib import Path

JSON_LEN_FMT = "!I"
JSON_LEN_SIZE = 4

BIN_LEN_FMT = "!Q"
BIN_LEN_SIZE = 8


def format_size(size_bytes):
    """바이트를 읽기 쉬운 형식으로 변환"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def format_speed(bytes_per_sec):
    """속도를 읽기 쉬운 형식으로 변환"""
    return f"{format_size(bytes_per_sec)}/s"


class ProgressBar:
    """진행률 표시 클래스"""
    def __init__(self, total, description="진행"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = time.time()
        self.last_print_time = 0
        self.bar_width = 30
    
    def update(self, amount):
        self.current += amount
        now = time.time()
        
        # 0.3초마다 또는 완료 시 출력
        if now - self.last_print_time >= 0.3 or self.current >= self.total:
            self.last_print_time = now
            self._print_progress()
    
    def _print_progress(self):
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            speed = self.current / elapsed
        else:
            speed = 0
        
        percent = (self.current / self.total) * 100 if self.total > 0 else 0
        
        # 프로그레스 바
        filled = int(self.bar_width * self.current / self.total) if self.total > 0 else 0
        bar = '█' * filled + '░' * (self.bar_width - filled)
        
        # 남은 시간 계산
        if speed > 0 and self.current < self.total:
            remaining = (self.total - self.current) / speed
            if remaining > 60:
                eta = f"남은 시간: {remaining/60:.1f}분"
            else:
                eta = f"남은 시간: {remaining:.0f}초"
        else:
            eta = ""
        
        print(f"\r[{self.description}] |{bar}| {percent:.1f}% "
              f"({format_size(self.current)}/{format_size(self.total)}) "
              f"{format_speed(speed)} {eta}    ", end="")
        sys.stdout.flush()
    
    def finish(self):
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            avg_speed = self.total / elapsed
        else:
            avg_speed = 0
        
        bar = '█' * self.bar_width
        print(f"\r[{self.description}] |{bar}| 100% 완료! "
              f"({format_size(self.total)}, {elapsed:.1f}초, 평균 {format_speed(avg_speed)})    ")


class FileCarvingWorker:
    def __init__(self, master_host: str, master_port: int, stream_block_size=4 * 1024 * 1024):
        self.master_host = master_host
        self.master_port = master_port
        self.stream_block_size = stream_block_size
        self.socket = None

        self.worker_id = None
        self.hostname = socket.gethostname()

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

    def recv_binary_stream_to_file_with_progress(self, out_path: Path) -> int:
        """진행률 표시와 함께 바이너리 수신"""
        size_b = self._recv_exact(BIN_LEN_SIZE)
        if not size_b:
            return -1
        total = struct.unpack(BIN_LEN_FMT, size_b)[0]

        remaining = total
        out_path.parent.mkdir(parents=True, exist_ok=True)

        progress = ProgressBar(total, "청크 수신")

        with open(out_path, "wb") as f:
            while remaining > 0:
                chunk = self.socket.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                f.write(chunk)
                remaining -= len(chunk)
                progress.update(len(chunk))
        
        progress.finish()
        return total

    def send_binary_stream_from_file_with_progress(self, file_path: Path, file_num: int, total_files: int) -> int:
        """진행률 표시와 함께 파일 전송"""
        total = file_path.stat().st_size
        self.socket.sendall(struct.pack(BIN_LEN_FMT, total))

        # 작은 파일은 진행률 생략
        show_progress = total > 512 * 1024  # 512KB 이상만 표시

        with open(file_path, "rb") as f:
            remaining = total
            sent = 0
            while remaining > 0:
                data = f.read(min(self.stream_block_size, remaining))
                if not data:
                    raise IOError("Unexpected EOF while sending file")
                self.socket.sendall(data)
                remaining -= len(data)
                sent += len(data)
                
                if show_progress:
                    percent = (sent / total) * 100
                    print(f"\r[결과 전송] 파일 {file_num+1}/{total_files}: "
                          f"{format_size(sent)}/{format_size(total)} ({percent:.0f}%)    ", end="")
                    sys.stdout.flush()
        
        if show_progress:
            print(f"\r[결과 전송] 파일 {file_num+1}/{total_files}: {format_size(total)} 완료!    ")
        
        return total

    # ----------------------------
    # JPEG carving with progress
    # ----------------------------
    def carve_jpeg_from_file_with_progress(self, chunk_path: Path, base_offset: int, 
                                           scan_block=8 * 1024 * 1024, tail_keep=2 * 1024 * 1024):
        """진행률 표시와 함께 JPEG 카빙"""
        SOI = b"\xff\xd8"
        EOI = b"\xff\xd9"

        found = []
        file_idx = 0
        
        chunk_size = chunk_path.stat().st_size
        progress = ProgressBar(chunk_size, "JPEG 카빙")

        with open(chunk_path, "rb") as f:
            file_pos = 0
            carry = b""
            carry_pos = 0

            while True:
                data = f.read(scan_block)
                if not data:
                    break

                buf = carry + data
                buf_start_pos = carry_pos
                
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
                    # EOI 찾기
                    e = buf.find(EOI, s + 2)
                    if e < 0:
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
                progress.update(len(data))

        progress.finish()
        return found

    # ----------------------------
    # Main worker flow
    # ----------------------------
    def connect(self):
        print(f"\n[워커] 마스터 서버에 연결 중: {self.master_host}:{self.master_port}")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.master_host, self.master_port))

        self.worker_id = f"worker_{self.socket.getsockname()[1]}"
        hello = {"worker_id": self.worker_id, "hostname": self.hostname, "status": "ready"}
        self.send_json(hello)
        print(f"[워커] 연결 성공! (ID: {self.worker_id})")

    def run_once(self):
        """마스터로부터 작업 수신 및 처리"""
        print("[워커] 작업 대기 중...")
        task = self.recv_json()
        if not task:
            print("[워커] 작업 수신 실패")
            return

        task_id = task["task_id"]
        read_start = int(task["read_start"])
        chunk_size = int(task["chunk_size"])

        print(f"\n[워커] 작업 수신!")
        print(f"  - Task ID: {task_id}")
        print(f"  - 청크 크기: {format_size(chunk_size)}")
        print(f"  - 오프셋: {read_start:,}")
        print()

        with tempfile.TemporaryDirectory(prefix=f"worker_{self.worker_id}_") as td:
            td = Path(td)
            chunk_path = td / f"chunk_task{task_id}.bin"

            # 1. 청크 수신 (진행률 표시)
            received = self.recv_binary_stream_to_file_with_progress(chunk_path)
            if received <= 0:
                self.send_json({"task_id": task_id, "recovered_count": 0})
                return

            # 2. 파일 카빙 (진행률 표시)
            print()
            recovered = self.carve_jpeg_from_file_with_progress(chunk_path, base_offset=read_start)

            print(f"\n[워커] 카빙 완료! {len(recovered)}개 JPEG 발견")

            # 3. 결과 전송
            self.send_json({"task_id": task_id, "recovered_count": len(recovered)})

            if recovered:
                print(f"[워커] 마스터로 결과 전송 중...")
                for i, item in enumerate(recovered):
                    meta = {"offset": int(item["offset"]), "size": int(item["size"])}
                    self.send_json(meta)
                    self.send_binary_stream_from_file_with_progress(
                        Path(item["path"]), i, len(recovered)
                    )
                print(f"[워커] 모든 결과 전송 완료!")
            
            print("\n[워커] 작업 완료!")

    def close(self):
        try:
            self.socket.close()
        except Exception:
            pass


def main():
    import argparse

    parser = argparse.ArgumentParser(description="파일 카빙 워커 (진행률 표시)")
    parser.add_argument("host", help="마스터 IP/호스트")
    parser.add_argument("port", type=int, help="마스터 포트")
    parser.add_argument("--block", "-b", type=int, default=4, help="전송 블록 크기(MB), 기본 4MB")
    args = parser.parse_args()

    print("=" * 50)
    print("  파일 카빙 워커 (진행률 표시 버전)")
    print("=" * 50)

    worker = FileCarvingWorker(args.host, args.port, stream_block_size=args.block * 1024 * 1024)
    try:
        worker.connect()
        worker.run_once()
    except ConnectionRefusedError:
        print(f"[워커] 오류: 마스터 서버에 연결할 수 없습니다 ({args.host}:{args.port})")
        print("[워커] 마스터 서버가 실행 중인지 확인하세요.")
    except Exception as e:
        print(f"[워커] 오류 발생: {e}")
    finally:
        worker.close()


if __name__ == "__main__":
    main()
