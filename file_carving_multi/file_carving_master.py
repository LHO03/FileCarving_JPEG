#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
파일 카빙 분산 처리 시스템 - 마스터 서버 (병렬 처리 + 멀티라인 진행률)
"""

import socket
import json
import struct
import hashlib
import threading
import time
import sys
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Windows 콘솔에서 ANSI escape code 활성화
if sys.platform == 'win32':
    os.system('')

# JSON length: 4 bytes
JSON_LEN_FMT = "!I"
JSON_LEN_SIZE = 4

# Binary length: 8 bytes
BIN_LEN_FMT = "!Q"
BIN_LEN_SIZE = 8


def format_size(size_bytes):
    """바이트를 읽기 쉬운 형식으로 변환"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f}PB"


def format_speed(bytes_per_sec):
    """속도를 읽기 쉬운 형식으로 변환"""
    return f"{format_size(bytes_per_sec)}/s"


def format_time(seconds):
    """시간을 읽기 쉬운 형식으로 변환"""
    if seconds < 60:
        return f"{seconds:.0f}초"
    elif seconds < 3600:
        return f"{seconds/60:.1f}분"
    else:
        return f"{seconds/3600:.1f}시간"


class MultiProgressDisplay:
    """여러 워커의 진행률을 동시에 표시하는 클래스"""
    
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.lock = threading.Lock()
        self.worker_states = {}
        self.displayed = False
        self.bar_width = 25
        
        # 각 워커 상태 초기화
        for i in range(num_workers):
            self.worker_states[i] = {
                'phase': 'waiting',  # waiting, sending, carving, receiving, done
                'current': 0,
                'total': 0,
                'start_time': None,
                'address': '',
                'message': '대기 중...'
            }
    
    def set_worker_info(self, worker_id, address, total):
        """워커 정보 설정"""
        with self.lock:
            self.worker_states[worker_id]['address'] = address
            self.worker_states[worker_id]['total'] = total
    
    def update(self, worker_id, current, phase='sending', message=None):
        """워커 진행률 업데이트"""
        with self.lock:
            state = self.worker_states[worker_id]
            state['current'] = current
            state['phase'] = phase
            
            if state['start_time'] is None and phase == 'sending':
                state['start_time'] = time.time()
            
            if message:
                state['message'] = message
            
            self._render()
    
    def set_phase(self, worker_id, phase, message=None):
        """워커 단계 변경"""
        with self.lock:
            self.worker_states[worker_id]['phase'] = phase
            if message:
                self.worker_states[worker_id]['message'] = message
            self._render()
    
    def _render(self):
        """전체 진행률 화면 렌더링"""
        # 이전 출력 지우기 (커서를 위로 이동)
        if self.displayed:
            # 워커 수 + 구분선 1줄만큼 위로 이동
            sys.stdout.write(f"\033[{self.num_workers + 1}A")
        
        # 구분선
        print("─" * 75)
        
        # 각 워커 상태 출력
        for i in range(self.num_workers):
            state = self.worker_states[i]
            line = self._format_worker_line(i, state)
            # 줄 지우고 출력
            sys.stdout.write(f"\033[K{line}\n")
        
        sys.stdout.flush()
        self.displayed = True
    
    def _format_worker_line(self, worker_id, state):
        """워커 한 줄 포맷팅"""
        phase = state['phase']
        addr = state['address'][:15].ljust(15) if state['address'] else '???'.ljust(15)
        
        if phase == 'waiting':
            return f"[워커 {worker_id}] {addr} │ ⏳ 대기 중..."
        
        elif phase == 'sending':
            current = state['current']
            total = state['total']
            percent = (current / total * 100) if total > 0 else 0
            
            # 프로그레스 바
            filled = int(self.bar_width * current / total) if total > 0 else 0
            bar = '█' * filled + '░' * (self.bar_width - filled)
            
            # 속도 계산
            elapsed = time.time() - state['start_time'] if state['start_time'] else 0
            speed = current / elapsed if elapsed > 0 else 0
            
            # 남은 시간
            if speed > 0 and current < total:
                eta = format_time((total - current) / speed)
            else:
                eta = "--"
            
            return (f"[워커 {worker_id}] {addr} │ "
                   f"|{bar}| {percent:5.1f}% │ "
                   f"{format_size(current):>9}/{format_size(total):>9} │ "
                   f"{format_speed(speed):>12} │ 남은: {eta}")
        
        elif phase == 'carving':
            return f"[워커 {worker_id}] {addr} │ 🔍 카빙 진행 중... {state['message']}"
        
        elif phase == 'receiving':
            return f"[워커 {worker_id}] {addr} │ 📥 결과 수신 중... {state['message']}"
        
        elif phase == 'done':
            return f"[워커 {worker_id}] {addr} │ ✅ 완료! {state['message']}"
        
        elif phase == 'error':
            return f"[워커 {worker_id}] {addr} │ ❌ 오류: {state['message']}"
        
        else:
            return f"[워커 {worker_id}] {addr} │ {state['message']}"
    
    def finish(self):
        """진행률 표시 종료"""
        with self.lock:
            if self.displayed:
                print()  # 마지막 줄바꿈


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
        
        self.progress_display = None

    def load_dd_image(self, image_path: str) -> bool:
        p = Path(image_path)
        if not p.exists():
            print(f"[마스터] 오류: 이미지 파일을 찾을 수 없음: {image_path}")
            return False

        self.dd_image_path = p
        self.image_size = p.stat().st_size

        print(f"[마스터] DD 이미지 로드: {p}")
        print(f"[마스터] 이미지 크기: {self.image_size:,} bytes ({format_size(self.image_size)})")
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

    def send_binary_stream_with_progress(self, sock: socket.socket, file_path: Path, 
                                         start: int, end: int, worker_id: int) -> None:
        """진행률 표시와 함께 파일 스트리밍 전송"""
        total = end - start
        if total < 0:
            raise ValueError("Invalid range")

        # 8-byte length
        sock.sendall(struct.pack(BIN_LEN_FMT, total))

        with open(file_path, "rb") as f:
            f.seek(start)
            remaining = total
            sent = 0
            last_update = 0
            
            while remaining > 0:
                to_read = min(self.stream_block_size, remaining)
                chunk = f.read(to_read)
                if not chunk:
                    raise IOError("Unexpected EOF while reading DD image")
                sock.sendall(chunk)
                remaining -= len(chunk)
                sent += len(chunk)
                
                # 진행률 업데이트 (너무 자주 하지 않도록)
                now = time.time()
                if now - last_update >= 0.2 or remaining == 0:
                    self.progress_display.update(worker_id, sent, 'sending')
                    last_update = now

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

        self.distribute_and_collect_parallel()
        self.print_summary()

    def process_worker(self, worker_id: int, worker: dict, task: dict, read_start: int, read_end: int) -> dict:
        """개별 워커 처리 (별도 스레드에서 실행)"""
        sock = worker["socket"]
        addr = worker["address"][0]
        chunk_size = read_end - read_start
        
        result_info = {
            "worker_id": worker_id,
            "address": addr,
            "success": False,
            "recovered_count": 0,
            "error": None
        }

        try:
            # 워커 정보 설정
            self.progress_display.set_worker_info(worker_id, addr, chunk_size)
            
            # 1) task info 전송
            self.send_json(sock, task)
            
            # 2) chunk stream 전송 (진행률 표시)
            self.send_binary_stream_with_progress(
                sock, self.dd_image_path, read_start, read_end, worker_id
            )
            
            # 3) 카빙 대기
            self.progress_display.set_phase(worker_id, 'carving', '')

            # 4) 결과 수신
            recovered_count = self.receive_results(sock, worker_id)
            
            result_info["success"] = True
            result_info["recovered_count"] = recovered_count
            self.progress_display.set_phase(worker_id, 'done', f'{recovered_count}개 파일 복구')

        except Exception as e:
            result_info["error"] = str(e)
            self.progress_display.set_phase(worker_id, 'error', str(e)[:30])
        finally:
            try:
                sock.close()
            except Exception:
                pass

        return result_info

    def distribute_and_collect_parallel(self) -> None:
        """병렬로 모든 워커에게 작업 분배 및 결과 수집"""
        if not self.workers or not self.dd_image_path:
            return

        n = len(self.workers)
        base = self.image_size // n

        print("[마스터] 병렬 작업 분배 시작")
        print(f"  - 전체 크기: {format_size(self.image_size)}")
        print(f"  - 워커 수: {n}")
        print(f"  - 청크 크기: ~{format_size(base)}")
        print(f"  - 오버랩: {format_size(self.overlap_size)}")
        print()

        # 진행률 디스플레이 초기화
        self.progress_display = MultiProgressDisplay(n)

        # 각 워커별 작업 정보 준비
        tasks_args = []
        for i, w in enumerate(self.workers):
            start_offset = i * base
            end_offset = (i + 1) * base if i < n - 1 else self.image_size

            read_start = 0 if i == 0 else max(0, start_offset - self.overlap_size // 2)
            read_end = self.image_size if i == n - 1 else min(self.image_size, end_offset + self.overlap_size // 2)
            chunk_size = read_end - read_start

            task = {
                "task_id": i,
                "start_offset": start_offset,
                "end_offset": end_offset,
                "read_start": read_start,
                "read_end": read_end,
                "chunk_size": chunk_size,
                "overlap_size": self.overlap_size,
            }
            tasks_args.append((i, w, task, read_start, read_end))

        # 병렬 실행
        start_time = time.time()
        print("=" * 75)
        print("[마스터] 모든 워커에게 동시 전송 시작!")
        print("=" * 75)

        with ThreadPoolExecutor(max_workers=n) as executor:
            futures = {
                executor.submit(self.process_worker, *args): args[0]
                for args in tasks_args
            }
            
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    self.progress_display.set_phase(worker_id, 'error', str(e)[:30])

        self.progress_display.finish()
        
        elapsed = time.time() - start_time
        print("=" * 75)
        print(f"[마스터] 모든 워커 처리 완료! (총 소요 시간: {format_time(elapsed)})")
        print("=" * 75)

    def receive_results(self, sock: socket.socket, worker_id: int) -> int:
        """워커로부터 결과 수신"""
        result = self.recv_json(sock)
        if not result:
            return 0

        recovered_count = int(result.get("recovered_count", 0))
        
        if recovered_count > 0:
            self.progress_display.set_phase(worker_id, 'receiving', f'0/{recovered_count}')

        for i in range(recovered_count):
            meta = self.recv_json(sock)
            if not meta:
                break

            offset = int(meta.get("offset", -1))

            tmp_path = self.results_dir / f"__tmp_worker{worker_id}_off{offset}.jpg"
            received = self.recv_binary_stream_to_file(sock, tmp_path)

            self.progress_display.set_phase(worker_id, 'receiving', f'{i+1}/{recovered_count}')

            if received <= 0:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            # 중복 제거 (해시)
            md5 = hashlib.md5()
            with open(tmp_path, "rb") as rf:
                for chunk in iter(lambda: rf.read(1024 * 1024), b""):
                    md5.update(chunk)
            file_hash = md5.hexdigest()

            with self.lock:
                if file_hash in self.file_hashes:
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue

                self.file_hashes.add(file_hash)

                final_name = f"recovered_{offset}_{file_hash[:8]}.jpg"
                final_path = self.results_dir / final_name
                tmp_path.replace(final_path)

                self.recovered_files.append({
                    "filename": final_name,
                    "size": received,
                    "offset": offset,
                    "hash": file_hash,
                    "worker_id": worker_id,
                })

        return recovered_count

    def print_summary(self) -> None:
        print("\n" + "=" * 75)
        print("  파일 카빙 완료 - 결과 요약")
        print("=" * 75)
        print(f"  총 복구 파일: {len(self.recovered_files)}개 (중복 제거됨)")
        
        if not self.recovered_files:
            return

        total = sum(x["size"] for x in self.recovered_files)
        print(f"  총 복구 크기: {format_size(total)}")
        print(f"  저장 위치: {self.results_dir.resolve()}")

        # 워커별 통계
        worker_stats = {}
        for f in self.recovered_files:
            wid = f["worker_id"]
            if wid not in worker_stats:
                worker_stats[wid] = {"count": 0, "size": 0}
            worker_stats[wid]["count"] += 1
            worker_stats[wid]["size"] += f["size"]

        print("\n  워커별 복구 현황:")
        for wid in sorted(worker_stats.keys()):
            stats = worker_stats[wid]
            print(f"    - 워커 {wid}: {stats['count']}개 파일, {format_size(stats['size'])}")

        print("\n  복구된 파일 목록:")
        for f in self.recovered_files:
            print(f"    - {f['filename']} ({format_size(f['size'])}, 워커 {f['worker_id']})")
        print("=" * 75)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="파일 카빙 마스터 서버 (병렬 + 멀티라인 진행률)")
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
