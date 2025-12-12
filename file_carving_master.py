#!/usr/bin/env python3
"""
파일 카빙 분산 처리 시스템 - 마스터 서버 (네트워크 버전)
DD 이미지를 청크로 나눠 워커들에게 전송하고, 복구된 파일을 수신
"""

import socket
import json
import struct
import os
import hashlib
import time
import threading
from pathlib import Path


class FileCarvingMaster:
    def __init__(self, port=5000, overlap_size=1*1024*1024):  # 1MB 오버랩
        self.port = port
        self.workers = []
        self.recovered_files = []
        self.dd_image_path = None
        self.image_size = 0
        self.overlap_size = overlap_size
        self.results_dir = Path("recovered_files")
        self.results_dir.mkdir(exist_ok=True)
        self.file_hashes = set()  # 중복 제거용
        self.lock = threading.Lock()
        
    def load_dd_image(self, image_path):
        """DD 이미지 로드"""
        self.dd_image_path = Path(image_path)
        
        if not self.dd_image_path.exists():
            print(f"[마스터] 오류: 이미지 파일을 찾을 수 없음: {image_path}")
            return False
        
        self.image_size = self.dd_image_path.stat().st_size
        print(f"[마스터] DD 이미지 로드: {self.dd_image_path}")
        print(f"[마스터] 이미지 크기: {self.image_size:,} bytes ({self.image_size / 1024 / 1024:.2f} MB)")
        return True
    
    def start_server(self):
        """마스터 서버 시작"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', self.port))
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
                    
                    # 워커 정보 수신
                    worker_info = self.receive_json(client_socket)
                    print(f"[마스터] 워커 정보: {worker_info}")
                    
                    self.workers.append({
                        'socket': client_socket,
                        'address': addr,
                        'info': worker_info
                    })
                    
                except socket.timeout:
                    if self.workers:
                        print(f"\n[마스터] 총 {len(self.workers)}개 워커 연결 완료")
                        break
                    else:
                        print("[마스터] 워커 연결 없음. 종료합니다.")
                        return
        except Exception as e:
            print(f"[마스터] 서버 오류: {e}")
            return
        
        # 작업 분배 및 처리
        self.distribute_and_process()
        
        # 결과 요약
        self.print_summary()
    
    def distribute_and_process(self):
        """청크 분배 및 결과 수집"""
        if not self.workers or not self.dd_image_path:
            return
        
        num_workers = len(self.workers)
        base_chunk_size = self.image_size // num_workers
        
        print(f"\n[마스터] 작업 분배 시작")
        print(f"  - 전체 크기: {self.image_size:,} bytes")
        print(f"  - 워커 수: {num_workers}")
        print(f"  - 청크 크기: ~{base_chunk_size:,} bytes")
        print(f"  - 오버랩: {self.overlap_size:,} bytes")
        
        # 각 워커에게 청크 전송 및 결과 수신 (순차 처리)
        with open(self.dd_image_path, 'rb') as f:
            for i, worker in enumerate(self.workers):
                # 시작/끝 오프셋 계산 (오버랩 포함)
                start_offset = i * base_chunk_size
                end_offset = (i + 1) * base_chunk_size if i < num_workers - 1 else self.image_size
                
                # 오버랩 적용
                read_start = max(0, start_offset - self.overlap_size // 2) if i > 0 else 0
                read_end = min(self.image_size, end_offset + self.overlap_size // 2) if i < num_workers - 1 else self.image_size
                
                # 청크 읽기
                f.seek(read_start)
                chunk_data = f.read(read_end - read_start)
                
                print(f"\n[마스터] 워커 {i} ({worker['address'][0]}) 처리 중...")
                print(f"  - 담당 영역: {start_offset:,} ~ {end_offset:,}")
                print(f"  - 전송 영역: {read_start:,} ~ {read_end:,} ({len(chunk_data):,} bytes)")
                
                # 작업 정보 전송
                task_info = {
                    'task_id': i,
                    'start_offset': start_offset,
                    'end_offset': end_offset,
                    'read_start': read_start,
                    'read_end': read_end,
                    'chunk_size': len(chunk_data),
                    'total_workers': num_workers
                }
                
                try:
                    # 1. 작업 정보 전송
                    self.send_json(worker['socket'], task_info)
                    
                    # 2. 청크 데이터 전송
                    self.send_binary(worker['socket'], chunk_data)
                    print(f"  - 데이터 전송 완료")
                    
                    # 3. 결과 수신
                    self.receive_results(worker, i)
                    
                except Exception as e:
                    print(f"  - 오류 발생: {e}")
                finally:
                    worker['socket'].close()
    
    def receive_results(self, worker, worker_id):
        """워커로부터 결과 수신"""
        # 결과 메타데이터 수신
        result = self.receive_json(worker['socket'])
        
        if not result:
            print(f"  - 결과 수신 실패")
            return
        
        recovered_count = result.get('recovered_count', 0)
        print(f"  - 발견된 파일: {recovered_count}개")
        
        # 각 파일 수신
        for j in range(recovered_count):
            # 파일 메타데이터
            file_info = self.receive_json(worker['socket'])
            
            # 파일 데이터
            file_data = self.receive_binary(worker['socket'])
            
            if file_data:
                # 중복 체크 (파일 해시 기반)
                file_hash = hashlib.md5(file_data).hexdigest()
                
                with self.lock:
                    if file_hash not in self.file_hashes:
                        self.file_hashes.add(file_hash)
                        
                        # 파일 저장
                        filename = f"recovered_{file_info['offset']}_{file_hash[:8]}.jpg"
                        filepath = self.results_dir / filename
                        
                        with open(filepath, 'wb') as f:
                            f.write(file_data)
                        
                        self.recovered_files.append({
                            'filename': filename,
                            'size': len(file_data),
                            'offset': file_info['offset'],
                            'hash': file_hash,
                            'worker_id': worker_id
                        })
                        
                        print(f"    + 저장: {filename} ({len(file_data):,} bytes)")
                    else:
                        print(f"    - 중복 스킵: offset {file_info['offset']}")
    
    def send_json(self, sock, data):
        """JSON 데이터 전송"""
        json_bytes = json.dumps(data).encode('utf-8')
        # 길이(4바이트) + 데이터
        sock.sendall(struct.pack('!I', len(json_bytes)))
        sock.sendall(json_bytes)
    
    def receive_json(self, sock):
        """JSON 데이터 수신"""
        # 길이 수신
        size_data = self._recv_exact(sock, 4)
        if not size_data:
            return None
        size = struct.unpack('!I', size_data)[0]
        
        # 데이터 수신
        json_bytes = self._recv_exact(sock, size)
        if not json_bytes:
            return None
        
        return json.loads(json_bytes.decode('utf-8'))
    
    def send_binary(self, sock, data):
        """바이너리 데이터 전송"""
        # 길이(4바이트) + 데이터
        sock.sendall(struct.pack('!I', len(data)))
        sock.sendall(data)
    
    def receive_binary(self, sock):
        """바이너리 데이터 수신"""
        size_data = self._recv_exact(sock, 4)
        if not size_data:
            return None
        size = struct.unpack('!I', size_data)[0]
        
        return self._recv_exact(sock, size)
    
    def _recv_exact(self, sock, size):
        """정확한 크기만큼 수신"""
        data = b''
        while len(data) < size:
            chunk = sock.recv(min(65536, size - len(data)))
            if not chunk:
                return None
            data += chunk
        return data
    
    def print_summary(self):
        """결과 요약 출력"""
        print("\n" + "=" * 60)
        print("파일 카빙 완료 - 결과 요약")
        print("=" * 60)
        print(f"총 복구 파일: {len(self.recovered_files)}개 (중복 제거됨)")
        
        if self.recovered_files:
            total_size = sum(f['size'] for f in self.recovered_files)
            print(f"총 복구 크기: {total_size:,} bytes ({total_size / 1024:.2f} KB)")
            print(f"저장 위치: {self.results_dir.absolute()}")
            
            print("\n복구된 파일 목록:")
            for f in self.recovered_files:
                print(f"  - {f['filename']} ({f['size']:,} bytes, 워커 {f['worker_id']})")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='파일 카빙 마스터 서버')
    parser.add_argument('image', help='DD 이미지 파일 경로')
    parser.add_argument('--port', '-p', type=int, default=5000, help='서버 포트 (기본: 5000)')
    parser.add_argument('--overlap', '-o', type=int, default=1, help='오버랩 크기 MB (기본: 1)')
    parser.add_argument('--output', '-O', type=str, default='recovered_files', help='출력 디렉토리')
    args = parser.parse_args()
    
    master = FileCarvingMaster(
        port=args.port,
        overlap_size=args.overlap * 1024 * 1024
    )
    master.results_dir = Path(args.output)
    master.results_dir.mkdir(exist_ok=True)
    
    if master.load_dd_image(args.image):
        master.start_server()


if __name__ == "__main__":
    main()
