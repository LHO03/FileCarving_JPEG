#!/usr/bin/env python3
"""
파일 카빙 분산 처리 시스템 - 워커 (네트워크 버전)
마스터로부터 청크를 수신하여 JPEG 카빙 후 결과 전송
"""

import socket
import json
import struct
import os
import sys
import hashlib


class FileCarvingWorker:
    def __init__(self, master_host, master_port=5000):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_id = f"worker_{os.getpid()}"
        self.socket = None
        self.recovered_files = []
        
    def connect(self):
        """마스터 서버에 연결"""
        print(f"[워커] 마스터 서버 연결 중: {self.master_host}:{self.master_port}")
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.master_host, self.master_port))
        
        # 워커 정보 전송
        worker_info = {
            'worker_id': self.worker_id,
            'hostname': socket.gethostname(),
            'status': 'ready'
        }
        self.send_json(worker_info)
        
        print(f"[워커] 연결 성공!")
        return True
    
    def run(self):
        """워커 메인 루프"""
        try:
            if not self.connect():
                return
            
            # 1. 작업 정보 수신
            print(f"[워커] 작업 대기 중...")
            task_info = self.receive_json()
            
            if not task_info:
                print(f"[워커] 작업 수신 실패")
                return
            
            print(f"[워커] 작업 수신:")
            print(f"  - Task ID: {task_info['task_id']}")
            print(f"  - 담당 영역: {task_info['start_offset']:,} ~ {task_info['end_offset']:,}")
            print(f"  - 청크 크기: {task_info['chunk_size']:,} bytes")
            
            # 2. 청크 데이터 수신
            print(f"[워커] 데이터 수신 중...")
            chunk_data = self.receive_binary()
            
            if not chunk_data:
                print(f"[워커] 데이터 수신 실패")
                return
            
            print(f"[워커] 데이터 수신 완료: {len(chunk_data):,} bytes")
            
            # 3. JPEG 카빙 수행
            self.carve_jpeg(chunk_data, task_info)
            
            # 4. 결과 전송
            self.send_results()
            
            print(f"[워커] 작업 완료!")
            
        except Exception as e:
            print(f"[워커] 오류: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.socket:
                self.socket.close()
    
    def carve_jpeg(self, data, task_info):
        """JPEG 파일 카빙"""
        print(f"[워커] JPEG 카빙 시작...")
        
        header = b'\xFF\xD8\xFF'  # JPEG SOI + APP marker
        footer = b'\xFF\xD9'      # JPEG EOI
        
        # 오프셋 계산을 위한 정보
        read_start = task_info['read_start']
        actual_start = task_info['start_offset']
        actual_end = task_info['end_offset']
        
        pos = 0
        found_count = 0
        
        while pos < len(data):
            # JPEG 헤더 찾기
            start = data.find(header, pos)
            if start == -1:
                break
            
            # 실제 파일 오프셋 계산
            absolute_offset = read_start + start
            
            # 이 파일이 내 담당 영역에서 시작하는지 확인
            if not (actual_start <= absolute_offset < actual_end):
                pos = start + 1
                continue
            
            # JPEG 푸터 찾기
            end = data.find(footer, start + len(header))
            
            if end == -1:
                # 푸터 없음 - 불완전한 파일
                pos = start + 1
                continue
            
            end += len(footer)
            jpeg_data = data[start:end]
            
            # 유효성 검증
            if self.validate_jpeg(jpeg_data):
                found_count += 1
                
                self.recovered_files.append({
                    'offset': absolute_offset,
                    'size': len(jpeg_data),
                    'data': jpeg_data
                })
                
                print(f"  - JPEG 발견: 오프셋 {absolute_offset:,}, 크기 {len(jpeg_data):,} bytes")
            
            pos = start + 1
        
        print(f"[워커] 카빙 완료: {found_count}개 파일 발견")
    
    def validate_jpeg(self, data):
        """JPEG 유효성 검증"""
        if len(data) < 100:
            return False
        
        # SOI 마커 확인
        if data[:2] != b'\xFF\xD8':
            return False
        
        # EOI 마커 확인
        if data[-2:] != b'\xFF\xD9':
            return False
        
        return True
    
    def send_results(self):
        """결과를 마스터에게 전송"""
        print(f"[워커] 결과 전송 중... ({len(self.recovered_files)}개 파일)")
        
        # 결과 메타데이터 전송
        result_info = {
            'worker_id': self.worker_id,
            'recovered_count': len(self.recovered_files)
        }
        self.send_json(result_info)
        
        # 각 파일 전송
        for i, file_info in enumerate(self.recovered_files):
            # 파일 메타데이터
            meta = {
                'offset': file_info['offset'],
                'size': file_info['size']
            }
            self.send_json(meta)
            
            # 파일 데이터
            self.send_binary(file_info['data'])
            
            print(f"  - 전송 완료: {i+1}/{len(self.recovered_files)}")
        
        print(f"[워커] 결과 전송 완료")
    
    def send_json(self, data):
        """JSON 데이터 전송"""
        json_bytes = json.dumps(data).encode('utf-8')
        self.socket.sendall(struct.pack('!I', len(json_bytes)))
        self.socket.sendall(json_bytes)
    
    def receive_json(self):
        """JSON 데이터 수신"""
        size_data = self._recv_exact(4)
        if not size_data:
            return None
        size = struct.unpack('!I', size_data)[0]
        
        json_bytes = self._recv_exact(size)
        if not json_bytes:
            return None
        
        return json.loads(json_bytes.decode('utf-8'))
    
    def send_binary(self, data):
        """바이너리 데이터 전송"""
        self.socket.sendall(struct.pack('!I', len(data)))
        self.socket.sendall(data)
    
    def receive_binary(self):
        """바이너리 데이터 수신"""
        size_data = self._recv_exact(4)
        if not size_data:
            return None
        size = struct.unpack('!I', size_data)[0]
        
        return self._recv_exact(size)
    
    def _recv_exact(self, size):
        """정확한 크기만큼 수신"""
        data = b''
        while len(data) < size:
            chunk = self.socket.recv(min(65536, size - len(data)))
            if not chunk:
                return None
            data += chunk
        return data


def main():
    if len(sys.argv) < 2:
        print("사용법: python file_carving_worker.py <마스터_IP> [포트]")
        print("예시: python file_carving_worker.py 192.168.1.100 5000")
        sys.exit(1)
    
    master_host = sys.argv[1]
    master_port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    
    worker = FileCarvingWorker(master_host, master_port)
    worker.run()


if __name__ == "__main__":
    main()
