#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
파일 카빙 분산 처리 시스템 - 워커 (Worker / Client)
================================================================================

[개요]
이 프로그램은 삭제된 JPEG 파일을 복구하기 위한 "분산 파일 카빙 시스템"의 
워커(클라이언트) 역할을 합니다.
워커는 마스터로부터 DD 이미지의 일부(청크)를 받아서 JPEG 파일을 찾고,
발견된 파일들을 다시 마스터에게 전송합니다.

[파일 카빙(File Carving)이란?]
삭제된 파일은 파일 시스템의 메타데이터(파일명, 위치 정보 등)만 지워지고,
실제 데이터는 디스크에 남아있는 경우가 많습니다.
파일 카빙은 파일의 시그니처(고유 패턴)를 이용해 이러한 데이터를 복구합니다.

[JPEG 시그니처]
- SOI (Start Of Image): FF D8 - JPEG 파일의 시작
- EOI (End Of Image): FF D9 - JPEG 파일의 끝
워커는 청크에서 FF D8로 시작해서 FF D9로 끝나는 데이터를 찾습니다.

[동작 흐름]
1. 마스터 서버에 연결
2. 작업 정보(task) 수신
3. 청크 데이터 수신 (스트리밍)
4. 청크에서 JPEG 파일 카빙
5. 발견된 JPEG 파일들을 마스터로 전송
================================================================================
"""

# ============================================================================
# 라이브러리 임포트 (Import Statements)
# ============================================================================

import socket          # TCP/IP 네트워크 통신을 위한 표준 라이브러리
                       # 워커는 클라이언트로서 마스터 서버에 연결합니다

import json            # JSON 데이터 처리 (딕셔너리 <-> 문자열 변환)

import struct          # 바이트와 Python 데이터 타입 간 변환
                       # 네트워크 통신에서 데이터 길이를 바이트로 표현할 때 사용

import tempfile        # 임시 파일/디렉토리 생성 라이브러리
                       # TemporaryDirectory: 자동으로 삭제되는 임시 디렉토리 생성
                       # 청크 데이터를 임시로 저장할 때 사용

import time            # 시간 관련 함수 (현재 시간, 경과 시간 계산 등)

import sys             # 시스템 관련 기능
                       # sys.stdout: 표준 출력 스트림 (진행률 표시에 사용)
                       # sys.stdout.flush(): 버퍼를 즉시 출력

from pathlib import Path  # 파일 경로를 객체지향적으로 다루는 클래스
                          # 문자열보다 안전하고 편리한 경로 처리 가능


# ============================================================================
# 상수 정의 (Constants)
# ============================================================================

# JSON 메시지의 길이를 표현하는 형식
# struct 포맷 문자열:
#   "!" : 네트워크 바이트 순서 (빅 엔디안, Big-endian)
#         - 빅 엔디안: 가장 큰 바이트가 먼저 오는 방식
#         - 네트워크 표준 바이트 순서
#   "I" : unsigned int (부호 없는 정수, 4바이트)
JSON_LEN_FMT = "!I"      # 4바이트 부호 없는 정수 형식
JSON_LEN_SIZE = 4        # 길이 필드 크기: 4바이트

# 바이너리 데이터의 길이를 표현하는 형식
#   "Q" : unsigned long long (부호 없는 정수, 8바이트)
# 30GB 같은 큰 청크도 표현할 수 있도록 8바이트 사용
BIN_LEN_FMT = "!Q"       # 8바이트 부호 없는 정수 형식
BIN_LEN_SIZE = 8         # 길이 필드 크기: 8바이트


# ============================================================================
# 유틸리티 함수 (Utility Functions)
# ============================================================================

def format_size(size_bytes):
    """
    바이트 단위의 크기를 사람이 읽기 쉬운 형식으로 변환합니다.
    
    [알고리즘]
    1024바이트 = 1KB, 1024KB = 1MB, ... 이런 식으로 단위를 올려가며
    값이 1024보다 작아질 때까지 나눕니다.
    
    [매개변수]
    size_bytes (int/float): 바이트 단위의 크기
    
    [반환값]
    str: 사람이 읽기 쉬운 형식의 문자열
    
    [사용 예시]
    >>> format_size(1536)
    '1.50 KB'
    >>> format_size(1073741824)
    '1.00 GB'
    """
    # 단위 목록 (바이트 -> 킬로바이트 -> 메가바이트 -> 기가바이트 -> 테라바이트)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:  # 1024보다 작으면 현재 단위 사용
            return f"{size_bytes:.2f} {unit}"  # 소수점 둘째 자리까지
        size_bytes /= 1024  # 다음 단위로
    # TB보다 크면 PB(페타바이트)로 표시
    return f"{size_bytes:.2f} PB"


def format_speed(bytes_per_sec):
    """
    초당 바이트 수를 전송 속도 형식으로 변환합니다.
    
    [매개변수]
    bytes_per_sec (float): 초당 전송 바이트 수
    
    [반환값]
    str: 속도 문자열 (예: "52.30 MB/s")
    """
    return f"{format_size(bytes_per_sec)}/s"


# ============================================================================
# ProgressBar 클래스
# ============================================================================

class ProgressBar:
    """
    콘솔에 진행률을 표시하는 클래스입니다.
    
    [동작 원리]
    \\r (캐리지 리턴)을 사용하여 커서를 줄 맨 앞으로 이동한 후
    같은 줄에 새 내용을 덮어씁니다.
    이렇게 하면 한 줄에서 진행률이 업데이트되는 효과를 얻습니다.
    
    [출력 형식 예시]
    [청크 수신] |████████████░░░░░░░░░░░░░░░░░░| 40.5% (5.91GB/14.59GB) 62.3MB/s 남은 시간: 2.3분
    
    [사용 예시]
    >>> progress = ProgressBar(total=1000000, description="다운로드")
    >>> for i in range(0, 1000000, 10000):
    ...     progress.update(10000)  # 10000바이트 처리됨
    >>> progress.finish()
    """
    
    def __init__(self, total, description="진행"):
        """
        ProgressBar 생성자
        
        [매개변수]
        total (int): 전체 처리해야 할 양 (바이트 등)
        description (str): 진행률 앞에 표시할 설명
        """
        self.total = total             # 전체 처리량
        self.current = 0               # 현재까지 처리된 양
        self.description = description # 표시할 설명 텍스트
        self.start_time = time.time()  # 시작 시간 (경과 시간 및 속도 계산용)
        self.last_print_time = 0       # 마지막 출력 시간 (너무 자주 출력하지 않기 위해)
        self.bar_width = 30            # 프로그레스 바의 문자 폭
    
    def update(self, amount):
        """
        진행률을 업데이트합니다.
        
        [매개변수]
        amount (int): 이번에 처리된 양
        
        [참고]
        너무 자주 화면을 갱신하면 성능이 저하되므로
        0.3초 간격으로만 출력합니다.
        """
        self.current += amount  # 현재 처리량 누적
        now = time.time()
        
        # 0.3초마다 또는 완료 시 출력
        if now - self.last_print_time >= 0.3 or self.current >= self.total:
            self.last_print_time = now
            self._print_progress()
    
    def _print_progress(self):
        """
        현재 진행률을 화면에 출력합니다.
        (private 메서드 - 클래스 내부에서만 사용)
        
        [출력 내용]
        - 프로그레스 바 (████████░░░░░░)
        - 백분율
        - 현재/전체 크기
        - 전송 속도
        - 예상 남은 시간
        """
        # 경과 시간 계산
        elapsed = time.time() - self.start_time
        
        # 전송 속도 계산 (바이트/초)
        if elapsed > 0:
            speed = self.current / elapsed
        else:
            speed = 0
        
        # 백분율 계산 (0으로 나누기 방지)
        percent = (self.current / self.total) * 100 if self.total > 0 else 0
        
        # 프로그레스 바 생성
        # filled: 채워진 블록 수
        filled = int(self.bar_width * self.current / self.total) if self.total > 0 else 0
        # █(채워진 블록)과 ░(빈 블록)으로 바 구성
        bar = '█' * filled + '░' * (self.bar_width - filled)
        
        # 남은 시간(ETA: Estimated Time of Arrival) 계산
        if speed > 0 and self.current < self.total:
            remaining = (self.total - self.current) / speed  # 남은 바이트 / 속도
            if remaining > 60:
                eta = f"남은 시간: {remaining/60:.1f}분"  # 1분 이상이면 분으로 표시
            else:
                eta = f"남은 시간: {remaining:.0f}초"
        else:
            eta = ""
        
        # \r: 캐리지 리턴 - 커서를 줄 맨 앞으로 이동
        # 이렇게 하면 같은 줄에 덮어쓰기 가능
        # end="": 줄바꿈 없이 출력
        print(f"\r[{self.description}] |{bar}| {percent:.1f}% "
              f"({format_size(self.current)}/{format_size(self.total)}) "
              f"{format_speed(speed)} {eta}    ", end="")
        
        # flush(): 버퍼를 즉시 출력
        # Python은 기본적으로 줄바꿈이 있어야 출력하는데,
        # end=""로 줄바꿈을 없앴으므로 명시적으로 flush 필요
        sys.stdout.flush()
    
    def finish(self):
        """
        진행률 표시를 완료하고 결과를 출력합니다.
        100% 완료 메시지와 평균 속도를 표시합니다.
        """
        elapsed = time.time() - self.start_time
        
        # 평균 속도 계산
        if elapsed > 0:
            avg_speed = self.total / elapsed
        else:
            avg_speed = 0
        
        # 완료된 프로그레스 바 (100%)
        bar = '█' * self.bar_width
        
        # 최종 메시지 출력 (줄바꿈 포함)
        print(f"\r[{self.description}] |{bar}| 100% 완료! "
              f"({format_size(self.total)}, {elapsed:.1f}초, 평균 {format_speed(avg_speed)})    ")


# ============================================================================
# FileCarvingWorker 클래스
# ============================================================================

class FileCarvingWorker:
    """
    파일 카빙 분산 처리 시스템의 워커(클라이언트) 클래스입니다.
    
    [역할]
    1. 마스터 서버에 연결
    2. 청크(DD 이미지의 일부) 데이터 수신
    3. 청크에서 JPEG 파일 카빙 (시그니처 검색)
    4. 발견된 JPEG 파일들을 마스터로 전송
    
    [JPEG 카빙 원리]
    JPEG 파일은 항상 FF D8 (SOI: Start Of Image)로 시작하고
    FF D9 (EOI: End Of Image)로 끝납니다.
    청크에서 이 패턴을 찾아 JPEG 데이터를 추출합니다.
    
    [스트리밍 처리]
    대용량 청크(수 GB)를 메모리에 전부 올리면 메모리 부족이 발생합니다.
    따라서:
    1. 청크를 임시 파일로 저장
    2. 파일을 블록 단위로 읽어가며 카빙
    이렇게 하면 메모리 사용량을 최소화할 수 있습니다.
    
    [속성(Attributes)]
    - master_host: 마스터 서버의 IP 주소
    - master_port: 마스터 서버의 포트 번호
    - stream_block_size: 스트리밍 시 한 번에 처리하는 블록 크기
    - socket: 마스터와 연결된 소켓
    - worker_id: 이 워커의 고유 ID
    - hostname: 이 PC의 호스트명
    - local_out_dir: 복구된 파일을 임시 저장하는 로컬 디렉토리
    """
    
    def __init__(self, master_host: str, master_port: int, stream_block_size=4 * 1024 * 1024):
        """
        FileCarvingWorker 생성자
        
        [매개변수]
        master_host (str): 마스터 서버의 IP 주소 또는 호스트명
            예: "192.168.1.100", "localhost"
            
        master_port (int): 마스터 서버의 포트 번호
            예: 5000
            
        stream_block_size (int): 스트리밍 블록 크기 (기본값: 4MB)
            - 파일을 읽고 쓸 때 한 번에 처리하는 크기
            - 너무 작으면 I/O 오버헤드 증가
            - 너무 크면 메모리 사용량 증가
        """
        self.master_host = master_host
        self.master_port = master_port
        self.stream_block_size = stream_block_size
        
        self.socket = None      # 마스터와 연결된 소켓 (연결 전에는 None)
        self.worker_id = None   # 워커 ID (연결 후 설정)
        
        # gethostname(): 현재 컴퓨터의 호스트명 반환
        # 워커 식별 및 로깅에 사용
        self.hostname = socket.gethostname()

        # 복구된 파일을 임시로 저장할 로컬 디렉토리
        # 나중에 마스터로 전송한 후에도 로컬에 남아있음
        self.local_out_dir = Path("worker_recovered")
        self.local_out_dir.mkdir(exist_ok=True)  # 디렉토리 생성 (이미 있으면 무시)

    # ========================================================================
    # 네트워크 통신 헬퍼 메서드 (Networking Helper Methods)
    # ========================================================================

    def _recv_exact(self, size: int) -> bytes:
        """
        소켓에서 정확히 지정된 크기만큼 데이터를 수신합니다.
        
        [왜 이 함수가 필요한가?]
        TCP의 recv() 함수는 요청한 크기보다 적은 데이터를 반환할 수 있습니다.
        네트워크 상황에 따라 데이터가 여러 패킷으로 나뉘어 도착하기 때문입니다.
        
        예를 들어, 10000바이트를 요청해도 처음에 3000바이트만 오고,
        다음 recv()에서 나머지 7000바이트가 올 수 있습니다.
        
        이 함수는 정확히 요청한 크기가 모일 때까지 반복해서 recv()를 호출합니다.
        
        [매개변수]
        size (int): 받을 데이터의 정확한 크기 (바이트)
        
        [반환값]
        bytes: 수신한 데이터 (정확히 size 바이트)
               연결이 끊어지면 빈 bytes 반환
        """
        # bytearray: 수정 가능한(mutable) 바이트 시퀀스
        # bytes는 불변(immutable)이라 추가할 때마다 새 객체 생성
        # bytearray는 extend()로 효율적으로 추가 가능
        buf = bytearray()
        
        while len(buf) < size:
            # recv(n): 소켓에서 최대 n바이트 수신
            # min(65536, size - len(buf)): 남은 크기와 64KB 중 작은 값
            # 65536(64KB)은 일반적인 TCP 버퍼 크기
            chunk = self.socket.recv(min(65536, size - len(buf)))
            
            # recv()가 빈 값을 반환하면 연결이 끊어진 것
            if not chunk:
                return b""  # 빈 bytes 반환
            
            # extend(): bytearray에 데이터 추가
            buf.extend(chunk)
        
        # bytes 타입으로 변환하여 반환
        return bytes(buf)

    def send_json(self, obj: dict) -> None:
        """
        딕셔너리를 JSON 형식으로 마스터에게 전송합니다.
        
        [프로토콜 형식]
        [4바이트: JSON 길이][JSON 문자열(UTF-8)]
        
        수신 측에서는 먼저 4바이트를 읽어 길이를 알아내고,
        그 길이만큼 추가로 읽어서 JSON을 파싱합니다.
        
        [매개변수]
        obj (dict): 전송할 딕셔너리
        
        [동작 순서]
        1. json.dumps(): 딕셔너리 -> JSON 문자열
        2. encode("utf-8"): 문자열 -> 바이트
        3. struct.pack(): 길이를 4바이트로 패킹
        4. sendall(): 길이 전송
        5. sendall(): 데이터 전송
        
        [사용 예시]
        >>> worker.send_json({"status": "ready", "worker_id": "worker_12345"})
        """
        # json.dumps(obj): Python 딕셔너리를 JSON 문자열로 변환
        # encode("utf-8"): 문자열을 UTF-8 바이트로 인코딩
        payload = json.dumps(obj).encode("utf-8")
        
        # struct.pack(format, value): 값을 지정된 형식의 바이트로 변환
        # "!I": 네트워크 바이트 순서, 4바이트 unsigned int
        # len(payload): JSON 데이터의 길이
        self.socket.sendall(struct.pack(JSON_LEN_FMT, len(payload)))
        
        # sendall(): 모든 데이터가 전송될 때까지 대기
        # send()와 달리 부분 전송 걱정 없이 전체 전송 보장
        self.socket.sendall(payload)

    def recv_json(self):
        """
        마스터로부터 JSON 데이터를 수신하여 딕셔너리로 반환합니다.
        
        [프로토콜 형식]
        [4바이트: JSON 길이][JSON 문자열(UTF-8)]
        
        [반환값]
        dict: 수신한 JSON을 파싱한 딕셔너리
              실패 시 None 반환
        
        [동작 순서]
        1. 4바이트 수신 (길이 정보)
        2. struct.unpack()으로 길이 추출
        3. 길이만큼 JSON 데이터 수신
        4. UTF-8 디코딩 후 JSON 파싱
        """
        # 1. 길이 정보 수신 (4바이트)
        size_b = self._recv_exact(JSON_LEN_SIZE)
        if not size_b:
            return None  # 연결 끊김
        
        # 2. struct.unpack(): 바이트를 Python 값으로 변환
        # 반환값은 튜플이므로 [0]으로 첫 번째 요소 추출
        size = struct.unpack(JSON_LEN_FMT, size_b)[0]
        
        # 3. JSON 데이터 수신
        payload = self._recv_exact(size)
        if not payload:
            return None
        
        # 4. 바이트 -> 문자열 -> 딕셔너리
        # decode("utf-8"): 바이트를 UTF-8 문자열로 디코딩
        # json.loads(): JSON 문자열을 딕셔너리로 파싱
        return json.loads(payload.decode("utf-8"))

    def recv_binary_stream_to_file_with_progress(self, out_path: Path) -> int:
        """
        마스터로부터 바이너리 데이터를 수신하여 파일로 저장합니다.
        진행률을 화면에 표시합니다.
        
        [프로토콜 형식]
        [8바이트: 데이터 길이][바이너리 데이터 스트림...]
        
        [왜 파일로 저장하는가?]
        청크가 수 GB일 수 있어서 메모리에 전부 올리면 OutOfMemory 발생.
        파일로 저장하면 디스크 공간만 있으면 됩니다.
        
        [매개변수]
        out_path (Path): 저장할 파일 경로
        
        [반환값]
        int: 수신한 총 바이트 수 (실패 시 -1)
        """
        # 1. 길이 정보 수신 (8바이트)
        size_b = self._recv_exact(BIN_LEN_SIZE)
        if not size_b:
            return -1
        
        # "!Q": 8바이트 unsigned long long
        total = struct.unpack(BIN_LEN_FMT, size_b)[0]

        remaining = total  # 남은 수신량
        
        # 부모 디렉토리 생성 (필요한 경우)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # 진행률 표시 객체 생성
        progress = ProgressBar(total, "청크 수신")

        # 파일에 기록 ("wb": write binary 모드)
        with open(out_path, "wb") as f:
            while remaining > 0:
                # 소켓에서 데이터 수신 (최대 64KB씩)
                chunk = self.socket.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                
                # 파일에 기록
                f.write(chunk)
                remaining -= len(chunk)
                
                # 진행률 업데이트
                progress.update(len(chunk))
        
        # 완료 메시지 출력
        progress.finish()
        return total

    def send_binary_stream_from_file_with_progress(self, file_path: Path, 
                                                    file_num: int, total_files: int) -> int:
        """
        파일을 바이너리 스트림으로 마스터에게 전송합니다.
        진행률을 화면에 표시합니다.
        
        [프로토콜 형식]
        [8바이트: 데이터 길이][바이너리 데이터 스트림...]
        
        [매개변수]
        file_path (Path): 전송할 파일 경로
        file_num (int): 현재 파일 번호 (0부터 시작, 진행률 표시용)
        total_files (int): 전체 파일 수 (진행률 표시용)
        
        [반환값]
        int: 전송한 총 바이트 수
        """
        # stat().st_size: 파일 크기 (바이트)
        total = file_path.stat().st_size
        
        # 8바이트 길이 정보 먼저 전송
        self.socket.sendall(struct.pack(BIN_LEN_FMT, total))

        # 작은 파일은 진행률 표시 생략 (512KB 미만)
        show_progress = total > 512 * 1024

        # 파일에서 읽어서 전송 ("rb": read binary)
        with open(file_path, "rb") as f:
            remaining = total  # 남은 전송량
            sent = 0           # 전송 완료량
            
            while remaining > 0:
                # 파일에서 데이터 읽기 (블록 크기와 남은 크기 중 작은 값)
                data = f.read(min(self.stream_block_size, remaining))
                if not data:
                    raise IOError("Unexpected EOF while sending file")
                
                # 소켓으로 전송
                self.socket.sendall(data)
                remaining -= len(data)
                sent += len(data)
                
                # 진행률 표시 (큰 파일만)
                if show_progress:
                    percent = (sent / total) * 100
                    print(f"\r[결과 전송] 파일 {file_num+1}/{total_files}: "
                          f"{format_size(sent)}/{format_size(total)} ({percent:.0f}%)    ", end="")
                    sys.stdout.flush()
        
        # 완료 메시지 (큰 파일만)
        if show_progress:
            print(f"\r[결과 전송] 파일 {file_num+1}/{total_files}: {format_size(total)} 완료!    ")
        
        return total

    # ========================================================================
    # JPEG 카빙 메서드 (JPEG Carving Methods)
    # ========================================================================

    def carve_jpeg_from_file_with_progress(self, chunk_path: Path, base_offset: int, 
                                           scan_block=8 * 1024 * 1024, 
                                           tail_keep=2 * 1024 * 1024):
        """
        청크 파일에서 JPEG 파일들을 카빙(추출)합니다.
        
        [JPEG 구조]
        JPEG 파일은 다음과 같은 구조를 가집니다:
        - SOI (Start Of Image): FF D8 - 파일 시작 마커
        - 여러 세그먼트 (JFIF, DQT, DHT, SOF, SOS 등)
        - 이미지 데이터
        - EOI (End Of Image): FF D9 - 파일 끝 마커
        
        [알고리즘]
        1. 청크를 블록 단위로 읽음 (메모리 절약)
        2. FF D8 (SOI) 패턴 검색
        3. SOI 발견 시 이후에서 FF D9 (EOI) 검색
        4. SOI ~ EOI 사이의 데이터를 JPEG 파일로 저장
        
        [오버랩 처리]
        블록 경계에서 시그니처가 잘릴 수 있으므로,
        이전 블록의 끝부분(tail_keep)을 다음 블록과 함께 처리합니다.
        
        예: 블록1 끝이 "...FF", 블록2 시작이 "D8..."인 경우
            tail_keep이 없으면 SOI(FF D8)를 놓침
        
        [매개변수]
        chunk_path (Path): 카빙할 청크 파일 경로
        base_offset (int): 이 청크의 원본 DD 이미지에서의 시작 오프셋
            - 복구된 파일의 정확한 위치 정보를 위해 필요
        scan_block (int): 한 번에 읽어서 검색할 블록 크기 (기본값: 8MB)
        tail_keep (int): 블록 경계 처리를 위해 유지할 끝부분 크기 (기본값: 2MB)
        
        [반환값]
        list: 발견된 JPEG 파일 정보 리스트
            각 항목: {"offset": int, "path": Path, "size": int}
        """
        # JPEG 시그니처 정의
        # b"..." : 바이트 리터럴 (문자열이 아닌 바이트 시퀀스)
        SOI = b"\xff\xd8"  # Start Of Image - JPEG 시작
        EOI = b"\xff\xd9"  # End Of Image - JPEG 끝

        found = []     # 발견된 JPEG 파일 정보 리스트
        file_idx = 0   # 파일 인덱스 (파일명 생성용)
        
        # 청크 전체 크기 (진행률 계산용)
        chunk_size = chunk_path.stat().st_size
        
        # 진행률 표시 객체 생성
        progress = ProgressBar(chunk_size, "JPEG 카빙")

        # 청크 파일 열기 ("rb": read binary)
        with open(chunk_path, "rb") as f:
            file_pos = 0    # 현재 파일 읽기 위치
            carry = b""     # 이전 블록에서 이월된 데이터 (tail)
            carry_pos = 0   # carry의 청크 내 시작 위치

            while True:
                # 블록 단위로 파일 읽기
                data = f.read(scan_block)
                if not data:
                    break  # 파일 끝

                # 이월된 데이터(carry)와 새 데이터 합치기
                # 블록 경계에 걸친 시그니처를 놓치지 않기 위함
                buf = carry + data
                
                # buf[0]이 해당하는 청크 내 위치
                buf_start_pos = carry_pos
                
                # 다음 iteration을 위한 carry 준비
                # buf의 마지막 tail_keep 바이트를 다음으로 이월
                if len(buf) > tail_keep:
                    next_carry = buf[-tail_keep:]  # 뒤에서 tail_keep 만큼
                    next_carry_pos = buf_start_pos + (len(buf) - tail_keep)
                else:
                    next_carry = buf
                    next_carry_pos = buf_start_pos

                # 현재 buf에서 JPEG 검색
                idx = 0  # buf 내 검색 시작 위치
                
                while True:
                    # SOI (FF D8) 찾기
                    # find(pattern, start): start 위치부터 pattern 검색
                    # 반환값: 찾은 위치 (없으면 -1)
                    s = buf.find(SOI, idx)
                    if s < 0:
                        break  # SOI 없음 -> 다음 블록으로
                    
                    # SOI 발견! 이후에서 EOI (FF D9) 찾기
                    e = buf.find(EOI, s + 2)  # SOI 다음부터 검색
                    if e < 0:
                        # EOI가 현재 buf에 없음
                        # 파일이 다음 블록에 걸쳐있을 수 있음
                        # 단순화를 위해 현재는 스킵 (완전한 구현은 상태 유지 필요)
                        idx = s + 2  # SOI 다음부터 다시 검색
                        continue

                    # JPEG 데이터 추출 (SOI부터 EOI까지, EOI 포함)
                    # buf[s:e+2]: s부터 e+1까지 (슬라이싱은 끝 인덱스 미포함)
                    jpg_bytes = buf[s : e + 2]
                    
                    # 원본 DD 이미지에서의 절대 오프셋 계산
                    # base_offset: 청크의 시작 오프셋
                    # buf_start_pos: buf의 청크 내 위치
                    # s: buf 내에서의 SOI 위치
                    abs_offset = base_offset + (buf_start_pos + s)

                    # JPEG 파일 저장
                    # 파일명 형식: worker_워커ID_off오프셋_idx인덱스.jpg
                    out_name = self.local_out_dir / f"worker_{self.worker_id}_off{abs_offset}_idx{file_idx}.jpg"
                    
                    with open(out_name, "wb") as out:
                        out.write(jpg_bytes)

                    # 발견 정보 저장
                    found.append({
                        "offset": abs_offset,        # 원본에서의 위치
                        "path": out_name,            # 로컬 저장 경로
                        "size": out_name.stat().st_size  # 파일 크기
                    })
                    file_idx += 1

                    # EOI 다음부터 계속 검색 (하나의 청크에 여러 JPEG 가능)
                    idx = e + 2

                # 다음 iteration 준비
                carry = next_carry
                carry_pos = next_carry_pos
                file_pos += len(data)
                
                # 진행률 업데이트
                progress.update(len(data))

        # 완료 메시지 출력
        progress.finish()
        return found

    # ========================================================================
    # 메인 워커 흐름 (Main Worker Flow)
    # ========================================================================

    def connect(self):
        """
        마스터 서버에 연결하고 자신의 정보를 전송합니다.
        
        [TCP 클라이언트 동작 순서]
        1. socket(): 소켓 생성
        2. connect(): 서버에 연결 요청
        3. 데이터 송수신
        
        [연결 성공 시]
        - 워커 ID 생성 (포트 번호 기반)
        - 워커 정보를 JSON으로 마스터에게 전송
        """
        print(f"\n[워커] 마스터 서버에 연결 중: {self.master_host}:{self.master_port}")
        
        # 소켓 생성
        # AF_INET: IPv4 주소 체계
        # SOCK_STREAM: TCP (연결 지향, 신뢰성 있는 통신)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # connect(): 서버에 연결
        # (host, port) 튜플로 연결 대상 지정
        # 연결 실패 시 ConnectionRefusedError 등 예외 발생
        self.socket.connect((self.master_host, self.master_port))

        # 워커 ID 생성
        # getsockname(): 이 소켓의 로컬 주소 반환 (IP, 포트)
        # 로컬 포트 번호를 ID로 사용 (같은 PC에서 여러 워커 구분 가능)
        self.worker_id = f"worker_{self.socket.getsockname()[1]}"
        
        # 마스터에게 워커 정보 전송
        hello = {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "status": "ready"
        }
        self.send_json(hello)
        
        print(f"[워커] 연결 성공! (ID: {self.worker_id})")

    def run_once(self):
        """
        마스터로부터 작업을 받아 한 번 처리합니다.
        
        [처리 흐름]
        1. 작업 정보(task) JSON 수신
        2. 청크 데이터를 임시 파일로 수신
        3. 임시 파일에서 JPEG 카빙
        4. 결과 개수 JSON 전송
        5. 각 JPEG 파일의 메타데이터와 데이터 전송
        
        [임시 디렉토리 사용]
        tempfile.TemporaryDirectory()를 사용하면
        with 블록이 끝날 때 자동으로 디렉토리와 내용이 삭제됩니다.
        대용량 청크가 디스크에 남지 않도록 합니다.
        """
        print("[워커] 작업 대기 중...")
        
        # 1. 작업 정보 수신
        task = self.recv_json()
        if not task:
            print("[워커] 작업 수신 실패")
            return

        # 작업 정보 추출
        task_id = task["task_id"]
        read_start = int(task["read_start"])  # 청크의 DD 이미지 내 시작 오프셋
        chunk_size = int(task["chunk_size"])  # 청크 크기

        print(f"\n[워커] 작업 수신!")
        print(f"  - Task ID: {task_id}")
        print(f"  - 청크 크기: {format_size(chunk_size)}")
        print(f"  - 오프셋: {read_start:,}")  # :, -> 천 단위 구분 기호
        print()

        # 2. 임시 디렉토리 생성 및 청크 수신
        # TemporaryDirectory: with 블록 끝나면 자동 삭제
        # prefix: 디렉토리 이름 접두사
        with tempfile.TemporaryDirectory(prefix=f"worker_{self.worker_id}_") as td:
            td = Path(td)  # 문자열 -> Path 객체 변환
            chunk_path = td / f"chunk_task{task_id}.bin"  # 청크 파일 경로

            # 청크 데이터 수신 (진행률 표시)
            received = self.recv_binary_stream_to_file_with_progress(chunk_path)
            
            if received <= 0:
                # 수신 실패 시 빈 결과 전송
                self.send_json({"task_id": task_id, "recovered_count": 0})
                return

            # 3. JPEG 카빙 (진행률 표시)
            print()  # 줄바꿈
            recovered = self.carve_jpeg_from_file_with_progress(
                chunk_path, 
                base_offset=read_start
            )

            print(f"\n[워커] 카빙 완료! {len(recovered)}개 JPEG 발견")

            # 4. 결과 개수 전송
            self.send_json({
                "task_id": task_id, 
                "recovered_count": len(recovered)
            })

            # 5. 각 JPEG 파일 전송
            if recovered:
                print(f"[워커] 마스터로 결과 전송 중...")
                
                for i, item in enumerate(recovered):
                    # 메타데이터 전송
                    meta = {
                        "offset": int(item["offset"]),  # 원본에서의 위치
                        "size": int(item["size"])       # 파일 크기
                    }
                    self.send_json(meta)
                    
                    # 파일 데이터 전송 (진행률 표시)
                    self.send_binary_stream_from_file_with_progress(
                        Path(item["path"]), 
                        i,              # 현재 파일 번호
                        len(recovered)  # 전체 파일 수
                    )
                
                print(f"[워커] 모든 결과 전송 완료!")
            
            print("\n[워커] 작업 완료!")
            
            # with 블록 끝 -> 임시 디렉토리 자동 삭제
            # 청크 파일은 삭제되지만, local_out_dir의 JPEG는 유지됨

    def close(self):
        """
        소켓 연결을 종료합니다.
        
        [예외 처리]
        이미 닫혀있거나 연결이 끊어진 경우에도
        에러 없이 조용히 처리합니다.
        """
        try:
            self.socket.close()
        except Exception:
            pass  # 에러 무시


# ============================================================================
# 메인 함수 (Entry Point)
# ============================================================================

def main():
    """
    프로그램의 진입점입니다.
    명령줄 인자를 파싱하고 워커를 실행합니다.
    
    [argparse 모듈]
    명령줄 인자를 쉽게 파싱하고 도움말을 자동 생성합니다.
    
    [사용 예시]
    python file_carving_worker.py 192.168.1.100 5000
    python file_carving_worker.py localhost 5000 --block 8
    python file_carving_worker.py --help  # 도움말
    """
    import argparse  # 명령줄 인자 파싱 라이브러리

    # ArgumentParser: 인자 파서 생성
    parser = argparse.ArgumentParser(
        description="파일 카빙 워커 (진행률 표시)"
    )
    
    # 위치 인자 (필수)
    parser.add_argument("host", help="마스터 IP/호스트")
    parser.add_argument("port", type=int, help="마스터 포트")
    
    # 옵션 인자 (선택)
    parser.add_argument("--block", "-b", type=int, default=4,
                        help="전송 블록 크기(MB), 기본 4MB")
    
    # 인자 파싱
    args = parser.parse_args()

    # 프로그램 시작 메시지
    print("=" * 50)
    print("  파일 카빙 워커 (진행률 표시 버전)")
    print("=" * 50)

    # 워커 객체 생성
    # MB -> 바이트 변환: * 1024 * 1024
    worker = FileCarvingWorker(
        args.host, 
        args.port, 
        stream_block_size=args.block * 1024 * 1024
    )
    
    try:
        # 마스터에 연결하고 작업 수행
        worker.connect()
        worker.run_once()
        
    except ConnectionRefusedError:
        # 마스터 서버가 실행 중이지 않거나 잘못된 주소
        print(f"[워커] 오류: 마스터 서버에 연결할 수 없습니다 ({args.host}:{args.port})")
        print("[워커] 마스터 서버가 실행 중인지 확인하세요.")
        
    except Exception as e:
        # 기타 예외
        print(f"[워커] 오류 발생: {e}")
        
    finally:
        # finally: 예외 발생 여부와 관계없이 항상 실행
        # 소켓 정리
        worker.close()


# ============================================================================
# 스크립트 실행 시 메인 함수 호출
# ============================================================================
# __name__ == "__main__": 이 파일이 직접 실행될 때만 True
# import될 때는 False이므로 main()이 자동 실행되지 않음

if __name__ == "__main__":
    main()
