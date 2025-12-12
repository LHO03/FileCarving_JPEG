#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
파일 카빙 분산 처리 시스템 - 마스터 서버 (Master Server)
================================================================================

[개요]
이 프로그램은 삭제된 JPEG 파일을 복구하기 위한 "분산 파일 카빙 시스템"의 마스터(서버) 역할을 합니다.
디지털 포렌식에서 파일 카빙(File Carving)이란 파일 시스템의 메타데이터 없이 
파일의 시그니처(고유 패턴)만을 이용해 삭제된 파일을 복구하는 기술입니다.

[동작 원리]
1. 마스터는 DD 이미지(디스크의 비트 단위 복사본)를 로드합니다.
2. 여러 워커(Worker) PC들이 네트워크로 연결됩니다.
3. DD 이미지를 워커 수만큼 분할(청크)하여 각 워커에게 전송합니다.
4. 각 워커는 받은 청크에서 JPEG 파일을 찾아 마스터로 전송합니다.
5. 마스터는 중복을 제거하고 최종 결과를 저장합니다.


[주요 특징]
- 병렬 처리: ThreadPoolExecutor를 사용해 여러 워커와 동시 통신
- 스트리밍 전송: 대용량 파일도 메모리 부족 없이 처리 (4MB 단위)
- 중복 제거: MD5 해시로 동일 파일 자동 제거
- 실시간 진행률: ANSI escape code로 멀티라인 진행률 표시

================================================================================
"""

# ============================================================================
# 라이브러리 임포트 (Import Statements)
# ============================================================================

import socket          # TCP/IP 네트워크 통신을 위한 표준 라이브러리
                       # 소켓(socket)은 네트워크 상에서 데이터를 주고받기 위한 끝점(endpoint)

import json            # JSON(JavaScript Object Notation) 형식의 데이터를 처리
                       # 딕셔너리 <-> 문자열 변환에 사용 (직렬화/역직렬화)

import struct          # 바이트(bytes)와 Python 데이터 타입 간 변환
                       # 네트워크 통신에서 데이터 길이를 바이트로 표현할 때 사용
                       # 예: struct.pack('!I', 100) -> 100을 4바이트 빅엔디안으로 변환

import hashlib         # 해시 함수 라이브러리 (MD5, SHA256 등)
                       # 파일의 고유 "지문"을 생성하여 중복 검사에 사용

import threading       # 멀티스레딩 지원 라이브러리
                       # Lock: 여러 스레드가 동시에 같은 데이터에 접근하는 것을 방지

import time            # 시간 관련 함수 (현재 시간, 대기 등)

import sys             # 시스템 관련 기능 (표준 출력, 플랫폼 정보 등)
                       # sys.stdout: 표준 출력 스트림
                       # sys.platform: 운영체제 확인 ('win32', 'linux' 등)

import os              # 운영체제 관련 기능 (파일/디렉토리 조작 등)

from pathlib import Path  # 파일 경로를 객체지향적으로 다루는 클래스
                          # 문자열보다 안전하고 편리한 경로 처리 가능
                          # 예: Path("folder") / "file.txt" -> "folder/file.txt"

from concurrent.futures import ThreadPoolExecutor, as_completed
# ThreadPoolExecutor: 스레드 풀을 관리하는 고수준 인터페이스
#   - 스레드 풀: 미리 생성해둔 스레드들을 재사용하는 방식
#   - 직접 Thread를 생성하는 것보다 효율적이고 관리가 쉬움
# as_completed: 여러 작업 중 완료된 순서대로 결과를 받아옴


# ============================================================================
# Windows 콘솔 ANSI 코드 활성화
# ============================================================================
# ANSI escape code는 터미널에서 커서 이동, 색상 변경 등을 제어하는 특수 문자열입니다.
# Windows 10 이상에서는 기본적으로 비활성화되어 있어서 활성화가 필요합니다.
# os.system('')을 호출하면 Windows에서 ANSI 코드가 활성화됩니다.

if sys.platform == 'win32':  # Windows 운영체제인 경우
    os.system('')            # 빈 명령 실행 -> ANSI 코드 활성화 (Windows 10+)


# ============================================================================
# 상수 정의 (Constants)
# ============================================================================

# JSON 메시지의 길이를 표현하는 형식
# "!I"는 struct 모듈의 포맷 문자열:
#   - "!" : 네트워크 바이트 순서 (빅 엔디안, Big-endian)
#           빅 엔디안: 큰 자릿수가 먼저 오는 방식 (사람이 숫자 쓰는 방식과 동일)
#   - "I" : unsigned int (부호 없는 정수, 4바이트, 0 ~ 4,294,967,295)
JSON_LEN_FMT = "!I"      # 4바이트 부호 없는 정수 (최대 약 4GB)
JSON_LEN_SIZE = 4        # 길이 필드 크기: 4바이트

# 바이너리 데이터의 길이를 표현하는 형식
#   - "Q" : unsigned long long (부호 없는 정수, 8바이트, 0 ~ 18,446,744,073,709,551,615)
# 30GB 같은 큰 청크도 표현할 수 있도록 8바이트 사용
BIN_LEN_FMT = "!Q"       # 8바이트 부호 없는 정수 (최대 약 16EB)
BIN_LEN_SIZE = 8         # 길이 필드 크기: 8바이트


# ============================================================================
# 유틸리티 함수 (Utility Functions)
# ============================================================================

def format_size(size_bytes):
    """
    바이트 단위의 크기를 사람이 읽기 쉬운 형식으로 변환합니다.
    
    [동작 원리]
    1024바이트 = 1KB, 1024KB = 1MB, ... 이런 식으로 단위를 올려가며
    1024보다 작아질 때까지 나눕니다.
    
    [매개변수]
    size_bytes (int/float): 바이트 단위의 크기
    
    [반환값]
    str: 변환된 문자열 (예: "1.50GB", "256.00MB")
    
    [사용 예시]
    >>> format_size(1536)
    '1.50KB'
    >>> format_size(1073741824)
    '1.00GB'
    """
    # 단위 목록: 바이트부터 테라바이트까지
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:  # 1024보다 작으면 현재 단위로 반환
            return f"{size_bytes:.2f}{unit}"  # 소수점 2자리까지 표시
        size_bytes /= 1024  # 1024로 나누어 다음 단위로
    # TB보다 크면 PB(페타바이트)로 표시
    return f"{size_bytes:.2f}PB"


def format_speed(bytes_per_sec):
    """
    초당 바이트 수를 전송 속도 형식으로 변환합니다.
    
    [매개변수]
    bytes_per_sec (float): 초당 전송 바이트 수
    
    [반환값]
    str: 속도 문자열 (예: "52.30MB/s")
    
    [사용 예시]
    >>> format_speed(52428800)
    '50.00MB/s'
    """
    return f"{format_size(bytes_per_sec)}/s"


def format_time(seconds):
    """
    초 단위의 시간을 읽기 쉬운 형식으로 변환합니다.
    
    [매개변수]
    seconds (float): 초 단위 시간
    
    [반환값]
    str: 변환된 시간 문자열 (예: "30초", "2.5분", "1.2시간")
    
    [사용 예시]
    >>> format_time(90)
    '1.5분'
    >>> format_time(3700)
    '1.0시간'
    """
    if seconds < 60:
        return f"{seconds:.0f}초"
    elif seconds < 3600:  # 60분 = 3600초
        return f"{seconds/60:.1f}분"
    else:
        return f"{seconds/3600:.1f}시간"


# ============================================================================
# MultiProgressDisplay 클래스
# ============================================================================

class MultiProgressDisplay:
    """
    여러 워커의 진행률을 터미널의 고정된 위치에서 동시에 업데이트하는 클래스입니다.
    
    [문제 상황]
    일반적인 print()는 항상 새 줄에 출력되므로, 여러 워커의 진행률을 
    동시에 보여주면 화면이 계속 스크롤됩니다.
    
    [해결 방법]
    ANSI escape code를 사용하여:
    1. 커서를 위로 이동 (\\033[nA: n줄 위로)
    2. 현재 줄 지우기 (\\033[K)
    3. 새 내용 출력
    
    이렇게 하면 같은 위치에서 내용이 업데이트되는 효과를 얻습니다.
    
    [ANSI Escape Code 설명]
    - \\033 또는 \\x1b : ESC 문자 (escape sequence 시작)
    - [nA : 커서를 n줄 위로 이동
    - [nB : 커서를 n줄 아래로 이동
    - [K  : 커서부터 줄 끝까지 지우기
    
    [멀티스레딩 고려사항]
    여러 스레드가 동시에 화면을 업데이트하면 출력이 섞일 수 있으므로,
    threading.Lock을 사용하여 한 번에 하나의 스레드만 출력하도록 합니다.
    
    [사용 예시]
    >>> display = MultiProgressDisplay(3)  # 3개 워커
    >>> display.init_display()
    >>> display.update(0, 5000, 'sending')  # 워커 0의 진행률 업데이트
    """
    
    def __init__(self, num_workers):
        """
        MultiProgressDisplay 생성자
        
        [매개변수]
        num_workers (int): 워커의 총 개수
        """
        self.num_workers = num_workers  # 워커 개수
        
        # threading.Lock(): 상호 배제(mutual exclusion) 락
        # 여러 스레드가 동시에 공유 자원에 접근하는 것을 방지
        # with self.lock: 구문으로 사용하면 자동으로 acquire/release 됨
        self.lock = threading.Lock()
        
        self.worker_states = {}  # 각 워커의 현재 상태를 저장하는 딕셔너리
        self.initialized = False  # 초기 화면 출력 여부
        self.bar_width = 20       # 프로그레스 바의 문자 폭
        
        # 각 워커의 초기 상태 설정
        for i in range(num_workers):
            self.worker_states[i] = {
                'phase': 'waiting',    # 현재 단계: waiting, sending, carving, receiving, done, error
                'current': 0,          # 현재까지 처리한 바이트 수
                'total': 0,            # 전체 처리해야 할 바이트 수
                'start_time': None,    # 전송 시작 시간 (속도 계산용)
                'address': '',         # 워커의 IP 주소
                'message': ''          # 추가 메시지
            }
    
    def init_display(self):
        """
        초기 화면 설정 - 워커 수만큼 빈 줄을 미리 출력합니다.
        
        [동작 원리]
        나중에 커서를 위로 올려서 덮어쓸 공간을 미리 확보합니다.
        이렇게 하지 않으면 커서를 위로 올릴 때 이전 출력을 덮어쓸 수 있습니다.
        """
        with self.lock:  # 락 획득 (다른 스레드가 동시에 접근 못함)
            if not self.initialized:
                # 워커 수만큼 초기 메시지 출력 (나중에 덮어쓸 공간 확보)
                for i in range(self.num_workers):
                    print(f"[워커 {i}] 초기화 중...")
                self.initialized = True
    
    def set_worker_info(self, worker_id, address, total):
        """
        워커의 기본 정보를 설정합니다.
        
        [매개변수]
        worker_id (int): 워커 번호 (0부터 시작)
        address (str): 워커의 IP 주소
        total (int): 전송할 총 바이트 수
        """
        with self.lock:
            self.worker_states[worker_id]['address'] = address
            self.worker_states[worker_id]['total'] = total
    
    def update(self, worker_id, current, phase='sending', message=None):
        """
        특정 워커의 진행률을 업데이트합니다.
        
        [매개변수]
        worker_id (int): 워커 번호
        current (int): 현재까지 처리한 바이트 수
        phase (str): 현재 단계 ('sending', 'carving', 'receiving', 'done', 'error')
        message (str, optional): 추가 메시지
        """
        with self.lock:
            state = self.worker_states[worker_id]
            state['current'] = current
            state['phase'] = phase
            
            # 전송 시작 시간 기록 (속도 계산을 위해)
            if state['start_time'] is None and phase == 'sending':
                state['start_time'] = time.time()
            
            if message:
                state['message'] = message
            
            # 화면 갱신
            self._render_all()
    
    def set_phase(self, worker_id, phase, message=None):
        """
        워커의 단계(phase)만 변경합니다.
        
        [매개변수]
        worker_id (int): 워커 번호
        phase (str): 변경할 단계
        message (str, optional): 표시할 메시지
        """
        with self.lock:
            self.worker_states[worker_id]['phase'] = phase
            if message:
                self.worker_states[worker_id]['message'] = message
            self._render_all()
    
    def _render_all(self):
        """
        모든 워커의 상태를 화면에 렌더링합니다.
        
        [동작 원리]
        1. ANSI 코드로 커서를 워커 수만큼 위로 이동
        2. 각 워커의 상태를 한 줄씩 출력 (이전 내용 덮어쓰기)
        3. flush()로 버퍼를 즉시 출력
        
        [주의사항]
        이 메서드는 lock이 이미 획득된 상태에서만 호출해야 합니다.
        (private 메서드이므로 _ 접두사 사용)
        """
        # 커서를 워커 수만큼 위로 이동
        # \033[nA : n줄 위로 이동하는 ANSI escape code
        sys.stdout.write(f"\033[{self.num_workers}A")
        
        # 각 워커의 상태를 출력
        for i in range(self.num_workers):
            line = self._format_worker_line(i, self.worker_states[i])
            # \033[K : 현재 커서 위치부터 줄 끝까지 지우기
            sys.stdout.write(f"\033[K{line}\n")
        
        # 버퍼를 즉시 출력 (버퍼링 없이 바로 화면에 표시)
        sys.stdout.flush()
    
    def _format_worker_line(self, worker_id, state):
        """
        워커 한 줄의 출력 문자열을 생성합니다.
        
        [매개변수]
        worker_id (int): 워커 번호
        state (dict): 워커의 현재 상태 딕셔너리
        
        [반환값]
        str: 포맷팅된 문자열 (예: "[워커 0] 192.168.1.100   | ████████░░░░ | 65.3% | ...")
        """
        phase = state['phase']
        
        # IP 주소를 15자로 맞춤 (왼쪽 정렬, 빈 공간은 공백으로)
        # ljust(15): 문자열을 15자로 맞추고 오른쪽에 공백 추가
        addr = state['address'][:15].ljust(15) if state['address'] else '???'.ljust(15)
        
        # 단계별로 다른 형식 출력
        if phase == 'waiting':
            # 대기 상태
            return f"[워커 {worker_id}] {addr} | 대기 중..."
        
        elif phase == 'sending':
            # 청크 전송 중 - 프로그레스 바와 상세 정보 표시
            current = state['current']
            total = state['total']
            
            # 백분율 계산 (0으로 나누기 방지)
            percent = (current / total * 100) if total > 0 else 0
            
            # 프로그레스 바 생성
            # filled: 채워진 블록 수
            filled = int(self.bar_width * current / total) if total > 0 else 0
            # █: 채워진 부분, ░: 빈 부분
            bar = '█' * filled + '░' * (self.bar_width - filled)
            
            # 전송 속도 계산
            elapsed = time.time() - state['start_time'] if state['start_time'] else 0
            speed = current / elapsed if elapsed > 0 else 0
            
            # 남은 시간(ETA) 계산
            if speed > 0 and current < total:
                eta = format_time((total - current) / speed)
            else:
                eta = "--"
            
            # f-string 포맷팅:
            # {:5.1f} : 전체 5자리, 소수점 1자리
            # :>8 : 오른쪽 정렬, 8자리
            return (f"[워커 {worker_id}] {addr} | {bar} | {percent:5.1f}% | "
                   f"{format_size(current):>8}/{format_size(total):>8} | "
                   f"{format_speed(speed):>10} | 남은: {eta}")
        
        elif phase == 'carving':
            # 카빙(파일 검색) 진행 중
            # 뒤에 공백을 많이 넣어서 이전 긴 문자열을 덮어씀
            return f"[워커 {worker_id}] {addr} | 카빙 진행 중...                                              "
        
        elif phase == 'receiving':
            # 결과 수신 중
            return f"[워커 {worker_id}] {addr} | 결과 수신 중... {state['message']}                          "
        
        elif phase == 'done':
            # 완료
            return f"[워커 {worker_id}] {addr} | 완료! {state['message']}                                     "
        
        elif phase == 'error':
            # 오류 발생
            return f"[워커 {worker_id}] {addr} | 오류: {state['message'][:40]}                               "
        
        else:
            # 기타 상태
            return f"[워커 {worker_id}] {addr} | {state.get('message', '')}                                   "
    
    def finish(self):
        """
        진행률 표시를 종료합니다.
        현재는 특별한 동작 없이 락만 획득했다가 해제합니다.
        """
        with self.lock:
            pass  # 커서가 이미 맨 아래에 있으므로 추가 작업 불필요


# ============================================================================
# FileCarvingMaster 클래스
# ============================================================================

class FileCarvingMaster:
    """
    파일 카빙 분산 처리 시스템의 마스터(서버) 클래스입니다.
    
    [역할]
    1. DD 이미지 파일을 로드하고 관리
    2. 워커(클라이언트) PC들의 연결을 수락
    3. DD 이미지를 청크로 분할하여 워커들에게 전송
    4. 워커들이 찾은 JPEG 파일들을 수신하고 저장
    5. 중복 파일 제거 및 최종 결과 정리
    
    [네트워크 구조]
    - 마스터는 서버 역할: 특정 포트에서 연결 대기
    - 워커는 클라이언트 역할: 마스터에 연결 요청
    - TCP/IP 소켓 통신 사용 (신뢰성 있는 연결 지향 통신)
    
    [통신 프로토콜]
    모든 메시지는 [길이][데이터] 형식으로 전송:
    - JSON 메시지: [4바이트 길이][JSON 문자열]
    - 바이너리 데이터: [8바이트 길이][바이너리 데이터]
    
    [속성(Attributes)]
    - port: 서버 포트 번호
    - overlap_size: 청크 간 오버랩 크기 (경계에 걸친 파일 복구용)
    - stream_block_size: 스트리밍 시 한 번에 읽는 블록 크기
    - workers: 연결된 워커 정보 리스트
    - dd_image_path: DD 이미지 파일 경로
    - image_size: DD 이미지 크기
    - results_dir: 복구된 파일 저장 디렉토리
    - file_hashes: 중복 검사용 해시 집합
    - recovered_files: 복구된 파일 정보 리스트
    """
    
    def __init__(self, port=5000, overlap_size=1 * 1024 * 1024, stream_block_size=4 * 1024 * 1024):
        """
        FileCarvingMaster 생성자
        
        [매개변수]
        port (int): 서버가 사용할 포트 번호 (기본값: 5000)
            - 포트: 하나의 IP에서 여러 서비스를 구분하는 번호
            - 0-1023: Well-known ports (시스템 예약)
            - 1024-49151: Registered ports
            - 49152-65535: Dynamic/Private ports
            
        overlap_size (int): 청크 간 오버랩 크기 (기본값: 1MB)
            - 청크 경계에 걸친 JPEG 파일을 복구하기 위함
            - 예: JPEG가 청크 0 끝과 청크 1 시작에 걸쳐있는 경우
            
        stream_block_size (int): 스트리밍 블록 크기 (기본값: 4MB)
            - 대용량 데이터를 한 번에 메모리에 올리지 않고
              작은 블록 단위로 읽어서 전송
        """
        self.port = port
        self.overlap_size = overlap_size
        self.stream_block_size = stream_block_size

        self.workers = []          # 연결된 워커들의 정보 리스트
        self.dd_image_path = None  # DD 이미지 파일 경로 (Path 객체)
        self.image_size = 0        # DD 이미지 전체 크기 (바이트)

        # 복구된 파일을 저장할 디렉토리
        # Path 객체의 mkdir(): 디렉토리 생성
        # exist_ok=True: 이미 존재해도 에러 발생하지 않음
        self.results_dir = Path("recovered_files")
        self.results_dir.mkdir(exist_ok=True)

        # set(): 중복을 허용하지 않는 집합 자료구조
        # 파일의 MD5 해시를 저장하여 중복 파일 검사에 사용
        self.file_hashes = set()
        
        # 멀티스레딩 환경에서 공유 자원 보호를 위한 락
        self.lock = threading.Lock()
        
        self.recovered_files = []  # 복구된 파일 정보 리스트
        
        self.progress_display = None  # 진행률 표시 객체

    def load_dd_image(self, image_path: str) -> bool:
        """
        DD 이미지 파일을 로드합니다.
        
        [DD 이미지란?]
        DD(Disk Dump)는 디스크의 비트 단위 완전한 복사본입니다.
        Linux의 dd 명령어나 FTK Imager 등으로 생성할 수 있습니다.
        파일 시스템 구조와 관계없이 디스크의 모든 섹터를 그대로 복사합니다.
        
        [매개변수]
        image_path (str): DD 이미지 파일의 경로
        
        [반환값]
        bool: 로드 성공 여부
        
        [사용 예시]
        >>> master = FileCarvingMaster()
        >>> if master.load_dd_image("usb_image.dd"):
        ...     print("로드 성공!")
        """
        # Path 객체 생성 (문자열보다 경로 처리에 편리)
        p = Path(image_path)
        
        # exists(): 파일/디렉토리 존재 여부 확인
        if not p.exists():
            print(f"[마스터] 오류: 이미지 파일을 찾을 수 없음: {image_path}")
            return False

        self.dd_image_path = p
        
        # stat(): 파일의 메타데이터(크기, 수정시간 등) 반환
        # st_size: 파일 크기 (바이트)
        self.image_size = p.stat().st_size

        print(f"[마스터] DD 이미지 로드: {p}")
        # :, -> 천 단위 구분 기호 추가 (예: 1,234,567)
        print(f"[마스터] 이미지 크기: {self.image_size:,} bytes ({format_size(self.image_size)})")
        return True

    # ========================================================================
    # 네트워크 통신 헬퍼 메서드 (Networking Helper Methods)
    # ========================================================================

    def _recv_exact(self, sock: socket.socket, size: int) -> bytes:
        """
        소켓에서 정확히 지정된 크기만큼 데이터를 수신합니다.
        
        [왜 이 함수가 필요한가?]
        TCP 소켓의 recv()는 요청한 크기보다 적은 데이터를 반환할 수 있습니다.
        예를 들어 1000바이트를 요청해도 500바이트만 올 수 있습니다.
        이 함수는 정확히 요청한 크기가 올 때까지 반복해서 recv()를 호출합니다.
        
        [매개변수]
        sock (socket.socket): 데이터를 받을 소켓
        size (int): 받을 데이터의 정확한 크기 (바이트)
        
        [반환값]
        bytes: 수신한 데이터 (정확히 size 바이트)
               연결이 끊어지면 빈 bytes 반환
        
        [동작 원리]
        1. bytearray에 받은 데이터를 계속 추가
        2. 원하는 크기가 될 때까지 반복
        3. 연결이 끊어지면 (recv가 빈 값 반환) 빈 bytes 반환
        """
        # bytearray: 수정 가능한 바이트 시퀀스
        # bytes는 불변(immutable)이지만 bytearray는 가변(mutable)
        buf = bytearray()
        
        while len(buf) < size:
            # recv(): 소켓에서 데이터 수신
            # min(65536, size - len(buf)): 남은 크기와 64KB 중 작은 값
            # 64KB(65536)는 일반적인 TCP 버퍼 크기
            chunk = sock.recv(min(65536, size - len(buf)))
            
            # recv()가 빈 값을 반환하면 연결이 끊어진 것
            if not chunk:
                return b""  # 빈 bytes 반환
            
            # extend(): bytearray에 데이터 추가
            buf.extend(chunk)
        
        # bytes로 변환하여 반환
        return bytes(buf)

    def send_json(self, sock: socket.socket, obj: dict) -> None:
        """
        딕셔너리를 JSON 형식으로 소켓을 통해 전송합니다.
        
        [프로토콜 형식]
        [4바이트: JSON 길이][JSON 문자열]
        
        [매개변수]
        sock (socket.socket): 데이터를 보낼 소켓
        obj (dict): 전송할 딕셔너리
        
        [동작 순서]
        1. 딕셔너리를 JSON 문자열로 변환 (json.dumps)
        2. JSON 문자열을 UTF-8 바이트로 인코딩
        3. 길이를 4바이트로 패킹하여 먼저 전송
        4. JSON 바이트 데이터 전송
        
        [사용 예시]
        >>> master.send_json(sock, {"status": "ok", "count": 5})
        """
        # json.dumps(): 딕셔너리 -> JSON 문자열
        # encode("utf-8"): 문자열 -> 바이트
        payload = json.dumps(obj).encode("utf-8")
        
        # struct.pack(): Python 값을 바이트로 변환
        # "!I": 네트워크 바이트 순서, unsigned int (4바이트)
        # 길이 정보를 먼저 전송
        sock.sendall(struct.pack(JSON_LEN_FMT, len(payload)))
        
        # sendall(): 모든 데이터가 전송될 때까지 대기
        # send()와 달리 부분 전송 걱정 없음
        sock.sendall(payload)

    def recv_json(self, sock: socket.socket):
        """
        소켓에서 JSON 데이터를 수신하여 딕셔너리로 반환합니다.
        
        [프로토콜 형식]
        [4바이트: JSON 길이][JSON 문자열]
        
        [매개변수]
        sock (socket.socket): 데이터를 받을 소켓
        
        [반환값]
        dict: 수신한 JSON을 파싱한 딕셔너리
              실패 시 None 반환
        
        [동작 순서]
        1. 먼저 4바이트(길이 정보) 수신
        2. 길이만큼 JSON 데이터 수신
        3. JSON 파싱하여 딕셔너리로 반환
        """
        # 1. 길이 정보 수신 (4바이트)
        size_b = self._recv_exact(sock, JSON_LEN_SIZE)
        if not size_b:
            return None
        
        # struct.unpack(): 바이트 -> Python 값
        # [0]: unpack은 튜플을 반환하므로 첫 번째 요소 추출
        size = struct.unpack(JSON_LEN_FMT, size_b)[0]
        
        # 2. JSON 데이터 수신
        payload = self._recv_exact(sock, size)
        if not payload:
            return None
        
        # 3. JSON 파싱
        # decode("utf-8"): 바이트 -> 문자열
        # json.loads(): JSON 문자열 -> 딕셔너리
        return json.loads(payload.decode("utf-8"))

    def send_binary_stream_with_progress(self, sock: socket.socket, file_path: Path, 
                                         start: int, end: int, worker_id: int) -> None:
        """
        파일의 특정 구간을 진행률을 표시하면서 스트리밍 전송합니다.
        
        [스트리밍이란?]
        대용량 데이터를 한 번에 메모리에 올리지 않고,
        작은 청크 단위로 읽어서 바로 전송하는 방식입니다.
        30GB 파일도 4MB만 메모리에 올려서 처리할 수 있습니다.
        
        [프로토콜 형식]
        [8바이트: 데이터 길이][데이터 스트림...]
        
        [매개변수]
        sock (socket.socket): 데이터를 보낼 소켓
        file_path (Path): 읽을 파일 경로
        start (int): 읽기 시작 위치 (바이트 오프셋)
        end (int): 읽기 종료 위치 (바이트 오프셋, 미포함)
        worker_id (int): 워커 번호 (진행률 표시용)
        
        [동작 순서]
        1. 먼저 전송할 총 크기를 8바이트로 전송
        2. 파일에서 블록 단위로 읽어서 소켓으로 전송
        3. 주기적으로 진행률 업데이트
        """
        total = end - start  # 전송할 총 바이트 수
        if total < 0:
            raise ValueError("Invalid range")

        # 8바이트 길이 정보 전송 ("!Q": unsigned long long)
        sock.sendall(struct.pack(BIN_LEN_FMT, total))

        # 파일 열기 ("rb": read binary, 바이너리 읽기 모드)
        with open(file_path, "rb") as f:
            # seek(): 파일 포인터를 특정 위치로 이동
            f.seek(start)
            
            remaining = total  # 남은 전송량
            sent = 0           # 전송 완료량
            last_update = 0    # 마지막 진행률 업데이트 시간
            
            while remaining > 0:
                # 읽을 크기 결정 (블록 크기와 남은 크기 중 작은 값)
                to_read = min(self.stream_block_size, remaining)
                
                # 파일에서 데이터 읽기
                chunk = f.read(to_read)
                if not chunk:
                    raise IOError("Unexpected EOF while reading DD image")
                
                # 소켓으로 전송
                sock.sendall(chunk)
                
                remaining -= len(chunk)
                sent += len(chunk)
                
                # 진행률 업데이트 (0.3초마다 또는 완료 시)
                now = time.time()
                if now - last_update >= 0.3 or remaining == 0:
                    self.progress_display.update(worker_id, sent, 'sending')
                    last_update = now

    def recv_binary_stream_to_file(self, sock: socket.socket, out_path: Path) -> int:
        """
        소켓에서 바이너리 데이터를 수신하여 파일로 저장합니다.
        
        [프로토콜 형식]
        [8바이트: 데이터 길이][데이터 스트림...]
        
        [매개변수]
        sock (socket.socket): 데이터를 받을 소켓
        out_path (Path): 저장할 파일 경로
        
        [반환값]
        int: 수신한 총 바이트 수 (실패 시 -1)
        
        [동작 순서]
        1. 8바이트 길이 정보 수신
        2. 길이만큼 데이터를 수신하면서 파일에 기록
        """
        # 길이 정보 수신
        size_b = self._recv_exact(sock, BIN_LEN_SIZE)
        if not size_b:
            return -1
        total = struct.unpack(BIN_LEN_FMT, size_b)[0]

        remaining = total
        
        # 부모 디렉토리가 없으면 생성
        # parents=True: 중간 디렉토리도 모두 생성
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # 파일에 기록 ("wb": write binary)
        with open(out_path, "wb") as f:
            while remaining > 0:
                # 소켓에서 데이터 수신
                chunk = sock.recv(min(65536, remaining))
                if not chunk:
                    raise IOError("Socket closed while receiving binary")
                
                # 파일에 기록
                f.write(chunk)
                remaining -= len(chunk)

        return total

    # ========================================================================
    # 서버 메인 로직 (Server Main Logic)
    # ========================================================================

    def start_server(self) -> None:
        """
        마스터 서버를 시작하고 워커 연결을 기다립니다.
        
        [TCP 서버 동작 순서]
        1. socket() - 소켓 생성
        2. setsockopt() - 소켓 옵션 설정
        3. bind() - IP와 포트에 소켓 바인딩
        4. listen() - 연결 대기 상태로 전환
        5. accept() - 클라이언트 연결 수락 (블로킹)
        
        [소켓 옵션]
        SO_REUSEADDR: 서버 재시작 시 "Address already in use" 에러 방지
                     이전 연결의 TIME_WAIT 상태를 무시하고 바로 바인딩 가능
        """
        # 소켓 생성
        # AF_INET: IPv4 주소 체계
        # SOCK_STREAM: TCP (연결 지향, 신뢰성 있는 스트림)
        # SOCK_DGRAM: UDP (비연결, 신뢰성 없는 데이터그램)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # 소켓 옵션 설정
        # SOL_SOCKET: 소켓 레벨 옵션
        # SO_REUSEADDR: 주소 재사용 허용 (1 = 활성화)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # 바인딩: 소켓을 특정 IP와 포트에 연결
        # "0.0.0.0": 모든 네트워크 인터페이스에서 연결 수락
        # "127.0.0.1": localhost만 (외부 연결 불가)
        server_socket.bind(("0.0.0.0", self.port))
        
        # 연결 대기 시작
        # 10: 대기 큐의 최대 크기 (동시에 대기 가능한 연결 수)
        server_socket.listen(10)

        print(f"\n[마스터] 서버 시작 - 포트: {self.port}")
        print(f"[마스터] 다른 PC에서 연결하려면: python file_carving_worker.py <이 PC의 IP> {self.port}")
        print("[마스터] 워커 연결 대기 중... (30초 타임아웃)")
        
        # 타임아웃 설정: accept()가 30초간 연결이 없으면 예외 발생
        server_socket.settimeout(30)

        try:
            while True:
                try:
                    # accept(): 클라이언트 연결 수락 (블로킹)
                    # 반환값: (클라이언트 소켓, (IP, 포트) 튜플)
                    client_socket, addr = server_socket.accept()
                    print(f"[마스터] 워커 연결됨: {addr}")
                    
                    # 워커로부터 정보 수신 (워커 ID, 호스트명 등)
                    info = self.recv_json(client_socket)
                    print(f"[마스터] 워커 정보: {info}")

                    # 워커 정보 저장
                    self.workers.append({
                        "socket": client_socket,
                        "address": addr,
                        "info": info
                    })
                    
                except socket.timeout:
                    # 타임아웃 발생
                    if self.workers:
                        # 워커가 1개 이상 연결되었으면 작업 시작
                        print(f"\n[마스터] 총 {len(self.workers)}개 워커 연결 완료\n")
                        break
                    else:
                        # 워커가 없으면 종료
                        print("[마스터] 워커 연결 없음. 종료합니다.")
                        return
        finally:
            # finally: try 블록이 어떻게 끝나든 실행됨
            # 서버 소켓 닫기 (클라이언트 소켓은 유지)
            server_socket.close()

        # 작업 분배 및 결과 수집
        self.distribute_and_collect_parallel()
        
        # 결과 요약 출력
        self.print_summary()

    def process_worker(self, worker_id: int, worker: dict, task: dict, 
                       read_start: int, read_end: int) -> dict:
        """
        개별 워커와의 통신을 처리합니다 (별도 스레드에서 실행).
        
        [동작 순서]
        1. 작업 정보(task) JSON 전송
        2. 청크 데이터 스트리밍 전송
        3. 워커의 카빙 완료 대기
        4. 복구된 파일들 수신
        
        [매개변수]
        worker_id (int): 워커 번호 (0부터 시작)
        worker (dict): 워커 정보 (소켓, 주소 등)
        task (dict): 작업 정보 (오프셋, 크기 등)
        read_start (int): 읽기 시작 오프셋
        read_end (int): 읽기 종료 오프셋
        
        [반환값]
        dict: 처리 결과 정보
            - worker_id: 워커 번호
            - address: 워커 IP
            - success: 성공 여부
            - recovered_count: 복구된 파일 수
            - error: 에러 메시지 (있는 경우)
        """
        sock = worker["socket"]
        addr = worker["address"][0]  # IP 주소만 추출
        chunk_size = read_end - read_start
        
        # 결과 정보 초기화
        result_info = {
            "worker_id": worker_id,
            "address": addr,
            "success": False,
            "recovered_count": 0,
            "error": None
        }

        try:
            # 진행률 표시를 위한 워커 정보 설정
            self.progress_display.set_worker_info(worker_id, addr, chunk_size)
            
            # 1) 작업 정보(task) JSON 전송
            self.send_json(sock, task)
            
            # 2) 청크 데이터 스트리밍 전송 (진행률 표시)
            self.send_binary_stream_with_progress(
                sock, self.dd_image_path, read_start, read_end, worker_id
            )
            
            # 3) 카빙 진행 중 상태로 변경
            self.progress_display.set_phase(worker_id, 'carving', '')

            # 4) 결과 수신
            recovered_count = self.receive_results(sock, worker_id)
            
            result_info["success"] = True
            result_info["recovered_count"] = recovered_count
            self.progress_display.set_phase(worker_id, 'done', f'{recovered_count}개 파일 복구')

        except Exception as e:
            # 예외 발생 시 에러 정보 저장
            result_info["error"] = str(e)
            self.progress_display.set_phase(worker_id, 'error', str(e)[:30])
            
        finally:
            # finally: 예외 발생 여부와 관계없이 항상 실행
            # 소켓 닫기
            try:
                sock.close()
            except Exception:
                pass  # 이미 닫혀있어도 무시

        return result_info

    def distribute_and_collect_parallel(self) -> None:
        """
        모든 워커에게 병렬로 작업을 분배하고 결과를 수집합니다.
        
        [병렬 처리의 장점]
        순차 처리: 워커0 전송 → 대기 → 워커1 전송 → 대기 → ...
        병렬 처리: 워커0 전송 ─┬─ 동시에 ─┬─ 빠른 완료!
                  워커1 전송 ─┘          └─
        
        [ThreadPoolExecutor 사용법]
        스레드 풀: 미리 생성된 스레드들을 재사용하는 패턴
        - submit(함수, 인자들): 작업 제출, Future 객체 반환
        - as_completed(futures): 완료된 순서대로 Future 반환
        
        [오버랩(Overlap) 처리]
        JPEG 파일이 청크 경계에 걸쳐있을 수 있습니다.
        예: 청크0의 끝 부분에서 JPEG가 시작되어 청크1에서 끝나는 경우
        
        이를 해결하기 위해 각 청크에 오버랩 영역을 추가합니다:
        청크0: [===================][오버랩]
        청크1:           [오버랩][===================]
        
        워커는 자신의 "담당 영역"에서 시작하는 파일만 처리하여 중복을 방지합니다.
        """
        if not self.workers or not self.dd_image_path:
            return

        n = len(self.workers)  # 워커 수
        base = self.image_size // n  # 기본 청크 크기 (정수 나눗셈)

        print("[마스터] 병렬 작업 분배 시작")
        print(f"  - 전체 크기: {format_size(self.image_size)}")
        print(f"  - 워커 수: {n}")
        print(f"  - 청크 크기: ~{format_size(base)}")
        print(f"  - 오버랩: {format_size(self.overlap_size)}")
        print()

        # 각 워커별 작업 정보 준비
        tasks_args = []  # (worker_id, worker, task, read_start, read_end) 튜플 리스트
        
        for i, w in enumerate(self.workers):
            # 담당 영역 계산 (실제 책임지는 범위)
            start_offset = i * base
            # 마지막 워커는 끝까지 담당 (나머지 처리)
            end_offset = (i + 1) * base if i < n - 1 else self.image_size

            # 읽기 영역 계산 (오버랩 포함)
            # 첫 번째 워커는 앞쪽 오버랩 없음
            read_start = 0 if i == 0 else max(0, start_offset - self.overlap_size // 2)
            # 마지막 워커는 뒤쪽 오버랩 없음
            read_end = self.image_size if i == n - 1 else min(self.image_size, end_offset + self.overlap_size // 2)
            chunk_size = read_end - read_start

            # 작업 정보 딕셔너리
            task = {
                "task_id": i,
                "start_offset": start_offset,  # 담당 시작
                "end_offset": end_offset,      # 담당 끝
                "read_start": read_start,      # 실제 읽기 시작 (오버랩 포함)
                "read_end": read_end,          # 실제 읽기 끝 (오버랩 포함)
                "chunk_size": chunk_size,
                "overlap_size": self.overlap_size,
            }
            tasks_args.append((i, w, task, read_start, read_end))

        # 진행률 디스플레이 초기화
        self.progress_display = MultiProgressDisplay(n)
        
        # 병렬 실행 시작
        start_time = time.time()
        print("=" * 80)
        print("[마스터] 모든 워커에게 동시 전송 시작!")
        print("=" * 80)
        
        # 진행률 표시 초기화 (빈 줄 출력)
        self.progress_display.init_display()

        # ThreadPoolExecutor: 스레드 풀 생성
        # max_workers=n: 최대 n개 스레드 (워커 수만큼)
        # with 문: 컨텍스트 매니저로 자동 정리
        with ThreadPoolExecutor(max_workers=n) as executor:
            # submit()으로 각 워커 처리 작업 제출
            # futures 딕셔너리: {Future 객체: worker_id}
            futures = {
                executor.submit(self.process_worker, *args): args[0]
                for args in tasks_args
            }
            # *args: 튜플 언패킹 (i, w, task, read_start, read_end)
            
            # as_completed(): 완료된 순서대로 Future 반환
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    # result(): 작업 결과 가져오기 (예외 발생 시 재발생)
                    result = future.result()
                except Exception as e:
                    self.progress_display.set_phase(worker_id, 'error', str(e)[:30])

        self.progress_display.finish()
        
        # 총 소요 시간 출력
        elapsed = time.time() - start_time
        print()
        print("=" * 80)
        print(f"[마스터] 모든 워커 처리 완료! (총 소요 시간: {format_time(elapsed)})")
        print("=" * 80)

    def receive_results(self, sock: socket.socket, worker_id: int) -> int:
        """
        워커로부터 복구된 파일들을 수신합니다.
        
        [프로토콜]
        1. 워커가 결과 개수 JSON 전송: {"recovered_count": n}
        2. n번 반복:
           - 파일 메타데이터 JSON 수신: {"offset": ..., "size": ...}
           - 파일 데이터 바이너리 수신
        
        [중복 제거]
        MD5 해시를 계산하여 이미 받은 파일인지 확인합니다.
        오버랩 영역으로 인해 같은 파일이 여러 워커에서 발견될 수 있습니다.
        
        [매개변수]
        sock (socket.socket): 워커와 연결된 소켓
        worker_id (int): 워커 번호
        
        [반환값]
        int: 수신한 파일 수 (중복 포함)
        """
        # 결과 개수 수신
        result = self.recv_json(sock)
        if not result:
            return 0

        recovered_count = int(result.get("recovered_count", 0))
        
        if recovered_count > 0:
            self.progress_display.set_phase(worker_id, 'receiving', f'0/{recovered_count}')

        for i in range(recovered_count):
            # 파일 메타데이터 수신
            meta = self.recv_json(sock)
            if not meta:
                break

            offset = int(meta.get("offset", -1))

            # 임시 파일로 저장
            tmp_path = self.results_dir / f"__tmp_worker{worker_id}_off{offset}.jpg"
            received = self.recv_binary_stream_to_file(sock, tmp_path)

            # 진행률 업데이트
            self.progress_display.set_phase(worker_id, 'receiving', f'{i+1}/{recovered_count}')

            if received <= 0:
                # 수신 실패 시 임시 파일 삭제
                try:
                    # unlink(): 파일 삭제
                    # missing_ok=True: 파일이 없어도 에러 발생하지 않음
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            # MD5 해시 계산 (중복 검사용)
            # hashlib.md5(): MD5 해시 객체 생성
            md5 = hashlib.md5()
            with open(tmp_path, "rb") as rf:
                # iter(함수, 종료값): 함수를 반복 호출하여 종료값이 나올 때까지 반복
                # lambda: rf.read(1024*1024): 1MB씩 읽는 익명 함수
                # b"": 빈 바이트가 나오면 종료 (파일 끝)
                for chunk in iter(lambda: rf.read(1024 * 1024), b""):
                    # update(): 해시에 데이터 추가
                    md5.update(chunk)
            # hexdigest(): 16진수 문자열로 해시값 반환
            file_hash = md5.hexdigest()

            # 락을 사용하여 공유 자원 보호
            with self.lock:
                # 중복 검사
                if file_hash in self.file_hashes:
                    # 중복 파일 삭제
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue

                # 해시 집합에 추가
                self.file_hashes.add(file_hash)

                # 최종 파일명 생성
                # 오프셋과 해시 앞 8자리를 포함하여 고유한 이름 생성
                final_name = f"recovered_{offset}_{file_hash[:8]}.jpg"
                final_path = self.results_dir / final_name
                
                # replace(): 파일 이동 (임시 파일 -> 최종 파일)
                tmp_path.replace(final_path)

                # 복구된 파일 정보 저장
                self.recovered_files.append({
                    "filename": final_name,
                    "size": received,
                    "offset": offset,
                    "hash": file_hash,
                    "worker_id": worker_id,
                })

        return recovered_count

    def print_summary(self) -> None:
        """
        파일 카빙 결과를 요약하여 출력합니다.
        복구된 파일 수, 총 크기, 워커별 통계 등을 보여줍니다.
        """
        print("\n" + "=" * 80)
        print("  파일 카빙 완료 - 결과 요약")
        print("=" * 80)
        print(f"  총 복구 파일: {len(self.recovered_files)}개 (중복 제거됨)")
        
        if not self.recovered_files:
            return

        # sum(): 리스트의 합계 계산
        # 제너레이터 표현식: (x["size"] for x in self.recovered_files)
        total = sum(x["size"] for x in self.recovered_files)
        print(f"  총 복구 크기: {format_size(total)}")
        
        # resolve(): 절대 경로로 변환
        print(f"  저장 위치: {self.results_dir.resolve()}")

        # 워커별 통계 집계
        worker_stats = {}
        for f in self.recovered_files:
            wid = f["worker_id"]
            if wid not in worker_stats:
                worker_stats[wid] = {"count": 0, "size": 0}
            worker_stats[wid]["count"] += 1
            worker_stats[wid]["size"] += f["size"]

        print("\n  워커별 복구 현황:")
        # sorted(): 정렬된 리스트 반환
        for wid in sorted(worker_stats.keys()):
            stats = worker_stats[wid]
            print(f"    - 워커 {wid}: {stats['count']}개 파일, {format_size(stats['size'])}")

        print("\n  복구된 파일 목록:")
        for f in self.recovered_files:
            print(f"    - {f['filename']} ({format_size(f['size'])}, 워커 {f['worker_id']})")
        print("=" * 80)


# ============================================================================
# 메인 함수 (Entry Point)
# ============================================================================

def main():
    """
    프로그램의 진입점 (Entry Point)입니다.
    명령줄 인자를 파싱하고 마스터 서버를 실행합니다.
    
    [argparse 모듈]
    명령줄 인자를 쉽게 파싱할 수 있는 표준 라이브러리입니다.
    자동으로 --help 옵션을 생성하고, 인자 유효성 검사도 해줍니다.
    
    [사용 예시]
    python file_carving_master.py usb_image.dd
    python file_carving_master.py usb_image.dd --port 5000 --overlap 2
    python file_carving_master.py --help  # 도움말 출력
    """
    import argparse  # 명령줄 인자 파싱 라이브러리

    # ArgumentParser: 인자 파서 생성
    # description: --help 시 표시될 프로그램 설명
    parser = argparse.ArgumentParser(
        description="파일 카빙 마스터 서버 (병렬 + 고정 멀티라인 진행률)"
    )
    
    # add_argument(): 인자 추가
    # 위치 인자 (필수): 이름만 지정
    parser.add_argument("image", help="DD 이미지 파일 경로")
    
    # 옵션 인자 (선택): --이름 또는 -약자
    # type: 인자의 타입 (기본값: str)
    # default: 기본값
    # help: 도움말 설명
    parser.add_argument("--port", "-p", type=int, default=5000,
                        help="서버 포트 (기본값: 5000)")
    parser.add_argument("--overlap", "-o", type=int, default=1,
                        help="오버랩 크기(MB), 기본 1MB")
    parser.add_argument("--block", "-b", type=int, default=4,
                        help="스트리밍 블록 크기(MB), 기본 4MB")
    parser.add_argument("--output", "-O", type=str, default="recovered_files",
                        help="출력 디렉토리 (기본값: recovered_files)")
    
    # parse_args(): 명령줄 인자 파싱
    # 반환값: 인자들이 속성으로 저장된 Namespace 객체
    args = parser.parse_args()

    # 마스터 객체 생성
    # MB -> 바이트 변환: * 1024 * 1024
    master = FileCarvingMaster(
        port=args.port,
        overlap_size=args.overlap * 1024 * 1024,
        stream_block_size=args.block * 1024 * 1024,
    )
    
    # 출력 디렉토리 설정
    master.results_dir = Path(args.output)
    master.results_dir.mkdir(exist_ok=True)

    # DD 이미지 로드 성공 시 서버 시작
    if master.load_dd_image(args.image):
        master.start_server()


# ============================================================================
# 스크립트 실행 시 메인 함수 호출
# ============================================================================
# __name__: 현재 모듈의 이름
# 직접 실행 시: "__main__"
# import 될 때: 모듈 이름 (예: "file_carving_master")
#
# 이 조건문 덕분에:
# - python file_carving_master.py로 실행하면 main() 호출
# - import file_carving_master로 가져오면 main() 호출 안 함

if __name__ == "__main__":
    main()
