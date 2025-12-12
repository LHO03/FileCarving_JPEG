# 파일 카빙 분산 처리 시스템

삭제된 JPEG 파일을 여러 컴퓨터에 분산하여 복구하는 시스템입니다.

## 시스템 구조

```
┌─────────────────┐                         ┌─────────────────┐
│     Master      │ ───── 청크 데이터 ────→ │     Worker 1    │
│   (서버 PC)     │ ←── 복구된 JPEG 파일 ── │   (클라이언트)   │
│                 │                         └─────────────────┘
│  - DD 이미지    │                         ┌─────────────────┐
│  - 작업 분배    │ ───── 청크 데이터 ────→ │     Worker 2    │
│  - 결과 수집    │ ←── 복구된 JPEG 파일 ── │   (클라이언트)   │
│  - 중복 제거    │                         └─────────────────┘
└─────────────────┘                                  ...
```

## 파일 구성

| 파일 | 설명 |
|------|------|
| `file_carving_master.py` | 마스터 서버 (DD 이미지 분배, 결과 수집) |
| `file_carving_worker.py` | 워커 클라이언트 (JPEG 카빙 수행) |
| `test_local.py` | 로컬 테스트용 스크립트 |

## 사용 방법

### 1. DD 이미지 준비

FTK Imager 등으로 USB/디스크의 DD 이미지를 생성합니다.

### 2. 마스터 서버 실행 (메인 PC)

```bash
python file_carving_master.py <DD이미지경로> [옵션]

# 예시
python file_carving_master.py usb_image.dd
python file_carving_master.py usb_image.dd --port 5000 --overlap 2
```

**옵션:**
- `--port`, `-p`: 서버 포트 (기본: 5000)
- `--overlap`, `-o`: 오버랩 크기 MB (기본: 1)
- `--output`, `-O`: 출력 디렉토리 (기본: recovered_files)

### 3. 워커 실행 (다른 PC들)

```bash
python file_carving_worker.py <마스터_IP> [포트]

# 예시
python file_carving_worker.py 192.168.1.100
python file_carving_worker.py 192.168.1.100 5000
```

### 4. 결과 확인

마스터 PC의 `recovered_files/` 디렉토리에 복구된 JPEG 파일들이 저장됩니다.

## 동작 흐름

```
1. 마스터: DD 이미지 로드 및 서버 시작
           ↓
2. 워커들: 마스터에 연결 (30초 대기)
           ↓
3. 마스터: 이미지를 청크로 분할하여 각 워커에게 전송
           ↓
4. 워커:   받은 청크에서 JPEG 시그니처 검색 (카빙)
           ↓
5. 워커:   발견된 JPEG 파일들을 마스터로 전송
           ↓
6. 마스터: 중복 제거 후 파일 저장
```

## 오버랩(Overlap) 처리

JPEG 파일이 청크 경계에 걸쳐있을 수 있습니다:

```
청크 0                     청크 1
[......JPEG 시작]──────────[JPEG 끝......]
              ↑ 경계
```

이를 해결하기 위해 각 청크에 오버랩 영역을 추가합니다:

```
청크 0: [────────────────][오버랩]
청크 1:          [오버랩][────────────────]
                    ↑ 겹치는 영역
```

워커는 자신의 **담당 영역에서 시작하는** 파일만 처리하여 중복을 방지합니다.

## 주요 클래스 및 함수

### 마스터 (file_carving_master.py)

| 함수/클래스 | 설명 |
|------------|------|
| `FileCarvingMaster` | 마스터 서버 클래스 |
| `load_dd_image()` | DD 이미지 파일 로드 |
| `start_server()` | 서버 시작 및 워커 연결 대기 |
| `distribute_and_process()` | 청크 분배 및 결과 수집 |
| `send_binary()` | 바이너리 데이터 전송 |
| `receive_binary()` | 바이너리 데이터 수신 |

### 워커 (file_carving_worker.py)

| 함수/클래스 | 설명 |
|------------|------|
| `FileCarvingWorker` | 워커 클라이언트 클래스 |
| `connect()` | 마스터 서버 연결 |
| `carve_jpeg()` | JPEG 파일 카빙 수행 |
| `validate_jpeg()` | JPEG 유효성 검증 |
| `send_results()` | 복구된 파일 전송 |

## 통신 프로토콜

모든 메시지는 `길이(4바이트) + 데이터` 형식으로 전송됩니다.

```
┌──────────┬─────────────────────┐
│  Length  │        Data         │
│ (4 bytes)│    (N bytes)        │
└──────────┴─────────────────────┘
```

- JSON 메시지: UTF-8 인코딩
- 바이너리 데이터: Raw bytes

## 로컬 테스트

한 컴퓨터에서 테스트하려면:

```bash
# 테스트 이미지 자동 생성 후 테스트
python test_local.py --test

# 실제 이미지로 테스트
python test_local.py usb_image.dd 4
```

## 제한사항

- 현재 JPEG 파일만 지원
- 워커 연결 대기 시간: 30초
- 네트워크 대역폭에 따라 대용량 이미지 처리 시간 증가

## 향후 개선 사항

- PNG, PDF 등 추가 파일 형식 지원
- 병렬 워커 처리 (현재는 순차 처리)
- 진행률 표시
- 워커 재연결 지원
