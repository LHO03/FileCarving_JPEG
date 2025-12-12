#!/usr/bin/env python3
"""
파일 카빙 시스템 테스트 스크립트
로컬에서 마스터와 워커를 함께 실행하여 테스트
"""

import subprocess
import sys
import time
import os
from pathlib import Path


def create_test_image(output_path="test_image.dd", num_jpegs=10):
    """테스트용 DD 이미지 생성"""
    print(f"\n[테스트] {num_jpegs}개 JPEG가 포함된 테스트 이미지 생성 중...")
    
    image_size = 50 * 1024 * 1024  # 50MB
    
    with open(output_path, 'wb') as f:
        f.write(b'\x00' * image_size)
        
        for i in range(num_jpegs):
            offset = (i + 1) * (image_size // (num_jpegs + 2))
            
            # 다양한 크기의 JPEG
            if i % 3 == 0:
                jpeg_size = 1000
            elif i % 3 == 1:
                jpeg_size = 30000
            else:
                jpeg_size = 100000
            
            # JPEG 구조
            jpeg_data = b'\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00'
            jpeg_data += b'\xFF\xDB\x00\x43\x00'
            jpeg_data += bytes([i % 256]) * (jpeg_size - len(jpeg_data) - 2)
            jpeg_data += b'\xFF\xD9'
            
            f.seek(offset)
            f.write(jpeg_data)
            
            print(f"  - JPEG {i+1}: 오프셋 {offset:,}, 크기 {len(jpeg_data):,} bytes")
    
    print(f"[테스트] 이미지 생성 완료: {output_path}")
    return output_path


def run_test(image_path, num_workers=2):
    """테스트 실행"""
    print(f"\n[테스트] 파일 카빙 테스트 시작")
    print(f"  - 이미지: {image_path}")
    print(f"  - 워커 수: {num_workers}")
    
    # 결과 디렉토리 초기화
    results_dir = Path("recovered_files")
    if results_dir.exists():
        for f in results_dir.glob("*"):
            f.unlink()
    
    # 마스터 시작
    print("\n[테스트] 마스터 서버 시작...")
    master = subprocess.Popen(
        [sys.executable, 'file_carving_master.py', image_path, '--overlap', '1'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    time.sleep(2)
    
    # 워커들 시작
    print(f"[테스트] 워커 {num_workers}개 시작...")
    workers = []
    for i in range(num_workers):
        w = subprocess.Popen(
            [sys.executable, 'file_carving_worker.py', 'localhost', '5000'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        workers.append(w)
        time.sleep(0.5)
    
    # 마스터 결과 대기
    master_out, _ = master.communicate(timeout=120)
    
    print("\n" + "=" * 60)
    print("마스터 출력:")
    print("=" * 60)
    print(master_out)
    
    # 워커 결과 수집
    for i, w in enumerate(workers):
        try:
            out, _ = w.communicate(timeout=5)
            print(f"\n워커 {i} 출력:")
            print(out)
        except:
            pass
    
    # 결과 검증
    verify_results()


def verify_results():
    """결과 검증"""
    print("\n" + "=" * 60)
    print("복구 결과 검증")
    print("=" * 60)
    
    results_dir = Path("recovered_files")
    if not results_dir.exists():
        print("복구된 파일 없음!")
        return
    
    jpg_files = list(results_dir.glob("*.jpg"))
    print(f"\n복구된 JPEG: {len(jpg_files)}개\n")
    
    valid = 0
    for f in sorted(jpg_files):
        with open(f, 'rb') as file:
            data = file.read()
        
        is_valid = (
            len(data) >= 20 and
            data[:2] == b'\xFF\xD8' and
            data[-2:] == b'\xFF\xD9'
        )
        
        status = "✓" if is_valid else "✗"
        print(f"  {status} {f.name}: {len(data):,} bytes")
        
        if is_valid:
            valid += 1
    
    print(f"\n유효한 JPEG: {valid}/{len(jpg_files)}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] != '--test':
        # 실제 이미지 사용
        image_path = sys.argv[1]
        num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    else:
        # 테스트 이미지 생성
        image_path = create_test_image()
        num_workers = 2
    
    run_test(image_path, num_workers)


if __name__ == "__main__":
    main()
