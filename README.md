# GCS Upload Speed Test

일반 GCS 클라이언트 업로드와 gRPC 클라이언트 업로드 속도를 같은 버킷, 같은 페이로드 크기, 같은 반복 횟수로 비교하는 작은 Go CLI입니다.

- HTTP/JSON: `storage.NewClient`
- gRPC: `storage.NewGRPCClient`
- 기본 버킷: `bucket-speed-test`
- 기본 동작: 업로드 후 테스트 객체 삭제 `--cleanup=true`

## 실행

repo 루트에서 바로 실행할 수 있습니다.

```bash
git clone https://github.com/3boku/gcs-client.git
cd gcs-client
go run . --size 128MiB --iterations 5 --concurrency 1
```

인증은 아래 순서로 찾습니다.

1. `--credentials /path/to/service-account.json`
2. `GOOGLE_APPLICATION_CREDENTIALS`
3. `GCP_SERVICE_ACCOUNT_JSON`
4. `GOOGLE_CREDENTIALS`
5. `GCP_SERVICE_ACCOUNT_JSON_B64`
6. 현재 디렉터리의 `service-account.json`
7. Application Default Credentials

테스트용 키 파일을 repo 루트의 `service-account.json`에 두면 자동으로 사용합니다. 이 파일은 `.gitignore` 대상입니다.

## 이번 테스트 결과

실행 명령:

```bash
go run . --size 64MiB --iterations 3 --concurrency 1 --timeout 10m
```

조건:

- 버킷: `bucket-speed-test`
- 페이로드: `64MiB`
- 반복: transport별 `3회`
- 동시성: `1`
- 청크 크기: `16MiB`
- cleanup: `true`
- gRPC 자동 CRC32C checksum: 활성화
- gRPC client metrics export: 비활성화

결과:

```text
transport runs    bytes/run        min     median       mean        max     throughput
http-json    3    64.00 MiB     8.039s     8.513s      8.46s     8.829s     7.56 MiB/s
grpc         3    64.00 MiB     7.741s      7.83s     7.872s     8.046s     8.13 MiB/s
```

요약:

- HTTP/JSON 평균 처리량: `7.56 MiB/s`
- gRPC 평균 처리량: `8.13 MiB/s`
- 이번 측정에서는 gRPC가 약 `7.5%` 빨랐습니다.

계산:

```text
(8.13 - 7.56) / 7.56 * 100 = 7.5%
```

## 옵션

```text
--bucket bucket-speed-test         # GCS bucket name
--transports both|http-json|grpc   # 기본값 both
--size 64MiB                       # 업로드 객체 1개당 페이로드 크기
--iterations 3                     # transport별 측정 반복 횟수
--concurrency 1                    # 반복 1회당 병렬 업로드 객체 수
--chunk-size 16MiB                 # GCS writer chunk size; 0이면 chunking 비활성화
--cleanup=true                     # 업로드한 테스트 객체 삭제
--warmup=true                      # transport별 warmup 업로드 수행
--warmup-size 1MiB                 # warmup 페이로드 크기
--disable-grpc-autochecksum=false  # gRPC Writer 자동 CRC32C checksum 유지
--grpc-conn-pool 0                 # 0이면 라이브러리 기본값 사용
--timeout 10m                      # 전체 benchmark timeout
```

## 비교 시 주의점

gRPC Writer는 기본적으로 CRC32C checksum을 자동 계산합니다. HTTP/JSON 업로드와 순수 transport 차이만 보고 싶으면 같은 조건으로 한 번 더 실행해 비교하세요.

```bash
go run . \
  --size 64MiB \
  --iterations 3 \
  --concurrency 1 \
  --disable-grpc-autochecksum=true
```

네트워크 상태, 실행 위치, 버킷 리전, Direct Connectivity 가능 여부에 따라 결과가 달라질 수 있습니다. 특히 GCP 내부에서 같은 리전 버킷으로 접근하면 gRPC가 더 유리하게 나올 수 있습니다.

테스트 후 로컬 키 파일을 지우려면:

```bash
rm service-account.json
```

채팅이나 로그에 노출된 서비스계정 키는 테스트가 끝난 뒤 GCP에서 폐기하고 새 키로 교체하는 것이 안전합니다.
