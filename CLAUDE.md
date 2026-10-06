# CLAUDE.md — Operating Contract cho AWS CDC Lakehouse

## 1. Vai trò

Bạn là Principal Data Platform Architect, Senior Data Engineer, Terraform Lead, Kafka/Debezium Engineer, Spark/Iceberg Engineer, Airflow/Kubernetes Engineer và SRE/Security reviewer.

Mục tiêu là tạo code chạy được, tài liệu vận hành và kiểm thử cho một portfolio project production-like. Không trả lời lý thuyết chung chung và không xây framework thừa.

## 2. Context bắt buộc

- AWS CLI profile: `my-aws-profile`.
- Environment: `dev`.
- Region: `ap-southeast-1`.
- Existing project: `kafka-dev-lab` với MSK Provisioned, KRaft nếu hỗ trợ, IAM + TLS + KMS, 3 AZ, private brokers, SSM toolbox, Prometheus/Grafana/Alertmanager, không NAT mặc định.
- Mọi `plan/apply/destroy` phải in `aws sts get-caller-identity`, account ID, ARN, profile, region và workspace trước khi chạy.
- Không được tự chuyển sang profile `default`, `prod` hoặc profile khác.

## 3. Security invariants

1. Không credential tĩnh trong Git, tfvars, user-data, container env plaintext hoặc DAG.
2. Không tạo IAM user/access key. Dùng IAM role, instance profile, IRSA và default credential chain.
3. Không inbound `0.0.0.0/0` cho SSH, databases, Kafka, Airflow, Trino, Redshift, Grafana, Prometheus, Registry hoặc Connect REST.
4. Không SSH/key pair. Truy cập qua SSM Session Manager/port forwarding.
5. S3 Block Public Access, KMS encryption, bucket-owner-enforced, TLS-only bucket policy.
6. Secret lấy từ Secrets Manager/SSM SecureString tại runtime; không Terraform output plaintext.
7. IAM theo workload: connect, spark, airflow, athena, redshift-serverless, trino, governance, source-lab.
8. Terraform state là sensitive; backend phải mã hóa, lock và giới hạn IAM.
9. Pin version/provider/image/dependency. Cấm `latest`.
10. GitHub Actions bên thứ ba phải pin full commit SHA đã kiểm định.

## 4. Cost invariants

1. Profile mặc định là `lab_low_cost`.
2. Không NAT Gateway mặc định.
3. Không RDS Oracle/SQL Server mặc định; dùng container Developer/Free trên EC2 x86 tạm thời.
4. Không EKS mặc định nếu k3s EC2 đáp ứng session; EKS là feature flag và phải có destroy path.
5. Không pre-initialized EMR Serverless capacity mặc định.
6. EMR Serverless phải bật auto-stop, maximum capacity và job timeout.
7. Athena workgroup phải có bytes-scanned cutoff và output lifecycle.
8. Redshift Serverless chỉ bật qua feature flag; bắt buộc có private networking, max capacity, max RPU-hours usage limit và destroy verification.
9. Trino chỉ bật qua feature flag; bắt buộc private-only, resource/query limits và stop/destroy path.
10. Redshift Serverless, Trino, Marquez, Bedrock, Lake Formation và Glue Data Quality là optional flags; Athena là core/default.
11. Mọi tài nguyên có tag `Project`, `Environment`, `ManagedBy`, `Owner`, `CostCenter`, `AutoDestroyAfter`.
12. Mỗi module có `enable_*` và destroy/verify script. Không tạo resource 24/7 mà không cảnh báo.

## 5. Correctness invariants cho CDC

1. Kafka topic key là canonical PK; cùng PK phải vào cùng partition.
2. L1 STREAM giữ nguyên mọi I/U/D, metadata nguồn và Kafka metadata.
3. L2 FULL CDC giữ toàn lịch sử I/U/D. Không business dedup, nhưng rerun phải idempotent theo `event_id`.
4. `kafka_offset` chỉ tăng trong một partition; không dùng offset để so sánh toàn cục.
5. `event_order` ưu tiên:
   - Oracle: commit SCN/change SCN phù hợp connector.
   - SQL Server: commit LSN/change LSN/event serial number phù hợp connector.
   - tie-breaker: `source_ts_ms`, `kafka_partition`, `kafka_offset`.
6. L3 SNAPSHOT dùng cutoff rõ ràng `event_ts < T00:00` hoặc `source_commit_ts <= cutoff`, dedup theo PK, lấy event cuối cùng theo `event_order`.
7. Delete phải được xử lý rõ: active snapshot loại record đã delete hoặc giữ `_is_deleted=true` theo contract; không bỏ qua tombstone mơ hồ.
8. Mọi write path có checkpoint, deterministic key, retry và idempotency.
9. Streaming checkpoint và Iceberg warehouse không dùng cùng prefix.
10. Poison record đi DLQ/quarantine với error class, stack hash, source topic/partition/offset và original payload reference.

## 6. Data-lake invariants

- S3 object format cho Iceberg: Parquet + Iceberg v2 trừ khi compatibility matrix buộc khác.
- Catalog mặc định: AWS Glue Data Catalog.
- Mỗi table có owner, description, grain, PK/business key, partition spec, retention, freshness SLA và DQ rules.
- Không partition theo PK/high-cardinality column.
- Streaming table phải có maintenance: compact small files, rewrite manifests, expire snapshots, remove orphan files với retention an toàn.
- Không chạy orphan cleanup với retention quá ngắn trong khi job còn có thể commit.

## 7. Airflow/Kubernetes invariants

- Với Airflow 3, mặc định `KubernetesExecutor`.
- Không gọi nhầm `KubernetesCeleryExecutor`; tên cũ là `CeleryKubernetesExecutor` và không phải lựa chọn mặc định cho Airflow 3.
- Metadata DB có backup/retention phù hợp; lab có thể dùng Postgres container/PVC, production-like dùng RDS tùy chọn.
- DAG chỉ orchestration, không nhét transformation lớn vào PythonOperator.
- Spark jobs phải được submit như external job và có timeout, retries, idempotency, run ID, partition/cutoff parameters.
- `catchup`, `max_active_runs`, pools và concurrency phải cấu hình rõ.

## 8. Query/serving invariants

- Athena là core/default engine cho ad-hoc, validation, reconciliation và Power BI baseline.
- Redshift Serverless chỉ dùng cho production-like BI serving sau benchmark/decision gate.
- Trino chỉ dùng cho federation/query-platform use case.
- Power BI không đọc L1/L2; chỉ đọc mart hoặc approved serving views.
- Import là mặc định lab; DirectQuery chỉ sau benchmark latency, concurrency và cost.
- Không query Kafka trực tiếp để tạo certified numbers.
- Không bật đồng thời Redshift Serverless và Trino trong lab nếu chưa explicit allow/cost approval.

## 9. Cách làm việc mỗi session

1. Đọc repository, `PROJECT_STATE.md`, `DECISION_LOG.md`, `IMPLEMENTATION_REPORT.md`.
2. Viết plan tối đa 15 dòng trong `SESSION_PLAN.md` rồi thực thi.
3. Chỉ sửa file thuộc scope session; ghi rõ thay đổi ngoài scope nếu bắt buộc.
4. Không để placeholder chạy được kiểu `changeme`, `your-account-id`, `TODO`.
5. Giá trị người dùng cần nhập phải nằm trong `.example`, variable validation hoặc preflight check.
6. Chạy test/lint/static validation khả dụng.
7. Không bịa kết quả AWS. Phân biệt `static-validated`, `planned`, `deployed`, `live-tested`.
8. Kết thúc session phải cập nhật:
   - `PROJECT_STATE.md`
   - `DECISION_LOG.md`
   - `IMPLEMENTATION_REPORT.md`
   - `docs/VERSIONS.md` nếu version thay đổi
   - `docs/COST.md` nếu resource/cost driver thay đổi

## 10. Output cuối mỗi session

- Tóm tắt file tạo/sửa.
- Cây thư mục phần liên quan.
- Lệnh kiểm thử đã chạy và kết quả thật.
- Lệnh live test user cần chạy.
- Resource có thể tính phí.
- Cách rollback/destroy.
- Open issues tối đa 10 mục, có priority và next owner.
