# Cấu hình demo và ngân sách cloud

Preset dưới đây để người vận hành review trước khi tự chạy Terraform plan/apply. Chưa có bằng chứng launch, quyền create, áp dụng credit hoặc workload cloud. Quota/giá do người vận hành kiểm tại Sydney (`ap-southeast-2`) ngày 2026-10-26:

- Standard On-Demand quota: 16 vCPU; truy vấn EC2 pending/running và EKS cluster không có kết quả.
- `t3.xlarge`: 4 vCPU / 16 GiB, 0,2112 USD/giờ Linux Shared On-Demand.
- `m6i.2xlarge`: 8 vCPU / 32 GiB, 0,48 USD/giờ cùng điều kiện.
- Credit hiển thị trong Console: 199,99 USD tại thời điểm ảnh; không phải cam kết mọi khoản phí đều được credit chi trả.

## Cấu hình đã chốt: 16 vCPU / 32 GiB

| Mức | Data Platform | ML/LLM worker | Tổng vCPU / RAM | Hai máy mỗi giờ |
|---|---|---|---|---:|
| **Demo (chốt)** | c6i.2xlarge | 2 × c6i.xlarge | 16 / 32 GiB | 0,888 USD |
| Peak (tham khảo) | m6i.2xlarge | 1 × m6i.2xlarge | 16 / 64 GiB | 0,96 USD |

Data Platform c6i.2xlarge (8 vCPU / 16 GiB) + hai worker c6i.xlarge (4 vCPU / 8 GiB mỗi máy) = 16 vCPU / 32 GiB, dùng hết quota Standard On-Demand. Không chọn t3 vì Spark và llama.cpp chạy CPU liên tục: t3 bị chậm khi hết CPU credit (`standard`) hoặc tính thêm tiền (`unlimited`). Giá Pricing API ngày 2026-10-26 (Linux Shared On-Demand, Sydney): c6i.2xlarge 0,444 USD/giờ, c6i.xlarge 0,222 USD/giờ. Lệnh kiểm lại:

```bash
aws pricing get-products --region us-east-1 --service-code AmazonEC2 \
  --filters Type=TERM_MATCH,Field=instanceType,Value=c6i.2xlarge \
    "Type=TERM_MATCH,Field=location,Value=Asia Pacific (Sydney)" \
    Type=TERM_MATCH,Field=operatingSystem,Value=Linux Type=TERM_MATCH,Field=tenancy,Value=Shared \
    Type=TERM_MATCH,Field=preInstalledSw,Value=NA Type=TERM_MATCH,Field=capacitystatus,Value=Used \
  --query 'PriceList[0]' --output text \
  | python3 -c 'import json,sys; t=json.load(sys.stdin)["terms"]["OnDemand"]; print([d["pricePerUnit"]["USD"] for o in t.values() for d in o["priceDimensions"].values()])'
```

Data Platform 16 GiB vẫn dùng overlay AWS chuẩn (`aws_runtime_profile: standard`); overlay performance cần 32 GiB nên không bật. Overlay chuẩn giới hạn tổng 6 CPU cho container (hai Spark worker 2 core mỗi worker, đổi 2026-10-27 để Bronze 100 GB dùng CPU đang bỏ không); 2 vCPU còn lại chừa cho hệ điều hành, Docker và SSM. ML Platform và LLM Platform chia trên hai worker 8 GiB; sau phần hệ thống/DaemonSet, mỗi node còn khoảng 6,5 GiB cho pod, nên không pod nào được xin quá mức đó. Phải đo RAM theo từng lớp; nếu không vừa, chỉ bật lớp cần demo.

Preset mẫu: [Data Platform](../../infra/aws/terraform/demo.tfvars.example) và [EKS worker](../../infra/aws/kubernetes/demo.tfvars.example). Bản peak giữ ở `demo-peak.tfvars.example` để tham khảo. Các mẫu không chứa key, role hay email thật. Budget cảnh báo đề xuất 40 USD trước credit cho đợt final, giữ khoảng 160 USD dự phòng. AWS Budgets chỉ cảnh báo, không phải công tắc ngắt chi phí.

## Tính theo lịch bật/tắt

Công thức cho bản demo, giữ EKS control plane trong standard support cả ngày:

`chi phí/ngày ≈ 0,888 × số giờ ba máy chạy + 0,10 × 24 + 0,005 × 3 × số giờ ba IPv4 tồn tại`

Bảng sau giả định ba máy chạy cùng số giờ, ba public IPv4 tự cấp chỉ tồn tại khi máy chạy, không có Elastic IP riêng, và control plane chạy liên tục:

| Thời gian ba máy chạy | Máy + EKS/ngày | Ba IPv4/ngày | Tổng/ngày | Tổng 3 ngày |
|---|---:|---:|---:|---:|
| 8 giờ/ngày | 9,50 | 0,12 | 9,62 | 28,87 USD |
| 12 giờ/ngày | 13,06 | 0,18 | 13,24 | 39,71 USD |
| 24 giờ/ngày | 23,71 | 0,36 | 24,07 | 72,22 USD |

Preset đặt budget cảnh báo 40 USD, khớp lịch 12 giờ/ngày; chạy liên tục sẽ vượt mức cảnh báo.

Chưa gồm EBS (boot 30 + data 100 + node 80 GiB gp3, PVC, snapshot), S3, ECR, load balancer và IPv4 của nó, log, truyền dữ liệu, thuế hoặc dịch vụ khác. EBS vẫn tính phí cả khi máy dừng. Không suy số ngày credit đủ dùng từ bảng này khi chưa kiểm các phần phí còn lại.

Nguồn giá nền: [EKS](https://aws.amazon.com/eks/pricing/), [public IPv4](https://aws.amazon.com/vpc/pricing/); giá EC2 lấy từ kết quả Pricing API của người vận hành. EKS extended support và các tính năng có phí riêng chưa nằm trong phép tính.

## Mức sẵn sàng (HA) và giới hạn

- **Có:** dự phòng mức máy. Feature API (2 replica), data-agent và coordinator (KEDA tối thiểu 2) khai `podAntiAffinity` ưu tiên đặt replica ở hai node khác nhau. Một worker chết hoặc đang được thay thì replica ở node kia vẫn phục vụ. Control plane EKS do AWS chạy dự phòng.
- **Ưu tiên, không bắt buộc (`preferred`):** nếu một node đầy, scheduler vẫn được đặt hai replica cùng node; khi đó mất dự phòng. Chọn `preferred` để kind một node ở local vẫn chạy cùng manifest. Kiểm bằng `kubectl get pods -o wide`.
- **Chưa có:** app một replica (Redis, pgvector, llama.cpp, KServe, MLflow/KFP, Langfuse, agentgateway) vẫn gián đoạn khi node của nó chết; hai worker cùng AZ nên sự cố AZ làm mất cả hai; Data Platform là một EC2; chưa có PodDisruptionBudget.
- **Production nên có:** ít nhất ba worker trải 2–3 AZ, PodDisruptionBudget, replica cho mọi service phục vụ, storage nhân bản hoặc dịch vụ quản lý (RDS, ElastiCache).

## Khi cần máy lớn (peak)

Overlay AWS chuẩn giới hạn cho VM 8 vCPU / 16 GiB. Đổi EC2 thành máy lớn hơn không tự tăng giới hạn container.

Để dùng preset peak, đặt `aws_runtime_profile: performance` trong biến Ansible thật. Playbook kiểm VM có ít nhất 8 vCPU / khoảng 32 GiB trước khi chọn thêm `docker-compose.aws-performance.yml`:

- Hai Spark worker: mỗi worker 2 CPU, container 6 GiB; Spark usable memory 5 GiB.
- Airflow: 2 CPU / 6 GiB; Spark driver/executor cấu hình 4 GiB, 2 executor core.
- Runtime thường trực có tổng limit khoảng 7 CPU / 21 GiB, còn phần cho OS và stack bổ sung.

Đây là giới hạn cấu hình, chưa phải phép đo thực tế. Kafka/Flink/CDC, DataHub và các thành phần khác cần profile cloud và đo RAM/CPU riêng trước khi chạy đồng thời. Không dùng phần RAM còn lại như bằng chứng chắc chắn mọi stack sẽ vừa.

## Quota, cập nhật node và tạm nghỉ

Bản demo dùng đủ 16/16 vCPU, nên preset EKS đặt:

- `node_desired_size=2`, `node_min_size=0`, `node_max_size=2`.
- `node_update_strategy="MINIMAL"`: thay từng node một, dừng node cũ trước khi tạo node mới. Đã dùng hết 16 vCPU, nên `DEFAULT` (tạo node mới trước) sẽ vượt quota. Trong lúc thay, node còn lại vẫn phục vụ các app có replica ở đó.
- `single_az_workers=true`: worker cố định ở subnet đầu để PVC EBS giữ cùng AZ sau scale-to-zero/resume. Control plane vẫn có subnet hai AZ.

Khi nghỉ, người vận hành cần chờ job hoàn tất, lưu artifact/receipt vào storage bền vững, rồi dừng EC2 Data Platform và đưa EKS worker về desired/min 0. Không stop một worker EC2 thủ công bên dưới managed node group vì Auto Scaling có thể tạo máy thay thế. Khi cần dùng lại, khởi động Data Platform, đưa worker desired về 2, kiểm PVC/readiness và nạp lại dữ liệu online nếu cần. URL demo không phục vụ khi workload đã dừng.

Scale-to-zero và restart chưa được kiểm runtime trong repo. Các database/model cần EBS/PVC/S3 thích hợp; Redis/cache hoặc state trong memory không tự được bảo toàn. EKS control plane, EBS và các tài nguyên giữ lại vẫn có phí lúc không có worker. Khi kết thúc giai đoạn dùng, cần review quy trình lưu dữ liệu và dọn tài nguyên để kết thúc phí nền.

Nguồn hành vi: [EKS update strategy](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-update-behavior.html), [EC2 instance lifecycle](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html).

[Cloud setup](cloud_setup.md) · [Mục lục](../README.md)
