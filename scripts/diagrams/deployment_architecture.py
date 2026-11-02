"""Vẽ topology triển khai cluster / node / namespace / Pod và container trên EC2."""

from html import escape

from common import ROOT
from render_png import render

OUT = ROOT / "docs" / "diagrams"
PALETTE = {
    "cloud": ("#f8fafc", "#8293a9"),
    "cluster": ("#f5f8ff", "#3975c6"),
    "ns": ("#f2f7ff", "#3975c6"),
    "deploy": ("#f6fcf8", "#368769"),
    "pod": ("white", "#368769"),
    "svc": ("#fff5e8", "#c78024"),
    "config": ("#f9f3ff", "#895abd"),
    "storage": ("#fff2f6", "#c4597c"),
}


class Topology:
    """Xuất vector có quan hệ bao chứa và kiểm tra hình học trước khi render."""

    def __init__(self, slug, title, w, h):
        self.slug, self.w, self.h = slug, w, h
        self.frames, self.edges, self.content = [], [], []
        self.rects = {}
        self.title = title
        self.text(70, 75, title, 45, bold=True)

    def text(self, x, y, value, size=22, color="#263d58", bold=False, width=None):
        """Ghi chữ; co nhẹ cỡ chữ khi tên resource dài hơn khối chứa."""
        if width:
            size = min(size, width / max(1, len(value)) / 0.61)
        self.content.append(
            f'<text x="{x}" y="{y}" font-family="DejaVu Sans,sans-serif" font-size="{size:.2f}" font-weight="{700 if bold else 400}" fill="{color}">{escape(value)}</text>'
        )

    def frame(self, key, x, y, w, h, title, kind="ns", parent=None, subtitle=None):
        """Vẽ cluster / namespace / workload với ranh giới lồng nhau."""
        self.rects[key] = (x, y, w, h, parent)
        fill, stroke = PALETTE[kind]
        dash = ' stroke-dasharray="10 6"' if kind == "deploy" else ""
        self.frames.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill}" stroke="{stroke}" stroke-width="2.5"{dash}/>'
        )
        self.text(
            x + 22, y + 36, title, 28 if kind in {"ns", "cluster"} else 25, stroke, True, w - 44
        )
        if subtitle:
            self.text(x + 22, y + 67, subtitle, 20, width=w - 44)

    def box(self, key, x, y, w, h, title, lines=(), kind="config", parent=None):
        """Vẽ một Service, Pod, container hoặc tài nguyên cấu hình."""
        self.rects[key] = (x, y, w, h, parent)
        fill, stroke = PALETTE[kind]
        self.content.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" fill="{fill}" stroke="{stroke}" stroke-width="2"/>'
        )
        if title.startswith("Pod /") and h < 90:
            self.text(x + 12, y + 23, title, 21, stroke, True, w - 24)
            if lines:
                self.text(x + 12, y + 47, " · ".join(lines), 18, width=w - 24)
        else:
            self.text(x + 16, y + 31, title, 24, stroke, True, w - 32)
            for i, line in enumerate(lines):
                self.text(x + 16, y + 60 + i * 27, line, 20, width=w - 32)

    def service(self, key, x, y, w, name, ports, parent, type_="ClusterIP"):
        """Biểu diễn Service và ánh xạ port → targetPort."""
        self.box(key, x, y, w, 77, f"Service / {name}", [f"{type_} · {ports}"], "svc", parent)

    def workload(self, key, x, y, w, name, pods, container, port, parent, note="", h=205):
        """Deployment bao chứa các Pod; mỗi Pod thể hiện container và cổng."""
        self.frame(key, x, y, w, h, name, "deploy", parent)
        gap, padding = 20, 20
        pw = (w - 2 * padding - gap * (len(pods) - 1)) / len(pods)
        for i, pod in enumerate(pods):
            self.box(
                f"{key}-pod-{i}",
                x + padding + i * (pw + gap),
                y + 58,
                pw,
                h - 83,
                f"Pod / {pod}",
                [f"container: {container}", f"containerPort: {port}"],
                "pod",
                key,
            )
        if note:
            self.text(x + 20, y + h - 10, note, 17, width=w - 40)

    def edge(self, points, label="", at=None, kind="data", dashed=False):
        """Vẽ network traffic, selector hoặc quan hệ quản lý."""
        colors = {"data": "#3478be", "call": "#d15a7a", "control": "#8a62b1", "storage": "#bd7728"}
        color = colors[kind]
        dash = ' stroke-dasharray="9 7"' if dashed else ""
        self.edges.append(
            f'<polyline points="{" ".join(f"{x},{y}" for x, y in points)}" fill="none" stroke="{color}" stroke-width="2.5" stroke-linejoin="round" marker-end="url(#{kind})"{dash}/>'
        )
        if label:
            x, y = at
            width = len(label) * 10.5 + 20
            self.edges.append(
                f'<rect x="{x - width / 2}" y="{y - 21}" width="{width}" height="29" rx="4" fill="white"/>'
            )
            self.edges.append(
                f'<text x="{x}" y="{y}" text-anchor="middle" font-family="DejaVu Sans,sans-serif" font-size="20" fill="{color}">{escape(label)}</text>'
            )

    def select(self, sx, sy, destinations):
        """Vẽ Service selector → các Pod qua EndpointSlice."""
        for px, py in destinations:
            self.edge([(sx, sy), (sx, py - 12), (px, py - 12), (px, py)], kind="data")

    def save(self):
        """Kiểm tra containment rồi xuất SVG / HTML / PNG."""
        for key, (x, y, w, h, parent) in self.rects.items():
            assert x >= 0 and y >= 0 and x + w <= self.w and y + h <= self.h, key
            if parent:
                a, b, c, d, _ = self.rects[parent]
                assert a <= x and b <= y and x + w <= a + c and y + h <= b + d, (key, parent)
        markers = "".join(
            f'<marker id="{k}" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0 0 L8 4 L0 8" fill="{v}"/></marker>'
            for k, v in {
                "data": "#3478be",
                "call": "#d15a7a",
                "control": "#8a62b1",
                "storage": "#bd7728",
            }.items()
        )
        svg = (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" role="img" aria-labelledby="title desc"><title id="title">{escape(self.title)}</title><desc id="desc">Topology theo manifest: AWS / VPC / host hoặc EKS cluster / worker node pool / namespace / Service / workload / Pod / container; có PVC, ServiceAccount, Secret và luồng kết nối. Tên Pod dùng mẫu; không phải snapshot live.</desc><defs>{markers}</defs><rect width="{self.w}" height="{self.h}" fill="white"/>'
            + "".join(self.frames + self.edges + self.content)
            + "</svg>"
        )
        (OUT / f"{self.slug}.svg").write_text(svg)
        path = OUT / f"{self.slug}.html"
        path.write_text(
            f'<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(self.title)}</title><style>body{{margin:0}}svg{{display:block;width:100%;height:auto}}</style>{svg}</html>'
        )
        render(path)
        print(f"{self.slug}: {len(self.rects)} resources / containment PASS")


def ml_llm():
    """Vẽ cluster và các namespace chứa Service / controller / Pod cụ thể."""
    d = Topology(
        "ml-llm-platform-system-architecture", "ML + LLM Platform · Deployment topology", 4800, 4280
    )
    d.text(
        70,
        120,
        "AWS ap-southeast-2 · cấu hình / evidence repo · số replica là desired; tên Pod dùng mẫu, không phải snapshot live",
        24,
    )
    d.frame("aws", 40, 180, 4720, 3780, "AWS account · ap-southeast-2", "cloud")
    d.frame(
        "vpc",
        70,
        250,
        4660,
        3320,
        "VPC 10.42.0.0/16 · private EC2 ↔ EKS · Security Groups theo cổng",
        "cloud",
        "aws",
    )
    d.frame(
        "ec2",
        100,
        330,
        800,
        3150,
        "EC2 / Data Platform",
        "cloud",
        "vpc",
        "r6i.xlarge · Docker Compose · IAM role / IMDSv2",
    )
    d.frame(
        "eks",
        980,
        330,
        3690,
        3150,
        "EKS cluster / platform-apps",
        "cluster",
        "vpc",
        "Managed control plane + managed node group · API private + public CIDR allowlist",
    )
    d.box(
        "cp",
        1040,
        430,
        3580,
        110,
        "EKS cluster API endpoint / private ENIs",
        [
            "Private endpoint :443 + public endpoint CIDR allowlist · control plane chạy trong AWS-managed VPC, được thể hiện ngoài VPC bên dưới"
        ],
        parent="eks",
    )
    d.frame(
        "pool",
        1040,
        610,
        3580,
        360,
        "Managed node group / platform-apps · 2 worker node (sizing theo evidence AWS)",
        "cloud",
        "eks",
    )
    for i, x in enumerate((1080, 2860)):
        d.frame(
            f"node{i}",
            x,
            680,
            1720,
            235,
            f"Worker {i + 1} · ký hiệu node {'A' if i == 0 else 'B'} · r6i.xlarge / 4 vCPU / 32 GiB",
            "cloud",
            "pool",
        )
        d.box(
            f"nodeagents{i}",
            x + 25,
            740,
            800,
            140,
            "kubelet · containerd · node agents",
            [
                "ns kube-system: VPC CNI / kube-proxy / EBS CSI / Pod Identity",
                "ns observability: Fluent Bit DaemonSet Pod",
            ],
            parent=f"node{i}",
        )
        d.box(
            f"scheduled-pods{i}",
            x + 855,
            740,
            840,
            140,
            "Application Pod(s) trên worker này",
            [
                "Pod IP từ VPC CNI · container runtime: containerd",
                "Scheduler gán các Pod thuộc namespace bên dưới",
            ],
            kind="pod",
            parent=f"node{i}",
        )
    d.text(
        1080,
        1015,
        "Namespace là phạm vi logic trên toàn cluster; Pod của một namespace có thể ở cả hai node. Không ghim Pod vào node nếu manifest không yêu cầu.",
        23,
    )
    d.text(
        1080,
        1050,
        "Feature API / coordinator / data-agent có preferred podAntiAffinity; không bảo đảm mỗi replica ở một node. Service → EndpointSlice → Pod IP.",
        22,
    )
    d.edge(
        [(2800, 540), (2800, 590), (1940, 590), (1940, 680)],
        "API / scheduling control flow",
        (2430, 578),
        "control",
        True,
    )
    d.edge([(2800, 590), (3720, 590), (3720, 680)], kind="control", dashed=True)
    # EC2: giữ các dependency ngoài cluster trong cùng hình.
    blocks = [
        (
            "airflow",
            440,
            390,
            "Container / airflow :8080 (host :8081)",
            [
                "DAG ml_feature_materialize:",
                "Spark export → feast apply → materialize",
                "DAG ml_rag_ingest:",
                "upload → chunk → embed → load → validate → publish",
                "Feast / fastembed là thư viện trong container",
            ],
        ),
        (
            "stream",
            900,
            330,
            "Containers / Kafka + Flink + feast-pusher",
            [
                "kafka:9092 → Flink SQL cửa sổ 24h / SKU",
                "Flink JobManager + TaskManager → Kafka",
                "feast-pusher: ONLINE_AND_OFFLINE",
                "Redis NodePort 30379 + Parquet S3",
            ],
        ),
        (
            "mlflow",
            1300,
            300,
            "Container / mlflow :5000 (host :5050)",
            [
                "KFP → MLflow qua private EC2 IP:5050",
                "PostgreSQL metadata DB trên EC2",
                "S3 artifact proxy · model / dataset version",
                "production gate → CLI build image",
            ],
        ),
        (
            "jenkins",
            1670,
            300,
            "Container / jenkins (host :8090)",
            [
                "Test / build / push ECR / Helm --atomic",
                "EKS API :443 → access entry platform-ci",
                "Namespace RBAC · deploy agent / API / RAG",
                "Eval harness / EP-BVA / mutation / Locust",
            ],
        ),
        (
            "vault",
            2040,
            230,
            "Container / vault :8200",
            [
                "KV secrets → ingress / CI / gateway",
                "EBS /mnt/platform-data/vault",
                "Init / unseal thủ công",
            ],
        ),
        (
            "datahub",
            2340,
            250,
            "DataHub stack / Compose riêng",
            [
                "GMS + frontend + dependencies",
                "Lineage feature / RAG + assertions",
                "Bật theo pha; cùng stack ảnh Data Platform",
            ],
        ),
        (
            "pgexternal",
            2660,
            250,
            "Container / postgres :5432",
            [
                "MLflow DB · Airflow DB · warehouse",
                "Node exporter :9100 / PG exporter :9187",
                "Prometheus EKS scrape IP riêng",
            ],
        ),
        (
            "ec2note",
            2990,
            230,
            "Network / identity",
            [
                "Không có Kubernetes Pod trên EC2 này",
                "Airflow → EKS: :30379 / :30432",
                "Jenkins → private EKS API :443",
            ],
        ),
    ]
    for key, y, h, title, lines in blocks:
        d.box(key, 130, y, 740, h, title, lines, parent="ec2")
    # Namespace feature-store: cả API và Redis.
    d.frame("feature-store", 1960, 1120, 820, 790, "Namespace / feature-store", parent="eks")
    d.service("api-svc", 2000, 1190, 740, "feature-api", ":8000 → http / :8000", "feature-store")
    d.workload(
        "api-deploy",
        2000,
        1295,
        740,
        "Deployment / feature-api · replicas=2",
        ["feature-api-<rs>-a", "feature-api-<rs>-b"],
        "api / FastAPI",
        "8000",
        "feature-store",
        h=190,
    )
    d.select(2370, 1267, [(2190, 1353), (2550, 1353)])
    d.box(
        "api-config",
        2000,
        1500,
        740,
        80,
        "Ingress feature-api · api.* · TLS Secret feature-api-tls",
        ["SA feature-api · Secret feature-api-env · probes /healthz /readyz"],
        parent="feature-store",
    )
    d.service(
        "redis-svc",
        2000,
        1595,
        740,
        "redis",
        ":6379 → :6379 · nodePort :30379",
        "feature-store",
        "NodePort",
    )
    d.workload(
        "redis-deploy",
        2000,
        1700,
        740,
        "Deployment / redis · replicas=1 · Recreate",
        ["redis-<rs>-<id>"],
        "redis:7.4-alpine / AOF",
        "6379",
        "feature-store",
        h=140,
    )
    d.select(2370, 1672, [(2370, 1758)])
    d.box(
        "redis-pvc",
        2000,
        1855,
        740,
        40,
        "PVC redis-data · 2 Gi / RWO → gp3 EBS → /data",
        kind="storage",
        parent="feature-store",
    )
    # Ingress controller et gateway sont des namespaces séparés.
    d.frame("ingress", 1040, 1120, 820, 340, "Namespace / ingress", parent="eks")
    d.service(
        "ingress-svc",
        1080,
        1190,
        740,
        "NGINX controller (Helm)",
        ":80 / :443 → controller Pod",
        "ingress",
        "LoadBalancer → AWS NLB",
    )
    d.workload(
        "ingress-deploy",
        1080,
        1295,
        740,
        "NGINX controller Deployment (Helm)",
        ["nginx-<rs>-<id>"],
        "nginx-ingress",
        "80 / 443",
        "ingress",
        h=145,
    )
    d.select(1450, 1267, [(1450, 1353)])
    d.frame("agentgateway", 1040, 1570, 820, 340, "Namespace / agentgateway", parent="eks")
    d.service(
        "agw-svc", 1080, 1640, 740, "agentgateway", ":3000 HTTP / :15020 metrics", "agentgateway"
    )
    d.workload(
        "agw-deploy",
        1080,
        1740,
        740,
        "Deployment / agentgateway · replicas=1",
        ["agentgateway-<rs>-<id>"],
        "agentgateway:v1.6.0",
        "3000 / 15020",
        "agentgateway",
        h=145,
    )
    d.select(1450, 1717, [(1450, 1798)])
    # kagent: controller sinh Deployment / Service cho Agent CR.
    d.frame(
        "kagent",
        2880,
        1120,
        1740,
        790,
        "Namespace / kagent · Helm release + Agent CRs",
        parent="eks",
    )
    d.service("ui-svc", 2920, 1190, 520, "kagent-ui", ":8080 → UI Pod", "kagent")
    d.workload(
        "ui-deploy",
        2920,
        1295,
        520,
        "UI Deployment (Helm)",
        ["kagent-ui-<rs>-<id>"],
        "UI chat",
        "8080",
        "kagent",
        h=145,
    )
    d.select(3180, 1267, [(3180, 1353)])
    d.box(
        "kagent-ctrl",
        3470,
        1190,
        1110,
        110,
        "kagent controller Pod(s) + bundled PostgreSQL + kmcp",
        [
            "Helm workload; controller watch Agent / ModelConfig / RemoteMCPServer CRDs",
            "WorkerPool kagent-default → warm worker Pod / 6 SandboxAgent actors",
        ],
        parent="kagent",
    )
    d.box(
        "kagentcfg",
        3470,
        1330,
        1110,
        110,
        "ModelConfig default-model-config · ConfigMap agent-prompts",
        [
            "Secret kagent-agw-key · RemoteMCPServer feature-tools → gateway /mcp",
            "Ingress agent-ui · agent.* · Secrets agent-ui-tls / agent-ui-basic-auth",
        ],
        parent="kagent",
    )
    for name, x, n in [("coordinator", 2920, 2), ("data-agent", 3480, 2), ("model-agent", 4040, 1)]:
        d.service(f"{name}-svc", x, 1490, 540, name, ":8080 → agent Pod(s)", "kagent")
        depname = f"Agent CR → Deployment / {name}"
        d.workload(
            f"{name}-dep",
            x,
            1600,
            540,
            depname,
            [f"{name}-Pod {i + 1}" for i in range(n)],
            "agent runtime",
            "8080",
            "kagent",
            h=190,
        )
        d.select(
            x + 270,
            1567,
            [
                (x + 20 + (500 - (n - 1) * 20) / n / 2 + i * ((500 - (n - 1) * 20) / n + 20), 1658)
                for i in range(n)
            ],
        )
        d.text(
            x + 15,
            1825,
            "KEDA / HPA: min=2, max=3" if n == 2 else "Replica do kagent mặc định",
            19,
            width=510,
        )
    d.text(
        2920,
        1873,
        "Agent CR → controller → Deployment / ReplicaSet / Pod + Service. model-agent chưa có ScaledObject KEDA.",
        21,
        width=1660,
    )
    # RAG.
    d.frame("rag", 1040, 2070, 820, 620, "Namespace / rag", parent="eks")
    d.service(
        "rag-svc", 1080, 2140, 740, "pgvector", ":5432 → :5432 · nodePort :30432", "rag", "NodePort"
    )
    d.workload(
        "rag-deploy",
        1080,
        2245,
        740,
        "Deployment / pgvector · replicas=1 · Recreate",
        ["pgvector-<rs>-<id>"],
        "postgres / pgvector PG17",
        "5432",
        "rag",
        h=195,
    )
    d.select(1450, 2217, [(1450, 2303)])
    d.box(
        "rag-pvc",
        1080,
        2490,
        740,
        80,
        "PVC pgdata · 1 Gi / RWO → gp3 EBS",
        ["Mount /var/lib/postgresql/data"],
        kind="storage",
        parent="rag",
    )
    d.box(
        "rag-config", 1080, 2600, 740, 65, "Secret rag-pg · ConfigMap pgvector-init", parent="rag"
    )
    # KServe.
    d.frame("ml-serving", 1960, 2070, 820, 620, "Namespace / ml-serving", parent="eks")
    d.service(
        "predictor-svc",
        2000,
        2140,
        740,
        "return-risk-predictor",
        ":80 → predictor :8080",
        "ml-serving",
    )
    d.box(
        "isvc",
        2000,
        2245,
        740,
        75,
        "InferenceService / return-risk · RawDeployment",
        ["KServe controller [ns kserve] reconcile → Deployment / Service"],
        parent="ml-serving",
    )
    d.workload(
        "predictor-deploy",
        2000,
        2350,
        740,
        "Generated Deployment · min=maxReplicas=1",
        ["return-risk-predictor-<rs>-<id>"],
        "kserve-container / model image",
        "8080",
        "ml-serving",
        h=190,
    )
    d.select(2370, 2217, [(2370, 2408)])
    d.box(
        "model-sa",
        2000,
        2580,
        740,
        85,
        "ServiceAccount / return-risk → Pod Identity",
        ["S3 registry read · Feast SDK → Redis service :6379"],
        parent="ml-serving",
    )
    # KFP: task Pods theo run, không phải replicas của Deployment.
    d.frame("kubeflow", 2880, 2070, 820, 620, "Namespace / kubeflow", parent="eks")
    d.service(
        "kfp-svc", 2920, 2140, 740, "ml-pipeline-ui", ":80 → UI workload (upstream KFP)", "kubeflow"
    )
    d.box(
        "kfp-system",
        2920,
        2245,
        740,
        85,
        "KFP system Pods / UI / API / workflow controller",
        ["Cài từ KFP 2.17.2 manifests · dependency Pods theo upstream"],
        parent="kubeflow",
    )
    d.frame(
        "kfp-run",
        2920,
        2360,
        740,
        185,
        "Pipeline run → task Pods / SA pipeline-runner",
        "deploy",
        "kubeflow",
    )
    for i, (name, x, y) in enumerate(
        [
            ("load / PIT", 2940, 2420),
            ("split", 3180, 2420),
            ("train", 3420, 2420),
            ("evaluate", 2940, 2480),
            ("register", 3180, 2480),
        ]
    ):
        d.box(f"task{i}", x, y, 220, 45, f"Pod / {name}", kind="pod", parent="kfp-run")
    d.box(
        "drift",
        2920,
        2580,
        740,
        85,
        "Job / mle-drift → Pod / Evidently",
        ["SA pipeline-runner → S3 read · JSON drift report · không tự retrain"],
        parent="kubeflow",
    )
    # LLM A/B.
    d.frame("llm", 3800, 2070, 820, 620, "Namespace / llm", parent="eks")
    for name, x, pvc, size in [
        ("qwen", 3840, "models", "3 Gi"),
        ("qwen-small", 4230, "models-small", "1 Gi"),
    ]:
        d.service(f"{name}-svc", x, 2140, 350, name, ":8080 → :8080", "llm")
        d.workload(
            f"{name}-dep",
            x,
            2245,
            350,
            f"Deployment / {name} · 1",
            [f"{name}-<rs>-<id>"],
            "llama.cpp / GGUF",
            "8080",
            "llm",
            h=235,
        )
        d.select(x + 175, 2217, [(x + 175, 2303)])
        d.box(
            f"{name}-pvc",
            x,
            2520,
            350,
            90,
            f"PVC / {pvc}",
            [f"{size} / gp3 → /models"],
            kind="storage",
            parent="llm",
        )
    d.text(3840, 2650, "1.5B mặc định · 0.5B khi bật A/B · gateway route 1:1", 20, width=740)
    # Observability và platform controllers.
    d.frame("observability", 1040, 2870, 1740, 540, "Namespace / observability", parent="eks")
    obs = [
        ("prometheus-server", 1080, ":80 → metrics API", "Prometheus + config reload"),
        ("grafana", 1500, ":80 → Grafana", "Grafana / dashboards"),
        ("loki", 1920, ":3100 → Loki", "SingleBinary / 1 Pod"),
        ("jaeger", 2340, ":4317 / :16686", "Jaeger / all-in-one Pod"),
    ]
    for name, x, port, desc in obs:
        d.service(f"{name}-svc", x, 2940, 390, name, port, "observability")
        d.box(
            f"{name}-pod",
            x,
            3060,
            390,
            120,
            f"Pod(s) / {name}",
            [desc],
            kind="pod",
            parent="observability",
        )
        d.select(x + 195, 3017, [(x + 195, 3060)])
    d.box(
        "obs-storage",
        1080,
        3240,
        1650,
        120,
        "DaemonSet / Fluent Bit → Pod trên mỗi worker → Service loki:3100",
        [
            "Loki: PVC 1 Gi / gp3 · Prometheus: không PVC, retention 6h · Jaeger: RAM · Grafana datasource ConfigMap",
            "Prometheus scrape /metrics → KEDA query · OTLP từ gateway / agent → Jaeger",
        ],
        parent="observability",
    )
    for name, y, port, dep, secret in [
        (
            "agentregistry",
            2870,
            "12121",
            "Helm workload → Pod(s) / agentregistry",
            "agentregistry-db",
        ),
        ("langfuse", 3150, "3000", "Deployment langfuse → 1 Pod / langfuse", "langfuse-env"),
    ]:
        d.frame(name, 2880, y, 820, 260, f"Namespace / {name}", parent="eks")
        d.service(f"{name}-svc", 2920, y + 65, 740, name, f":{port} → app Pod(s)", name)
        d.box(
            f"{name}-workload",
            2920,
            y + 155,
            740,
            85,
            dep,
            [f"Secret {secret} → database riêng trên pgvector.rag:5432"],
            kind="pod",
            parent=name,
        )
        d.select(3290, y + 142, [(3290, y + 155)])
    for name, y, title, detail in [
        (
            "cert-manager",
            2870,
            "Deployment / controller + cainjector + webhook",
            "Controller Pods → Certificate / TLS Secrets / ClusterIssuer",
        ),
        (
            "keda",
            3010,
            "KEDA operator + metrics API Pods",
            "ScaledObject [ns kagent] → HPA → Deployment replicas",
        ),
        (
            "kserve",
            3150,
            "KServe controller-manager Pod(s)",
            "InferenceService [ns ml-serving] → predictor Deployment / Service",
        ),
        (
            "ate-system",
            3290,
            "Agent Substrate / API + controller + node agents",
            "WorkerPool kagent-default [ns kagent] → warm worker Pod / gVisor",
        ),
    ]:
        d.frame(name, 3800, y, 820, 120, f"Namespace / {name}", parent="eks")
        d.text(3820, y + 66, title, 21, width=780)
        d.text(3820, y + 98, detail, 19, width=780)
    d.text(
        2920,
        3445,
        "SandboxAgent CRs [ns kagent]: 6 actors / 1 warm worker; không phải 6 Deployment Pods.",
        18,
        width=1680,
    )
    d.box(
        "managed-cp",
        1040,
        3600,
        3580,
        55,
        "AWS-managed control plane / ngoài VPC khách hàng: kube-apiserver · scheduler · controller-manager · etcd",
        parent="aws",
    )
    d.edge(
        [(4620, 3627), (4640, 3627), (4640, 485), (4620, 485)],
        "Managed control plane → API endpoint",
        (4330, 3587),
        "control",
        True,
    )
    # AWS managed services ngoài VPC.
    d.frame(
        "s3", 100, 3670, 3190, 220, "Amazon S3 · ngoài VPC · bucket private / IAM", "storage", "aws"
    )
    d.box(
        "s3raw",
        140,
        3740,
        930,
        110,
        "Raw + lakehouse / Gold",
        [
            "Data Platform → feature / training / RAG inputs",
            "S3 object storage · không phải PVC / EBS",
        ],
        kind="storage",
        parent="s3",
    )
    d.box(
        "s3feature",
        1110,
        3740,
        1070,
        110,
        "feature-store / offline + registries",
        ["sku_daily / sku_return_24h / training_lines", "registry.db / rag-registry.db"],
        kind="storage",
        parent="s3",
    )
    d.box(
        "s3artifact",
        2220,
        3740,
        1030,
        110,
        "MLflow artifacts / RAG Parquet",
        ["MLflow EC2 ghi artifacts · KFP qua server proxy", "RAG chunk / embedding / registry"],
        kind="storage",
        parent="s3",
    )
    d.frame("ecr", 3400, 3670, 1220, 220, "Amazon ECR · ngoài VPC", "storage", "aws")
    d.box(
        "images",
        3440,
        3740,
        1140,
        110,
        "Container images / image:SHA",
        ["Jenkins / CLI push → node IAM role pull", "API / train / return-risk model-inside-image"],
        kind="storage",
        parent="ecr",
    )
    d.box(
        "clients",
        3500,
        30,
        1100,
        110,
        "Seller / API client / vận hành",
        ["HTTPS :443 · api.* / agent.* / grafana.* / traces.* / registry.*"],
    )
    d.box(
        "nlb",
        3500,
        282,
        1100,
        40,
        "AWS NLB public · provision bởi Service type LoadBalancer",
        parent="vpc",
        kind="svc",
    )
    d.edge([(4050, 140), (4050, 282)], "HTTPS", (4150, 235), "call")
    d.edge(
        [(4600, 302), (4700, 302), (4700, 1100), (1875, 1100), (1875, 1228), (1820, 1228)],
        "NLB → NGINX Service → controller Pod",
        (4220, 1085),
        "call",
    )
    d.box(
        "pv",
        1040,
        3425,
        1740,
        40,
        "Cluster-scoped StorageClass gp3 / PersistentVolume · ebs.csi.aws.com · WaitForFirstConsumer / Retain",
        kind="storage",
        parent="eks",
    )
    d.box(
        "ebs",
        1040,
        3500,
        1740,
        50,
        "AWS EBS gp3 volumes · mã hóa · cùng AZ với Pod / node attach · backing cho PVC RWO",
        kind="storage",
        parent="vpc",
    )
    d.edge(
        [(1080, 2530), (1015, 2530), (1015, 3445), (1040, 3445)],
        "PVC pgdata → PV",
        (1150, 2780),
        "storage",
        True,
    )
    d.edge(
        [(2740, 1875), (2835, 1875), (2835, 3445), (2780, 3445)],
        "PVC redis-data → PV",
        (2835, 2800),
        "storage",
        True,
    )
    d.edge([(1820, 3465), (1820, 3500)], kind="storage", dashed=True)
    # Đường gọi Service; đường trả kết quả không vẽ ngược.
    d.edge(
        [(1820, 1210), (1905, 1210), (1905, 1180), (2000, 1180), (2000, 1210)],
        "api. → Ingress feature-api",
        (1890, 1095),
        "call",
    )
    d.edge(
        [(1450, 1190), (1450, 1090), (3180, 1090), (3180, 1190)],
        "agent. → Ingress agent-ui → Service kagent-ui:8080",
        (3020, 1080),
        "call",
    )
    d.edge(
        [(3450, 1567), (3450, 1960), (1450, 1960), (1450, 1717)],
        "A2A / LLM / MCP qua gateway",
        (2550, 1945),
        "call",
    )
    d.edge(
        [(1820, 1678), (1905, 1678), (1905, 1975), (2840, 1975), (2840, 1528), (2920, 1528)],
        "A2A dispatch → Agent Services",
        (2590, 2008),
        "call",
    )
    d.edge(
        [(1820, 1785), (1875, 1785), (1875, 1930), (2810, 1930), (2810, 1228), (2740, 1228)],
        "MCP → Feature API :8000/mcp/",
        (2190, 1920),
        "call",
    )
    d.edge(
        [(1180, 1885), (1180, 2000), (3750, 2000), (3750, 2178), (3840, 2178)],
        "LLM /v1 → qwen:8080 / qwen-small:8080",
        (3420, 1985),
        "call",
    )
    d.edge([(2020, 2470), (1890, 2470), (1890, 1640), (2000, 1640)], kind="data")
    d.edge(
        [(2500, 1485), (2500, 1575), (2370, 1575), (2370, 1595)],
        "Feast SDK → Redis",
        (2580, 1570),
        "data",
    )
    d.edge(
        [(2740, 1450), (2810, 1450), (2810, 2030), (2370, 2030), (2370, 2140)],
        "/predict → return-risk-predictor",
        (2530, 2020),
        "call",
    )
    d.edge(
        [(2020, 1450), (1930, 1450), (1930, 2020), (1450, 2020), (1450, 2140)],
        "RAG → pgvector.rag:5432",
        (1620, 2010),
        "data",
    )
    d.edge(
        [(600, 830), (930, 830), (930, 1550), (1910, 1550), (1910, 1633), (2000, 1633)],
        "EC2 → nodeIP:30379 → Service Redis",
        (1130, 1535),
        "data",
    )
    d.edge(
        [(870, 760), (950, 760), (950, 2105), (1080, 2105), (1080, 2178)],
        "RAG load → nodeIP:30432",
        (950, 2050),
        "data",
    )
    d.edge(
        [(3300, 2495), (3300, 2735), (950, 2735), (950, 1450), (870, 1450)],
        "register → EC2 MLflow :5050",
        (2290, 2725),
        "data",
    )
    d.edge(
        [(3290, 3390), (3290, 3450), (1885, 3450), (1885, 2178), (1820, 2178)],
        "Registry / prompt DB → pgvector.rag:5432",
        (2470, 3440),
        "data",
    )
    d.edge(
        [(4010, 3740), (4010, 3580), (2830, 3580), (2830, 850)],
        "ECR pull → node IAM role → containerd",
        (3370, 3567),
        "storage",
        True,
    )
    d.edge(
        [(1640, 3740), (1640, 3610), (2820, 3610), (2820, 2620), (2740, 2620)],
        "S3 read qua Pod Identity / SA",
        (2200, 3597),
        "storage",
        True,
    )
    d.text(
        110,
        4015,
        "KÝ HIỆU: khung xanh = namespace trong cluster; khung xanh lá nét đứt = workload controller; khối Pod chứa container; vàng = Service; hồng = PVC / storage.",
        24,
    )
    d.text(
        110,
        4060,
        "Service chọn Ready Pod qua selector / EndpointSlice, không chọn node. ReplicaSet và hậu tố Pod do controller sinh; các tên <rs>/<id> là mẫu.",
        24,
    )
    d.text(
        110,
        4105,
        "TLS Secret / Ingress resource ở namespace ứng dụng; NGINX ở ns ingress. Feature API RollingUpdate maxSurge=1 / maxUnavailable=0; Helm --atomic rollback khi lỗi.",
        24,
    )
    d.text(
        110,
        4150,
        "gp3 StorageClass: EBS CSI → PV → PVC → Pod volume; RWO / WaitForFirstConsumer / Retain. API, KFP, predictor dùng ServiceAccount → Pod Identity cho S3.",
        24,
    )
    d.text(
        110,
        4195,
        "Nguồn: infra/aws/kubernetes · infra/k8s · infra/helm/feature-api · scripts/setup. Workload theo Helm/upstream có nhãn đại diện; không giả lập số Pod live.",
        23,
    )
    d.save()


def data_platform():
    """Vẽ EC2 / Docker networks / từng container và các cụm Spark, Flink."""
    d = Topology(
        "data-platform-system-architecture",
        "Data Platform · Host / network / container topology",
        4200,
        3510,
    )
    d.text(
        70,
        120,
        "AWS ap-southeast-2 · Data Platform chạy Docker Compose trên một EC2; Spark / Flink cluster là process trong container, không phải EKS.",
        25,
    )
    d.frame("aws", 570, 200, 3570, 2940, "AWS account · ap-southeast-2", "cloud")
    d.frame(
        "vpc",
        600,
        270,
        3510,
        2420,
        "VPC 10.42.0.0/16 · EC2 private IP · Security Group · quản trị qua SSM",
        "cloud",
        "aws",
    )
    d.frame(
        "host",
        640,
        370,
        3430,
        2230,
        "EC2 / Data Platform · r6i.xlarge · 4 vCPU / 32 GiB",
        "cloud",
        "vpc",
        "Linux host · Docker Engine · IAM instance role / IMDSv2 · EBS /mnt/platform-data",
    )
    d.frame(
        "docker",
        1080,
        480,
        2940,
        1520,
        "Docker bridge network / platform_default · DNS theo Compose service name",
        "cluster",
        "host",
    )
    d.box(
        "engineer",
        70,
        500,
        440,
        220,
        "Data Engineer / ML Engineer",
        [
            "DBeaver / SQL client",
            "aws ssm start-session",
            "UI / SQL qua tunnel",
            "Không có Kubernetes namespace",
        ],
        parent=None,
    )
    d.box(
        "generator",
        680,
        500,
        350,
        370,
        "Host CLI / generator",
        [
            "Python one-off process",
            "5 channel adapters",
            "Amazon / Shopify / Shopee",
            "TikTok Shop / Walmart",
            "skew / schema / duplicate",
            "JSONL → S3 raw",
            "JDBC / SQL → PostgreSQL",
            "Publisher / replay → Kafka",
        ],
        parent="host",
    )
    d.box(
        "volumes",
        680,
        980,
        350,
        480,
        "EBS data filesystem",
        [
            "/mnt/platform-data/",
            "postgres → PGDATA",
            "airflow → /opt/airflow",
            "spark-worker-1/2 → spill",
            "spark-events → event logs",
            "kafka → log segments",
            "flink → file output",
            "vault → /vault/file",
            "Bind mounts, không phải PVC",
            "Dữ liệu còn khi container đổi",
        ],
        kind="storage",
        parent="host",
    )
    d.box(
        "exporters",
        680,
        1560,
        350,
        240,
        "Host-published exporters",
        [
            "node-exporter :9100",
            "postgres-exporter :9187",
            "Bind private IP; SG từ EKS",
            "Prometheus scrape từ ảnh 2",
        ],
        parent="host",
    )
    d.box(
        "hostops",
        680,
        1900,
        350,
        250,
        "Hạ tầng host",
        [
            "Terraform → VPC / EC2 / S3",
            "Ansible → runtime / config",
            "Jenkins / Vault / MLflow",
            "Chi tiết workload ở ảnh 2",
            "Profile bật theo pha",
        ],
        parent="host",
    )
    # Một container airflow chứa scheduler, webserver, local task và Spark client driver.
    d.frame(
        "airflow",
        1140,
        560,
        850,
        330,
        "Compose service / airflow · 1 container",
        "deploy",
        "docker",
    )
    d.box(
        "airflow-process",
        1170,
        630,
        790,
        160,
        "Container / airflow · port :8080",
        [
            "webserver + scheduler + LocalExecutor",
            "DP1 ingest/validate → DP2 Silver → DP3 Gold → DP4 warehouse",
            "SparkSubmitOperator → client driver trong chính container này",
        ],
        kind="pod",
        parent="airflow",
    )
    d.text(
        1170,
        835,
        "Published host 127.0.0.1:8081 → container :8080 · metadata DB postgres:5432",
        20,
        width=790,
    )
    d.frame(
        "catalog",
        2130,
        560,
        850,
        330,
        "Compose service / lakekeeper · 1 container",
        "deploy",
        "docker",
    )
    d.box(
        "catalog-process",
        2160,
        630,
        790,
        160,
        "Container / lakekeeper · REST :8181",
        [
            "Iceberg catalog → namespace / table schema / snapshots",
            "Metadata backend → PostgreSQL database riêng",
            "S3 credential từ IAM instance role",
        ],
        kind="pod",
        parent="catalog",
    )
    d.text(2160, 835, "Host loopback :8181 → container :8181 · catalog profile", 20, width=790)
    d.frame(
        "postgres",
        3120,
        560,
        850,
        330,
        "Compose service / postgres · 1 container",
        "deploy",
        "docker",
    )
    d.box(
        "postgres-process",
        3150,
        630,
        790,
        160,
        "Container / postgres:15 · :5432",
        [
            "DB source Walmart / warehouse / Airflow / Lakekeeper / MLflow",
            "warehouse_lifecycle dim / fact / SCD2 / indexes",
            "wal_level=logical · replication slots cho CDC",
        ],
        kind="pod",
        parent="postgres",
    )
    d.text(
        3150, 835, "Host loopback :5432 → container :5432 · EBS bind mount PGDATA", 20, width=790
    )
    d.frame(
        "spark",
        1140,
        1020,
        1840,
        550,
        "Spark standalone cluster · 1 master container + 2 worker containers",
        "cluster",
        "docker",
        "Các container cùng EC2 host; worker không phải EC2 instance / Kubernetes node.",
    )
    d.box(
        "spark-master",
        1180,
        1120,
        510,
        180,
        "Container / spark-master",
        [
            "Master RPC :7077 · Web UI :8080",
            "Quản lý app / tài nguyên worker",
            "Không chạy client driver",
            "512 MiB container limit",
        ],
        kind="pod",
        parent="spark",
    )
    for name, x in [("spark-worker-1", 1750), ("spark-worker-2", 2370)]:
        d.frame(name, x, 1120, 560, 350, f"Container / {name}", "deploy", "spark")
        d.box(
            f"{name}-executor",
            x + 25,
            1190,
            510,
            205,
            "Executor JVM process(es)",
            [
                "Thực thi Spark tasks / shuffle / spill",
                "Python worker nếu job cần",
                "SPARK_WORKER_CORES=2",
                "Worker memory=2560m",
            ],
            kind="pod",
            parent=name,
        )
        d.text(x + 25, 1435, "Container limit: 2 CPU / 3500m", 19, width=510)
    d.text(
        1180,
        1520,
        "Driver ở Airflow → master đăng ký app → workers launch executor JVM → đọc/ghi S3 Iceberg + PostgreSQL.",
        22,
        width=1760,
    )
    d.frame(
        "trino", 3120, 1020, 850, 350, "Compose service / trino · 1 container", "deploy", "docker"
    )
    d.box(
        "trino-process",
        3150,
        1090,
        790,
        190,
        "Container / Trino coordinator + worker",
        [
            "Single-node Trino · HTTP :8080 (host :8083)",
            "Iceberg connector → Lakekeeper + S3",
            "PostgreSQL connector → postgres:5432",
            "Novel DE-1 · federation SQL / DBeaver",
        ],
        kind="pod",
        parent="trino",
    )
    d.frame(
        "kafka", 1140, 1680, 850, 270, "Compose service / kafka · 1 container", "deploy", "docker"
    )
    d.box(
        "kafka-process",
        1170,
        1745,
        790,
        150,
        "Container / Kafka 3.9.1 · KRaft",
        [
            "Broker + controller: INTERNAL :9092 / quorum :9093",
            "Host loopback :9094 → EXTERNAL :9094",
            "Lifecycle events / CDC topics · EBS log segments",
        ],
        kind="pod",
        parent="kafka",
    )
    d.frame(
        "flink", 2130, 1680, 850, 270, "Flink cluster · 2 Compose containers", "cluster", "docker"
    )
    d.box(
        "flink-jm",
        2160,
        1750,
        350,
        140,
        "flink-jobmanager",
        ["REST / UI :8081", "Job graph / checkpoints", "Host UI :8082"],
        kind="pod",
        parent="flink",
    )
    d.box(
        "flink-tm",
        2570,
        1750,
        380,
        140,
        "flink-taskmanager",
        ["2 task slots / executors", "Process memory 4096m", "Limit 4608m"],
        kind="pod",
        parent="flink",
    )
    d.frame("cdc", 3120, 1510, 850, 440, "CDC / UI · Compose profiles cdc", "deploy", "docker")
    d.box(
        "connect",
        3150,
        1580,
        790,
        160,
        "Container / debezium-connect",
        [
            "Connect REST :8083 (host :8084)",
            "Postgres WAL → change events → Kafka topics",
            "Novel DE-2 · create / update / delete probe",
        ],
        kind="pod",
        parent="cdc",
    )
    d.box(
        "kafkaui",
        3150,
        1780,
        790,
        110,
        "Container / kafka-ui (khi bật)",
        ["UI :8080 (host :8085) · topics / messages / offsets"],
        kind="pod",
        parent="cdc",
    )
    # DataHub stack tách network / project với platform Compose.
    d.frame(
        "dh-network",
        1080,
        2130,
        2940,
        400,
        "DataHub quickstart · Docker Compose project / network riêng · cùng EC2 host",
        "config",
        "host",
    )
    dh = [
        (
            "gms",
            1120,
            "datahub-gms-quickstart",
            ":8080 / host :8089",
            "metadata API / lineage / assertions",
        ),
        ("frontend", 1710, "frontend-quickstart", ":9002 / host :9002", "DataHub UI"),
        ("dhmysql", 2300, "mysql", ":3306", "Metadata DB"),
        ("dhsearch", 2890, "opensearch", ":9200", "Search / index"),
    ]
    for key, x, title, port, detail in dh:
        d.box(
            key,
            x,
            2210,
            550,
            130,
            f"Container / {title}",
            [port, detail],
            kind="pod",
            parent="dh-network",
        )
    d.box(
        "dhkafka",
        3480,
        2210,
        500,
        130,
        "Container / kafka-broker",
        [":9092 · DataHub metadata events", "Kafka riêng, không phải kafka platform"],
        kind="pod",
        parent="dh-network",
    )
    d.text(
        1120,
        2395,
        "Airflow publish → GMS qua host endpoint / Docker host gateway; GMS → metadata DB / search / event broker. UI → GMS.",
        22,
        width=2820,
    )
    d.text(
        1120,
        2440,
        "Quickstart còn có init / setup jobs theo compose upstream; chạy theo pha để chia sẻ tài nguyên với Spark / stream.",
        21,
        width=2820,
    )
    # Object storage ngoài VPC; không đưa S3 vào Docker network.
    d.frame(
        "s3",
        640,
        2800,
        3430,
        260,
        "Amazon S3 · outside VPC · raw bucket + lakehouse bucket",
        "storage",
        "aws",
    )
    d.box(
        "raw",
        680,
        2870,
        1020,
        135,
        "Raw / source exports",
        [
            "JSONL 5 channel · generator output được lưu lại",
            "Spark đọc source snapshots / lifecycle / label bridge",
        ],
        kind="storage",
        parent="s3",
    )
    d.box(
        "iceberg",
        1750,
        2870,
        1200,
        135,
        "Iceberg tables · Bronze / Silver / Gold",
        [
            "data files + metadata / snapshots · REST catalog Lakekeeper",
            "SCD2 / dim / fact / label / feat_ · partition / compaction",
        ],
        kind="storage",
        parent="s3",
    )
    d.box(
        "feature",
        3000,
        2870,
        1030,
        135,
        "Feature / training / RAG → ảnh 2",
        [
            "Feast Parquet / registry / training_lines",
            "Gold Return replay → Trino → Kafka → feature 24h",
        ],
        kind="storage",
        parent="s3",
    )
    d.edge(
        [(1030, 700), (1060, 700), (1060, 515), (3530, 515), (3530, 630)],
        "Walmart source import / SQL",
        (2780, 503),
        "data",
    )
    d.edge(
        [(2930, 1330), (3010, 1330), (3010, 930), (3600, 930), (3600, 790)],
        "JDBC source / Gold warehouse publish",
        (3370, 917),
        "data",
    )
    # Runtime networking và các pipeline chính.
    d.edge(
        [(510, 610), (600, 610), (600, 330), (1560, 330), (1560, 630)],
        "SSM tunnel → 127.0.0.1:8081",
        (1140, 315),
        "control",
        True,
    )
    d.edge(
        [(1030, 650), (1060, 650), (1060, 2760), (1190, 2760), (1190, 2870)],
        "generate → raw S3",
        (1080, 2745),
        "data",
    )
    d.edge(
        [(1950, 720), (2060, 720), (2060, 940), (1430, 940), (1430, 1120)],
        "spark-submit → :7077",
        (1730, 925),
        "control",
        True,
    )
    d.edge(
        [(1690, 1220), (1715, 1220), (1715, 1170), (1750, 1170)],
        "launch executor",
        (1730, 1095),
        "control",
        True,
    )
    d.edge(
        [(1690, 1260), (1720, 1260), (1720, 1490), (2650, 1490), (2650, 1470)],
        kind="control",
        dashed=True,
    )
    d.edge(
        [(1960, 675), (2080, 675), (2080, 535), (3500, 535), (3500, 630)],
        "metadata DB → postgres:5432",
        (2870, 525),
        "data",
    )
    d.edge([(2950, 715), (3150, 715)], "catalog DB", (3050, 700), "data")
    d.edge(
        [(2400, 1190), (2400, 965), (2550, 965), (2550, 790)],
        "Iceberg REST :8181",
        (2560, 950),
        "data",
    )
    d.edge(
        [(2910, 1320), (3030, 1320), (3030, 2760), (2350, 2760), (2350, 2870)],
        "Spark read / write S3 via IAM",
        (2690, 2748),
        "data",
    )
    d.edge([(3550, 1090), (3550, 790)], "JDBC :5432", (3670, 975), "data")
    d.edge(
        [(3150, 1180), (3065, 1180), (3065, 950), (2650, 950), (2650, 790)],
        "Iceberg REST catalog",
        (2900, 938),
        "data",
    )
    d.edge(
        [(3640, 1280), (3640, 1410), (4050, 1410), (4050, 2740), (2860, 2740), (2860, 2870)],
        "Trino reads S3",
        (3660, 2728),
        "data",
    )
    d.edge([(1990, 1820), (2160, 1820)], "Kafka :9092", (2075, 1805), "data")
    d.edge([(2510, 1820), (2570, 1820)], kind="control", dashed=True)
    d.edge(
        [(2570, 1890), (2570, 2060), (2970, 2060), (2970, 2775), (2760, 2775), (2760, 2870)],
        "stream file output / publish → lakehouse",
        (3180, 2045),
        "data",
    )
    d.edge(
        [(3860, 790), (4000, 790), (4000, 1550), (3700, 1550), (3700, 1580)],
        "WAL logical replication",
        (3970, 1470),
        "data",
    )
    d.edge(
        [(3150, 1650), (3080, 1650), (3080, 2040), (1550, 2040), (1550, 1895)],
        "CDC → Kafka topics",
        (2400, 2027),
        "data",
    )
    d.edge(
        [(1330, 790), (1330, 980), (1095, 980), (1095, 2070), (1390, 2070), (1390, 2210)],
        "publish metadata → DataHub GMS :8089",
        (1470, 2058),
        "control",
        True,
    )
    d.edge(
        [(2780, 2340), (2780, 2370), (1395, 2370), (1395, 2340)],
        "GMS backend / indexes / metadata events",
        (2060, 2360),
        "data",
    )
    d.text(
        80,
        3230,
        "ĐỌC HÌNH: AWS → VPC → EC2 host → Docker Engine / network → Compose service → container → JVM / task processes. Không có Service / Pod kiểu Kubernetes ở Data Platform.",
        24,
    )
    d.text(
        80,
        3275,
        "Tên container có suffix động do Compose tạo; hình dùng service name làm định danh ổn định. DNS nội bộ là airflow, spark-master, postgres, kafka, lakekeeper…",
        24,
    )
    d.text(
        80,
        3320,
        "Host-published port khác container port: Airflow 8081→8080, Trino 8083→8080, Flink UI 8082→8081, Connect 8084→8083. AWS host ports chủ yếu bind loopback.",
        24,
    )
    d.text(
        80,
        3365,
        "Rubric: batch / stream generator · Spark baseline / skew / evolution / dedup · Flink watermark / late / window · lakehouse / warehouse · DP1–DP4 · DataHub · Trino / CDC.",
        24,
    )
    d.text(
        80,
        3410,
        "Nguồn: docker-compose.yml + docker-compose.aws.yml + docker-compose.aws-stream.yml · docker/datahub-compose.override.yml · infra/aws/ansible. Chạy theo profile / pha.",
        24,
    )
    d.save()


if __name__ == "__main__":
    data_platform()
    ml_llm()
