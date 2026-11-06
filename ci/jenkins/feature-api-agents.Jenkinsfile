// CI/CD MCP server + Agent: test → build image Feature API (MCP tool) → helm --atomic
// → đồng bộ system prompt từ Langfuse → deploy Agent dùng MCP tool đó → smoke test qua agentgateway. Secret ở Vault (secret/platform/*); Jenkins chỉ giữ
// token chỉ-đọc `vault-ci-token` và `kind-kubeconfig`.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  parameters {
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 'Use s3 and Pod Identity on EKS')
    string(name: 'CLOUD_REGION', defaultValue: 'ap-southeast-1', description: 'AWS region for S3')
    string(name: 'CLOUD_LAKEHOUSE_BUCKET', defaultValue: '', description: 'Required when DATA_STORAGE=s3')
    string(name: 'IMAGE_REGISTRY', defaultValue: 'localhost:5001', description: 'Registry namespace prefix; use ECR for cloud')
    string(name: 'TARGET_CONTEXT', defaultValue: 'kind-platform', description: 'Kubecontext from the deployment kubeconfig')
    string(name: 'KUBE_CREDENTIAL', defaultValue: 'kind-kubeconfig', description: 'Jenkins file credential containing the target kubeconfig')
    string(name: 'EVAL_SKUS', defaultValue: '', description: 'SKU chấm agent; trống = bộ local sku-0000x, AWS (pipeline A) điền APP-…')
  }
  environment {
    EVAL_SKUS = "${params.EVAL_SKUS}"
    PLATFORM_STORAGE = "${params.DATA_STORAGE}"
    AWS_REGION = "${params.CLOUD_REGION}"
    LAKEHOUSE_BUCKET = "${params.CLOUD_LAKEHOUSE_BUCKET}"
    REGISTRY = "${params.IMAGE_REGISTRY}"
    KCTX = "${params.TARGET_CONTEXT}"
    VAULT_ADDR = 'http://localhost:8200'
    TAG = "${env.GIT_COMMIT ?: env.BUILD_NUMBER}"
  }
  stages {
    stage('Test') {
      steps {
        sh 'uv run --no-project --python 3.12 --with-requirements serving/requirements-dev.txt pytest serving/tests -q -o pythonpath=. --cov=serving --cov-fail-under=90'
      }
    }
    stage('Build') {
      steps { sh 'docker build -f docker/Dockerfile.serving -t platform-feature-api:$TAG .' }
    }
    stage('Push') {
      steps { sh 'scripts/run/push_images.sh platform-feature-api:$TAG' }
    }
    stage('Deploy MCP server') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // helm upgrade --atomic: pod mới không Ready trong hạn ⇒ tự rollback về revision trước.
          sh '''
            set +x
            if [ "$PLATFORM_STORAGE" = minio ]; then eval "$(scripts/setup/vault_env.sh minio)"; fi
            set +x
            eval "$(scripts/setup/vault_env.sh postgres)"
            SKIP_BUILD_PUSH=1 REPLICAS=2 bash scripts/setup/deploy_kind_feature_api.sh "$TAG"
          '''
        }
      }
    }
    stage('Sync prompts from Langfuse') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // System prompt lấy bản `production` trên Langfuse → ConfigMap agent-prompts (Agent đọc qua systemMessageFrom).
          sh 'python3 scripts/setup/sync_agent_prompts.py'
        }
      }
    }
    stage('Deploy Agent') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh '''
            kubectl --context $KCTX apply -f infra/k8s/agents/data-agent.yaml -f infra/k8s/agents/model-agent.yaml
            kubectl --context $KCTX -n kagent wait agent/data-agent agent/model-agent --for=condition=Ready --timeout=180s
          '''
        }
      }
    }
    stage('Eval agent') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // Agent 1 chấm feature; Agent 2 chấm hai tool: dự đoán (so với Feature API /predict) và RAG (so với rag/corpus).
          sh '''
            bash scripts/run/eval_agent_ci.sh
            EVAL_AGENT=model-agent EVAL_TASK=predict bash scripts/run/eval_agent_ci.sh
            EVAL_AGENT=model-agent EVAL_TASK=policy bash scripts/run/eval_agent_ci.sh
          '''
        }
      }
    }
    stage('Smoke') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // kagent tự liệt kê tool qua gateway; thiếu tool = MCP server hoặc gateway hỏng.
          sh '''
            tools=$(kubectl --context $KCTX -n kagent get remotemcpserver feature-tools -o jsonpath='{.status.discoveredTools[*].name}')
            echo "tools: $tools"
            for t in get_sku_features get_policy_chunks search_policy_chunks predict_return_risk; do
              echo "$tools" | grep -qw "$t" || { echo "thiếu tool $t"; exit 1; }
            done
          '''
        }
      }
    }
  }
  post {
    always { archiveArtifacts artifacts: 'out/agent-eval*.json*', allowEmptyArchive: true }
  }
}
