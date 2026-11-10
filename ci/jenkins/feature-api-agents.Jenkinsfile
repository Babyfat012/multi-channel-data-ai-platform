// CI/CD MCP server + Agent: test → build image Feature API (MCP tool) → helm --atomic
// → sync system prompts from Langfuse → deploy Agents that use those MCP tools → smoke test via agentgateway. Secrets in Vault (secret/platform/*); Jenkins only holds
// the read-only token `vault-ci-token` and `kind-kubeconfig`.
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
    string(name: 'EVAL_SKUS', defaultValue: '', description: 'SKUs to evaluate agents on; empty = local set sku-0000x, AWS (pipeline A) fills APP-…')
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
          // helm upgrade --atomic: if the new pod is not Ready in time ⇒ automatic rollback to the previous revision.
          sh '''
            set +x
            if [ "$PLATFORM_STORAGE" = minio ]; then eval "$(scripts/setup/vault_env.sh minio)"; fi
            set +x
            eval "$(scripts/setup/vault_env.sh postgres)"
            if [ "$PLATFORM_STORAGE" = s3 ]; then
              # Enable RAG (registry already on S3; without this variable /chunks returns 503 and model-agent's RAG tool breaks).
              export FEAST_RAG_REGISTRY="s3://$LAKEHOUSE_BUCKET/feature-store/rag-registry.db"
              # D3 not migrated yet (no publications/ on S3 yet): empty = legacy reader. Remove this line after the first publication.
              export FEAST_PUBLICATION_ROOT=
            fi
            SKIP_BUILD_PUSH=1 REPLICAS=2 bash scripts/setup/deploy_kind_feature_api.sh "$TAG"
            # Point the gateway's MCP backend at the feature-mcp pod (deployed above) then reload the ConfigMap; no Secrets in the file.
            kubectl --context $KCTX apply -f infra/k8s/agents/agentgateway.yaml
            kubectl --context $KCTX -n agentgateway rollout restart deploy/agentgateway
            kubectl --context $KCTX -n agentgateway rollout status deploy/agentgateway --timeout=180s
          '''
        }
      }
    }
    stage('Sync prompts from Langfuse') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // System prompt: take the `production` version from Langfuse → ConfigMap agent-prompts (Agents read it via systemMessageFrom).
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
          // Agent 1 is evaluated on features; Agent 2 on two tools: prediction (vs Feature API /predict) and RAG (vs rag/corpus).
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
          // kagent lists tools through the gateway itself; a missing tool = MCP server or gateway broken.
          sh '''
            tools=$(kubectl --context $KCTX -n kagent get remotemcpserver feature-tools -o jsonpath='{.status.discoveredTools[*].name}')
            echo "tools: $tools"
            for t in get_sku_features get_policy_chunks search_policy_chunks predict_return_risk; do
              echo "$tools" | grep -qw "$t" || { echo "missing tool $t"; exit 1; }
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
