// CI/CD Coordinator Agent: check manifest → prompt from Langfuse → deploy → wait for Ready → agent card via the A2A gateway.
// Gateway API key taken from Vault (secret/platform/agentgateway) via the read-only token `vault-ci-token`.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  parameters {
    string(name: 'TARGET_CONTEXT', defaultValue: 'kind-platform', description: 'Kubecontext from the deployment kubeconfig')
    string(name: 'KUBE_CREDENTIAL', defaultValue: 'kind-kubeconfig', description: 'Jenkins file credential containing the target kubeconfig')
    string(name: 'EVAL_TIMEOUT', defaultValue: '600', description: 'Seconds to wait per eval question; the Coordinator calls the CPU LLM over many rounds. A short timeout abandons running sessions and piles load on the LLM (06/11: 180 s ⇒ 9/12)')
    string(name: 'EVAL_SKUS', defaultValue: '', description: 'SKUs to evaluate agents on; empty = local set sku-0000x, AWS (pipeline A) fills APP-…')
  }
  environment {
    EVAL_SKUS = "${params.EVAL_SKUS}"
    EVAL_TIMEOUT = "${params.EVAL_TIMEOUT}"
    KCTX = "${params.TARGET_CONTEXT}"
    VAULT_ADDR = 'http://localhost:8200'
  }
  stages {
    stage('Validate') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh 'kubectl --context $KCTX apply --dry-run=server -f infra/k8s/agents/coordinator.yaml'
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
    stage('Deploy') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh '''
            kubectl --context $KCTX apply -f infra/k8s/agents/coordinator.yaml
            kubectl --context $KCTX -n kagent wait agent/coordinator --for=condition=Ready --timeout=180s
          '''
        }
      }
    }
    stage('Eval agent') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh 'EVAL_AGENT=coordinator bash scripts/run/eval_agent_ci.sh'
        }
      }
    }
    stage('Smoke A2A') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh '''
            set +x
            eval "$(scripts/setup/vault_env.sh agentgateway)"
            kubectl --context $KCTX -n agentgateway run smoke-$BUILD_NUMBER --rm -i --restart=Never \
              --image=curlimages/curl:8.11.1 --env=KEY="$AGW_AGENT_KEY" -- sh -c \
              'curl -sf -H "Authorization: Bearer $KEY" http://agentgateway:3000/a2a/coordinator/.well-known/agent-card.json' \
              | grep -q '"name":"coordinator"'
          '''
        }
      }
    }
  }
  post {
    always { archiveArtifacts artifacts: 'out/agent-eval*.json*', allowEmptyArchive: true }
  }
}
