// A/B gate for the small LLM (diagram: Qwen 0.5B "B: candidate, gets 50%, kept only if it wins A/B"). Manual job:
// scale B up → eval A and B on the same SKUs → record keep/drop in agentregistry (data-agent-b@stable) → apply the
// gateway (B in or out of the split) → scale B down if it lost. A losing B is a result, not a failed build.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 60, unit: 'MINUTES') }
  parameters {
    string(name: 'TARGET_CONTEXT', defaultValue: 'kind-platform', description: 'Kubecontext from the deployment kubeconfig')
    string(name: 'KUBE_CREDENTIAL', defaultValue: 'kind-kubeconfig', description: 'Jenkins file credential containing the target kubeconfig')
    string(name: 'EVAL_SKUS', defaultValue: '', description: 'SKUs to evaluate both branches on; empty = local set sku-0000x, AWS (pipeline A) fills APP-…')
  }
  environment {
    KCTX = "${params.TARGET_CONTEXT}"
    EVAL_SKUS = "${params.EVAL_SKUS}"
    VAULT_ADDR = 'http://localhost:8200'
  }
  stages {
    stage('A/B gate') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh 'python3 -m scripts.llm.ab_gate'
        }
      }
    }
  }
  post {
    always { archiveArtifacts artifacts: 'out/agent-eval-data-agent*-features*.json*', allowEmptyArchive: true }
  }
}
