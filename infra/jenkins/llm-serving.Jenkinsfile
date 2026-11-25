// LLM serving (diagram: "llm-serving"): model A = llama.cpp × 2 behind the llm-d EPP, branch B = llama.cpp 0.5B, both behind
// agentgateway (infra/k8s/agents/llm.yaml, ab-llm-small.yaml, agentgateway.yaml). Manual job, like the other deploys.
// Deploy → Smoke (EPP sees every replica, one chat through gateway → EPP) → A/B gate (keep B only if it wins: scale B up,
// eval A and B on the same SKUs, record keep/drop in agentregistry, B in or out of the gateway split, scale B down if it lost).
// No automatic rollback: the replicas restart with Recreate, so a bad change is undone by reverting and rerunning.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 90, unit: 'MINUTES') }
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
    stage('Deploy llama.cpp + llm-d EPP') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // First start downloads the model into each replica (~1 GB): up to 10 minutes.
          sh '''
            kubectl --context $KCTX apply -f infra/k8s/agents/llm.yaml -f infra/k8s/agents/agentgateway.yaml
            kubectl --context $KCTX -n agentgateway rollout restart deploy/agentgateway
            kubectl --context $KCTX -n llm rollout status deploy/epp --timeout=5m
            kubectl --context $KCTX -n llm rollout status deploy/qwen --timeout=15m
            kubectl --context $KCTX -n agentgateway rollout status deploy/agentgateway --timeout=5m
          '''
        }
      }
    }
    stage('Smoke') {
      steps {
        withCredentials([file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          sh '''
            kubectl --context $KCTX -n agentgateway port-forward svc/agentgateway 18300:3000 >/dev/null & pf=$!
            trap "kill $pf" EXIT
            sleep 3
            export AGW_AGENT_KEY=$(kubectl --context $KCTX -n agentgateway get secret agentgateway-keys -o jsonpath='{.data.AGW_AGENT_KEY}' | base64 -d)
            python3 -m scripts.llm.smoke_llm_serving
          '''
        }
      }
    }
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
