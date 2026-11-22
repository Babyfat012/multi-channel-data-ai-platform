// CD return-risk model (diagram D12 → D14): show challenger vs champion → ML Engineer approves → move MLflow alias
// `production` → deploy KServe (scripts/ml/deploy_return_risk.py reads that alias). Training (KFP) only sets
// `challenger`; this pipeline is the ONLY writer of `production`, and disableConcurrentBuilds keeps it single.
pipeline {
  // No agent at the top: the Approve stage waits up to a day and must not hold the only executor (jenkins.yaml).
  agent none
  options { timestamps(); disableConcurrentBuilds() }
  parameters {
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 's3 = Jenkins on EC2: the machine .env, S3 + Pod Identity on EKS')
    string(name: 'IMAGE_REGISTRY', defaultValue: 'localhost:5001', description: 'Registry namespace prefix; use ECR for cloud')
    string(name: 'TARGET_CONTEXT', defaultValue: 'kind-platform', description: 'Kubecontext from the deployment kubeconfig')
    string(name: 'KUBE_CREDENTIAL', defaultValue: 'kind-kubeconfig', description: 'Jenkins file credential containing the target kubeconfig')
  }
  environment {
    PLATFORM_STORAGE = "${params.DATA_STORAGE}"
    REGISTRY = "${params.IMAGE_REGISTRY}"
    KCTX = "${params.TARGET_CONTEXT}"
    VAULT_ADDR = 'http://localhost:8200'
    TAG = "${env.GIT_COMMIT ?: env.BUILD_NUMBER}"
    MLFLOW = 'uv run --no-project --with mlflow-skinny==3.17.0 python'
  }
  stages {
    stage('Show challenger') {
      agent any
      steps {
        script {
          // stderr = the comparison for the reviewer (build log); stdout = the version under review.
          env.CHALLENGER = sh(returnStdout: true, script: '$MLFLOW -m scripts.ml.promote_return_risk show').trim()
        }
      }
    }
    stage('Approve') {
      steps {
        timeout(time: 24, unit: 'HOURS') {
          script {
            env.APPROVER = input(message: "Promote return-risk v${env.CHALLENGER} to production? Read the comparison above.",
                                 ok: 'Approve', submitterParameter: 'APPROVER')
          }
        }
      }
    }
    stage('Promote') {
      agent any
      steps {
        // Re-checks that CHALLENGER is still the challenger: a training run during the wait must not slip through.
        sh '$MLFLOW -m scripts.ml.promote_return_risk promote --version "$CHALLENGER" --approved-by "$APPROVER"'
      }
    }
    stage('Deploy model') {
      agent any
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: params.KUBE_CREDENTIAL, variable: 'KUBECONFIG')]) {
          // deploy_return_risk loads .env (bucket, region, MinIO keys): AWS = the REAL EC2 .env; local = example + Vault.
          sh '''
            set +x
            if [ "$PLATFORM_STORAGE" = s3 ]; then
              install -m 600 /repo/.env .env
            else
              { cat .env.example; scripts/shared/vault_env.sh minio | sed 's/^export //'; } > .env
            fi
            $MLFLOW -m scripts.ml.deploy_return_risk
          '''
        }
      }
      post { always { sh 'rm -f .env' } }
    }
  }
}
