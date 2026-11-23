// CI/CD RAG Data Pipeline: test chunk/lineage → check DAG imports → build Airflow image
// (feast-venv with feast[postgres] + fastembed) → restart Airflow → trigger DAG ml_rag_ingest and wait for the result.
// Credentials come from Vault via the read-only token `vault-ci-token`.
// Rollback (diagram "Deploy to EC2 · rolls back if a check fails"): a release that passes every check is kept as
// out/ci-compose-images.good.yaml; a failure after Deploy started redeploys that release and re-checks it. Limit: the
// DAG code is bind-mounted from the workspace (gap D2), so rollback restores the image, not the dags/ code.

// Deploy and its health check, used as-is by the stages AND by the rollback, so the two paths cannot drift apart.
// Local: temporary .env from .env.example + Vault, setup_airflow loads the Connections. AWS (s3): the REAL EC2 .env
// (Ansible, /repo/.env) + AWS overlay; Airflow config is in environment variables so no setup_airflow.
DEPLOY_SH = '''
  if [ "$DATA_STORAGE" = s3 ]; then
    install -m 600 /repo/.env .env
  else
    { cat .env.example
      scripts/shared/vault_env.sh minio | sed 's/^export //'
      scripts/shared/vault_env.sh postgres | sed 's/^export //'; } > .env
  fi
  scripts/shared/ci_compose.sh --profile airflow up --no-build --pull always -d --wait airflow
  if [ "$DATA_STORAGE" != s3 ]; then python3 -m scripts.data.setup_airflow; rm -f .env; fi
'''
CHECK_SH = '''scripts/shared/ci_compose.sh --profile airflow exec -T airflow python -c "
from airflow.models import DagBag
b = DagBag('/opt/airflow/dags', include_examples=False)
assert 'ml_rag_ingest' in b.dags and not b.import_errors, b.import_errors"'''

pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  parameters {
    string(name: 'IMAGE_REGISTRY', defaultValue: 'localhost:5001', description: 'Registry namespace prefix; use ECR for cloud')
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 's3 = Jenkins on EC2: uses the machine .env + AWS overlay')
  }
  environment {
    DATA_STORAGE = "${params.DATA_STORAGE}"
    VAULT_ADDR = 'http://localhost:8200'
    REGISTRY = "${params.IMAGE_REGISTRY}"
    TAG = "${env.GIT_COMMIT ?: env.BUILD_NUMBER}"
  }
  stages {
    stage('Test') {
      steps { sh 'uv run --python 3.12 pytest rag/tests -q' }
    }
    stage('Build') {
      steps { sh 'docker compose --profile airflow build airflow' }
    }
    stage('Push') {
      steps { sh 'python3 -m scripts.shared.push_images platform-airflow:2.10.5-spark3.5.3' }
    }
    stage('Release config') {
      steps {
        sh 'mkdir -p out'
        sh 'python3 -m scripts.shared.compose_release_images airflow=platform-airflow > out/ci-compose-images.yaml'
      }
    }
    stage('Deploy') {
      steps {
        script { env.DEPLOY_STARTED = 'true' }  // from here on a failure has touched the running service
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) { sh DEPLOY_SH }
      }
    }
    stage('DAG import') {
      steps { sh CHECK_SH }
    }
    stage('Run DAG') {
      steps {
        sh '''
          run=ci_$BUILD_NUMBER
          scripts/shared/ci_compose.sh --profile airflow exec -T airflow airflow dags trigger ml_rag_ingest -r $run
          for i in $(seq 1 90); do
            state=$(scripts/shared/ci_compose.sh --profile airflow exec -T airflow airflow dags list-runs -d ml_rag_ingest -o plain \
              | awk -v r="$run" '$2 == r {print $3}')
            echo "state=$state"; [ "$state" = success ] && exit 0; [ "$state" = failed ] && exit 1; sleep 10
          done; exit 1
        '''
      }
    }
    stage('Keep as last good') {
      steps { sh 'cp out/ci-compose-images.yaml out/ci-compose-images.good.yaml' }
    }
  }
  post {
    failure {
      script {
        if (env.DEPLOY_STARTED != 'true') {
          echo 'Failed before Deploy: nothing was changed, no rollback needed.'
        } else if (!fileExists('out/ci-compose-images.good.yaml')) {
          echo 'ROLLBACK NOT POSSIBLE: no release has passed every check yet (first deploy). The failed release keeps running.'
        } else {
          sh 'cp out/ci-compose-images.good.yaml out/ci-compose-images.yaml && echo "Rolling back to:" && cat out/ci-compose-images.yaml'
          withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) { sh DEPLOY_SH }
          sh CHECK_SH
          echo 'Rolled back to the last good release; the build stays failed.'
        }
      }
    }
    // cleanup runs after failure (always would run first and delete the .env the rollback needs).
    cleanup { sh 'rm -f .env' }
  }
}
