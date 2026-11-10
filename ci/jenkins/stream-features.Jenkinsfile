// CI/CD for the stream feature job: test → build → deploy → verify.
// AWS (s3): TWO PyFlink jobs `ml-sku-return-24h-online` / `-offline` on the Flink ML 2.2 cluster (Compose).
// Local (minio): variant B Flink SQL `ml-sku-return-24h` + feast-pusher. Secrets live in Vault (secret/platform/*); Jenkins
// only holds the READ-ONLY token `vault-ci-token` (policy platform-read, created by scripts/setup/setup_vault.sh).
// The agent needs Docker, docker compose, uv; runs on a machine with the stack (local or a GCE VM).
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 30, unit: 'MINUTES') }
  parameters {
    string(name: 'IMAGE_REGISTRY', defaultValue: 'localhost:5001', description: 'Registry namespace prefix; use ECR for cloud')
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 's3 = Jenkins on EC2: .env + AWS/stream overlay, Flink variant from pipeline A')
  }
  environment {
    DATA_STORAGE = "${params.DATA_STORAGE}"
    CI_STREAM = '1'
    REGISTRY = "${params.IMAGE_REGISTRY}"
    TAG = "${env.GIT_COMMIT ?: env.BUILD_NUMBER}"
    POSTGRES_DB = 'sources'
    COMPOSE = 'scripts/run/ci_compose.sh --profile stream --profile flink --profile ml-stream --profile flink-ml'
  }
  stages {
    stage('Test') {
      steps {
        sh 'uv sync --frozen'
        sh 'uv run ruff format --check feature_repo ml flink_jobs tests/flink && uv run ruff check feature_repo ml flink_jobs tests/flink'
        // Feast logic + view definitions; Spark tests for daily features run in the Airflow image.
        sh 'uv run pytest -q tests/unit/test_docstrings.py tests/unit/generator/test_publish_order_lifecycle_events.py'
        sh '''docker run --rm --network none --read-only --tmpfs /tmp:rw,exec,size=1g \
              --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
              platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_sku_daily_features.py'''
      }
    }
    stage('Build') {
      steps {
        sh 'docker compose --profile stream --profile flink --profile ml-stream --profile flink-ml build flink-jobmanager feast-pusher flink-ml-jobmanager'
        // The PyFlink job runs for real on a mini cluster inside the image (Java + PyFlink + Feast): windows match an independent count.
        sh 'docker run --rm --network none --hostname localhost --tmpfs /tmp:exec -e HOME=/tmp -v "$PWD":/w:ro -w /w -e PYTHONDONTWRITEBYTECODE=1 platform-flink-ml:2.2.1 python -m pytest -q -p no:cacheprovider tests/flink'
      }
    }
    stage('Push') {
      steps { sh 'scripts/run/push_images.sh platform-flink:1.20.3-kafka3.4.0 platform-feast:0.66.0 platform-flink-ml:2.2.1' }
    }
    stage('Release config') {
      steps {
        sh 'mkdir -p out'
        sh 'scripts/run/compose_release_images.sh flink-jobmanager=platform-flink flink-taskmanager=platform-flink feast-pusher=platform-feast flink-ml-jobmanager=platform-flink-ml flink-ml-taskmanager=platform-flink-ml > out/ci-compose-images.yaml'
      }
    }
    stage('Deploy') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) {
          withEnv(['VAULT_ADDR=http://localhost:8200']) {
            // AWS: the REAL EC2 .env; the Feast registry is managed by DAG feast_apply (Airflow) ⇒ no apply from CI; S3 via IAM.
            sh '[ "$DATA_STORAGE" != s3 ] || install -m 600 /repo/.env .env'
            sh '''if [ "$DATA_STORAGE" = s3 ]; then
                    # AWS: Flink ML 2.2 cluster, two PyFlink jobs (online → EKS Redis, offline → S3 Parquet); no pusher.
                    $COMPOSE up --no-build --pull always -d --wait kafka flink-ml-jobmanager flink-ml-taskmanager
                    COMPOSE="$COMPOSE" scripts/run/run_stream_feature_jobs.sh
                  else
                    $COMPOSE up --no-build --pull always -d --wait kafka flink-jobmanager flink-taskmanager
                    set +x; eval "$(scripts/setup/vault_env.sh minio)"
                    # Local: register new views (if features.py changed) before the pusher reads the registry.
                    $COMPOSE run --rm --no-deps feast-pusher feast apply
                    $COMPOSE up --no-build --pull always -d --force-recreate feast-pusher
                    # Cancel any job with the same name then resubmit ⇒ never two jobs running in parallel.
                    scripts/run/run_sku_return_feature.sh
                  fi'''
          }
        }
      }
    }
    stage('Verify') {
      steps {
        // AWS: exactly 2 PyFlink jobs RUNNING on Flink ML; local: 1 SQL job + pusher running.
        sh '''if [ "$DATA_STORAGE" = s3 ]; then port=${FLINK_ML_UI_PORT:-8086}; want="ml-sku-return-24h-online ml-sku-return-24h-offline"
              else port=${FLINK_UI_PORT:-8082}; want="ml-sku-return-24h"; fi
              for i in $(seq 1 30); do
                curl -fsS localhost:$port/jobs/overview | WANT="$want" python3 -c "import json,os,sys
running=sorted(j['name'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING')
sys.exit(0 if all(running.count(w) == 1 for w in os.environ['WANT'].split()) else 1)" && exit 0
                sleep 5
              done; exit 1'''
        sh '[ "$DATA_STORAGE" = s3 ] || $COMPOSE ps feast-pusher --format "{{.State}}" | grep -qx running'
      }
    }
  }
  post {
    failure { sh '$COMPOSE logs --tail 50 feast-pusher flink-jobmanager flink-ml-jobmanager flink-ml-taskmanager || true' }
    always { sh 'rm -f .env' }
  }
}
