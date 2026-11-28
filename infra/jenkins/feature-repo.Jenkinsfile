// CI/CD for the Feast feature definitions (feature_store/): test → feast plan → feast apply.
// Registering features is a code change, not a data change: this job runs when features.py / feature_store.yaml change,
// started by hand like the other deploy jobs (the human approval). The nightly DAG ml_feature_materialize only loads data.
// plan/apply run INSIDE the running Airflow container through the DAG's own run_feast(), so they use exactly the
// registry, offline paths, Redis address and credentials of the data pipeline (Airflow Variables / Connections, EC2 IAM role).
// No rollback: `feast apply` is declarative, so a bad apply is undone by reverting features.py and running this job again.

// Local: compose needs no secrets for `exec`; AWS: the REAL EC2 .env so compose resolves the AWS overlay.
FEAST_SH = { String command -> """
  [ "\$DATA_STORAGE" != s3 ] || install -m 600 /repo/.env .env
  scripts/shared/ci_compose.sh --profile airflow exec -T airflow python -c \\
    "import sys; sys.path.insert(0, '/opt/airflow/dags'); from ml_feature_materialize import run_feast; run_feast('${command}')"
""" }

pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 20, unit: 'MINUTES') }
  parameters {
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 's3 = Jenkins on EC2: uses the machine .env + AWS overlay')
  }
  environment {
    DATA_STORAGE = "${params.DATA_STORAGE}"
  }
  stages {
    stage('Test') {
      steps {
        sh '''uv run --python 3.12 pytest -q tests/unit/dags/test_ml_feature_materialize.py tests/unit/test_feature_names.py \
              tests/unit/test_feature_publication.py tests/unit/test_replay_return_events.py'''
      }
    }
    // Shows which entities / views / sources would be added, changed or deleted; nothing is written.
    stage('feast plan') {
      steps { sh FEAST_SH('plan') }
    }
    stage('feast apply') {
      steps { sh FEAST_SH('apply') }
    }
  }
  post {
    cleanup { sh 'rm -f .env' }
  }
}
