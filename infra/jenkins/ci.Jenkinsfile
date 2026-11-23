// CI on every commit to main (diagram D1 → D2: Developer pushes to GitHub, Jenkins runs the tests). Polled every
// 5 minutes (jenkins.yaml); tests only, no build and no deploy: the deploy pipelines stay manual until a staging
// step with approval exists. Same commands as .github/workflows/ci.yml (tests/unit/test_registry_cicd.py keeps
// them equal), so Jenkins and GitHub Actions cannot disagree about whether a commit is green.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 20, unit: 'MINUTES') }
  stages {
    stage('Install') {
      steps { sh 'uv sync --frozen --python 3.12.13' }
    }
    stage('Ruff') {
      steps {
        sh 'uv run ruff format --check .'
        sh 'uv run ruff check .'
      }
    }
    stage('Contract registry') {
      steps { sh 'uv run python -m spark_jobs.lib.data_contracts check-registry --contracts-dir contracts' }
    }
    stage('Pytest') {
      steps { sh 'uv run pytest -q' }
    }
  }
}
