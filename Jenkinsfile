pipeline {
  agent any

  environment {
    CFY_MANAGER_URL = credentials('CFY_MANAGER_URL')  // or set as a plain env var
    CFY_USERNAME    = credentials('CFY_USERNAME')
    CFY_PASSWORD    = credentials('CFY_PASSWORD')
    CFY_TENANT      = credentials('CFY_TENANT')
    CFY_INSECURE    = "true"
  }

  stages {
    stage('Checkout') {
      steps { checkout scm }
    }

    stage('Install deps') {
      steps {
        sh '''
          python3 -m venv .venv
          . .venv/bin/activate
          pip install -r requirements.txt
        '''
      }
    }

    stage('Deploy to Cloudify') {
      steps {
        sh '''
          . .venv/bin/activate
          python3 scripts/cloudify_deploy.py \
            --blueprint-id hello-bp \
            --blueprint-dir blueprints/hello \
            --application-file blueprint.yaml \
            --deployment-id hello-dev \
            --inputs-file inputs/dev.yaml \
            --workflow install \
            --wait
        '''
      }
    }
  }
}
