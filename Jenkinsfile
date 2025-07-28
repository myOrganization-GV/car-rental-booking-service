pipeline {
    agent any

    tools {
        maven 'Maven3'
    }

    environment {
        APP_NAME = "car-rental-booking-service"
        RELEASE = "1.0.0"
        DOCKER_USER = "gukami98"
        DOCKER_PASS = credentials('DOCKERHUB_LOGIN')
        IMAGE_NAME = "${DOCKER_USER}/${APP_NAME}"
        IMAGE_TAG = "latest"
        GCP_PROJECT  = 'voltaic-circuit-459820-q9'
        GCP_ZONE     = 'us-central1-c'              
        GCP_INSTANCE = 'instance-20250728-083500'
    }

    stages {
        stage("Cleanup Workspace") {
            steps {
                cleanWs()
            }
        }

        stage("Checkout Common") {
            steps {
                git branch: 'main', credentialsId: 'GITHUB_LOGIN', url: 'https://github.com/myOrganization-GV/car-rental-common'
            }
        }
        stage("Build Common Application") {
            steps {
                sh "mvn clean install -DskipTests"
            }
        }

        stage("Checkout Booking Service") {
            steps {
                git branch: 'main', credentialsId: 'GITHUB_LOGIN', url: 'https://github.com/myOrganization-GV/car-rental-booking-service'
            }
        }
        stage("Build Orchestrator Application") {
            steps {
                sh "mvn clean package -DskipTests"
            }
        }
        stage("Build and Push Docker Image") {
            steps {
                script {
                    def jarPath = "target/car-rental-booking-service-0.0.1-SNAPSHOT.jar"
                    if (!fileExists(jarPath)) {
                        error "JAR file not found at ${jarPath}"
                    }

                    docker.withRegistry('https://index.docker.io/v1/', 'DOCKERHUB_LOGIN') {
                        dockerImage = docker.build("${IMAGE_NAME}:${IMAGE_TAG}")
                        dockerImage.push("${IMAGE_TAG}")
                        dockerImage.push('latest')
                    }

                    sh "docker logout"
                }
            }
        }

        stage("Deploy to GCP VM") {
            steps {
               
                withCredentials([file(credentialsId: 'GCP_SA_KEY', variable: 'KEYFILE')]) {
                    sh '''
                        gcloud auth activate-service-account --key-file=$KEYFILE --quiet
                        gcloud config set project $GCP_PROJECT --quiet
                        gcloud config set compute/zone $GCP_ZONE --quiet

                        gcloud compute ssh gui_s@${GCP_INSTANCE} \\
                        --zone=${GCP_ZONE} --quiet \\
                        --command "cd ~/car-rental-app && docker compose pull car-rental-orchestrator && docker compose up -d --no-deps --force-recreate $APP_NAME"
                    '''
                }
            }
        }
    }
}
