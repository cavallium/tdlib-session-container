#!/usr/bin/env groovy
// see https://jenkins.io/doc/book/pipeline/syntax/

pipeline {
	environment {
		MVN_SET = credentials('maven_settings')
		JAVA_TOOL_OPTIONS = '-Duser.home=/var/maven'
	}
	agent any
	options {
		timestamps()
		ansiColor("xterm")
	}
	parameters {
		booleanParam(name: "RELEASE",
			description: "Build a release from current commit.",
			defaultValue: false)
	}
	stages {
		stage("Setup workspace") {
			agent none
			steps {
				sh "mkdir -p \"/var/jenkins_cache/.m2\""
				sh "chown 1000:1000 -R \"/var/jenkins_cache/.m2\""
				sh "mkdir -p \"/var/jenkins_cache/.ccache\""
				sh "chown 1000:1000 -R \"/var/jenkins_cache/.ccache\""
			}
		}

		stage("Build & Deploy SNAPSHOT") {
			agent {
				docker {
					image 'maven:3.6.3-openjdk-11'
					args '-v $HOME:/var/maven'
					reuseNode true
				}
			}
			steps {
				sh "mvn -s $MVN_SET -B deploy"
			}
		}

		stage("Release") {
			agent {
				docker {
					image 'maven:3.6.3-openjdk-11'
					args '-v $HOME:/var/maven'
					reuseNode true
				}
			}
			when {
				expression { params.RELEASE }
			}
			steps {
				sh "git config user.email \"jenkins@mchv.eu\""
				sh "git config user.name \"Jenkins\""
				sh "cd ${workspace}"
				sh "git add --all || true"
				sh "git commit -m \"Add generated files\" || true"
				sh "mvn -B -s $MVN_SET -Drevision=${BUILD_NUMBER} clean deploy"
			}
		}
	}
	post {
		always {
			/* clean up directory */
			deleteDir()
		}
	}
}
