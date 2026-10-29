#!/usr/bin/env groovy

@Library('lib@openeo_opensearch_client')_

def jdk_version = '11'

javaPipeline {
  package_name  = 'openeo-opensearch-client'
  maven_version = '3.9.11'
  create_git_tag_job = true
  wipeout_workspace = true
  dev_repository = 'openeo-maven-snapshot-local'
  prod_repository = 'openeo-maven-release-local'
}
