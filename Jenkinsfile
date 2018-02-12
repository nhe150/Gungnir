#!groovy
@Library('sapPipeline') _

def isMasterBranch() {
  return env.BRANCH_NAME.contains('master')
}

def repoHTMLUrl = "https://sqbu-github.cisco.com/SAP/Gungnir"
def repoName = "Gungnir"
def SparkRoom = "Y2lzY29zcGFyazovL3VzL1JPT00vN2FiYzYxNjAtMDQ2YS0xMWU3LTkxNjMtZjExOTM1MWRjNWIx"

node ("Linux") {
   stage('Git') {
      //git 'https://sqbu-github.cisco.com/SAP/Gungnir.git'
      checkout scm
   }
   stage('Build') {
       //def builds = [:]
       //builds['scala'] = {
      try {
           // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-0.13.13'
           sh "${tool name: 'sbt-1.0.3', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt compile test"
      } catch (err) {
          echo "Caught: ${err}"
          currentBuild.result = 'FAILURE'
      } finally {
          stage ('Notification') {
              if (currentBuild.result == 'FAILURE') {
                  message = "Build/UT failed."
              } else {
                  message = "Build/UT success."
              }

              if (isPRBuild() || isMaterBranch()) {
                  sparkSend(roomId: SparkRoom, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              } else {
                  sparkSend(toPersonEmail: env.CHANGE_AUTHOR_EMAIL, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              }
          }
      }
       //}
       //builds['others'] = {
       //    echo 'building others'
       //}
     //parallel builds
   }
   stage('Results') {
      junit '**/target/test-reports/*.xml'
   }
}
