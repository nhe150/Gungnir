#!groovy
@Library('sapPipeline') _

def repoHTMLUrl = "https://sqbu-github.cisco.com/SAP/Gungnir"
def repoName = "Gungnir"
def SparkRoom = "Y2lzY29zcGFyazovL3VzL1JPT00vMjY1ZjI5YzAtOWQxZS0xMWU3LWIwZjYtMTc0NjBiNmY4N2Nk"
def latestCommitUser
node ("Linux") {
  initializeEnv(repoName) 
  stage('Git') {
      //git 'https://sqbu-github.cisco.com/SAP/Gungnir.git'
      checkout scm
   }
  
  if (!isPRBuild() && !isMaterBranch()) {
    latestCommitUser = getLatestCommitUser()
  }
   stage('Build') {
       //def builds = [:]
       //builds['scala'] = {
      try {
           // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-0.13.13'
           //sh "${tool name: 'sbt-1.0.3', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt compile test"
         sh "${tool name: 'sbt-1.0.3', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt clean assembly coverage test coverageReport jacoco"
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
                  sparkSend(toPersonEmail: latestCommitUser, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              }
          }
      }
       //}
       //builds['others'] = {
       //    echo 'building others'
       //}
     //parallel builds
   }
   stage('Upload Test Results') {
      junit '**/target/test-reports/*.xml'
      jacoco exclusionPattern: '**/PipelineRunner.class, **/DataVolumeMonitor.class'
   }
  stage('Package') {
    try {
           // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-0.13.13'
           sh "${tool name: 'sbt-1.0.3', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt package"
      } catch (err) {
          echo "Caught: ${err}"
          currentBuild.result = 'FAILURE'
      } finally {
          stage ('Notification') {
              if (currentBuild.result == 'FAILURE') {
                  message = "Package failed."
              } else {
                  message = "Package success."
              }

              if (isPRBuild() || isMaterBranch()) {
                  sparkSend(roomId: SparkRoom, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              } else {
                  sparkSend(toPersonEmail: latestCommitUser, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              }
          }
      }
  }
  stage('Upload Artifacts') {
    archive "target/scala-2.11/gungnir_*.jar"
    if (isMaterBranch()) {        
        //sh """
        //        cd target/
        //        tar cvf [Gungnir].tar [Gungnir].jar
        //    """
        //    sh """
        //        sha256sum target/[Gungnir]-1.0-${env.BUILD_NUMBER}.tar >> [Gungnir].sha
        //        curl -F "file=@target/[Gungnir]-1.0-${env.BUILD_NUMBER}.tar" -F "sha256=<[Gungnir].sha" https://rmc.webex.com/api/package/SparkKafka/centos7.2_64
        //    """
    }       
  }
}
