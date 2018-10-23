@Library('sapPipeline') _

// Set the target version information.  
def version = "0.2"
env.version = version
def TARGET_VERSION = version + "." + currentBuild.number
env.TARGET_VERSION = TARGET_VERSION
def packageName = "Gungnir-assembly"

if (!isMaterBranch()) {
        def branch = env.BRANCH_NAME.replaceAll('\\/','-')
        packageName = "${packageName}-${branch}"
    }else{
        packageName = "${packageName}"
}

// Set the title (beyond the build number) that will be used.
// Set the display name of the build to include both build number and version number.
def TARGET_TITLE = "v" + TARGET_VERSION

currentBuild.displayName= '#' + currentBuild.number + ' ' + TARGET_TITLE
env.TARGET_TITLE = currentBuild.displayName

def CURRENT_DATE = new Date().format("yyy/MM/dd")
def PCIA20Room = "Y2lzY29zcGFyazovL3VzL1JPT00vMjY1ZjI5YzAtOWQxZS0xMWU3LWIwZjYtMTc0NjBiNmY4N2Nk"

def repoHTMLUrl = "https://sqbu-github.cisco.com/SAP/Gungnir"
def repoName = "Gungnir"

// need to change if add package
def TarChoice = '''Gungnir-assembly'''
def LabMap = ["Gungnir-assembly":"Gungnir-batch-topCount-monthly,Gungnir-batch-topCount-weekly,Gungnir-batch-topCount,Gungnir-stream-monthly-aggregates,Gungnir-stream-weekly-aggregates,Gungnir-stream-activeUser,Gungnir-stream_registeredEndpoint,Gungnir-stream_callQuality,Gungnir_stream_splitData,Gungnir-stream_callDuration,Gungnir-stream_autoLicense,Gungnir-stream_fileUsed"]
def BTSMap = ["Gungnir-assembly":"Gungnir-batch-topCount-monthly,Gungnir-batch-topCount-weekly,Gungnir-batch-topCount,Gungnir-stream-monthly-aggregates,Gungnir-stream-weekly-aggregates,Gungnir-stream-activeUser,Gungnir-stream_registeredEndpoint,Gungnir-stream_callQuality,Gungnir_stream_splitData,Gungnir-stream_callDuration,testForPiPeLine_BTS3,Gungnir-stream_autoLicense,Gungnir-stream_fileUsed,testForPiPeLine_BTS2"]
def ProdMap = ["Gungnir-assembly":"Gungnir-batch-topCount-monthly,Gungnir-batch-topCount-weekly,Gungnir-batch-topCount,Gungnir-stream-monthly-aggregates,Gungnir-stream-weekly-aggregates,Gungnir-stream-activeUser,Gungnir-stream_registeredEndpoint,Gungnir-stream_callQuality,Gungnir_stream_splitData,Gungnir-stream_callDuration,Gungnir-stream_autoLicense,Gungnir-stream_fileUsed,testForPiPeLine_Prod2,testForPiPeLine_Prod4"]

def String LabDesc = " "
LabMap.each{component, jobs->
          LabDesc = LabDesc + "<li><b>${component}</b>: ${jobs}"
              }   
              
def String BTSDesc = " "
BTSMap.each{component, jobs->
          BTSDesc = BTSDesc + "<li><b>${component}</b>: ${jobs}"
              }     

def String ProdDesc =" "
ProdMap.each{component, jobs->
          ProdDesc = ProdDesc + "<li><b>${component}</b>: ${jobs}"  
} 

node ("Linux") {
    deleteDir()
    def workspace = pwd()
    echo workspace
    def lastCommitHash
    def lastCommitUserEmail
    
  stage('Get Code') {
      checkout scm
 if (!isPRBuild() && !isMaterBranch()) {
         lastCommitUserEmail = getLatestCommitUser()
       }
   }
 /*  
   stage('Build and UT') {
      try {
         // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-0.13.13'
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
                  sparkSend(roomId: PCIA20Room, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              } else {
                  sparkSend(toPersonEmail: lastCommitUserEmail, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              }
          }
      }
   }
*/  
//   stage('Upload Test Results') {
//      junit '**/target/test-reports/*.xml'
//      jacoco exclusionPattern: '**/com/cisco/gungnir/pipelines/**/*'
//   }
 
  stage('Package') {
    try {
           // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-0.13.13'
           sh "${tool name: 'sbt-1.0.3', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt 'set test in assembly := {}' clean assembly"
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
                  sparkSend(toPersonEmail: lastCommitUserEmail, html: message, repoHTMLUrl: repoHTMLUrl, repoName: repoName)
              }
          }
      }
  }
  
  stage('Upload Artifacts') {
    sh """
    cd target/scala-2.11
     tar cvf ${packageName}-${version}-${env.BUILD_NUMBER}.tar Gungnir-assembly-0.2-SNAPSHOT.jar
     mv ${packageName}-${version}-${env.BUILD_NUMBER}.tar ../../
     cd ../..
    """
    archive "${packageName}-${version}-${env.BUILD_NUMBER}.tar"
    
    sh """
                sha256sum ${packageName}-${version}-${env.BUILD_NUMBER}.tar >> Gungnir.sha   
                curl -F "file=@${packageName}-${version}-${env.BUILD_NUMBER}.tar" -F "sha256=<Gungnir.sha" https://rmc.webex.com/api/package/SparkKafka/centos7.2_64
            """
  }
  
  stage('deploy to Lab') {
               if (isPRBuild() || isMaterBranch()) {
                    sparkDeploy(roomId: PCIA20Room ,repoName: repoName, repoHTMLUrl:repoHTMLUrl, env:"Lab")
                } else {
                    sparkDeploy(toPersonEmail: lastCommitUserEmail ,repoName: repoName, repoHTMLUrl:repoHTMLUrl, env:"Lab")
                }
      generateInputParameters(TarChoice:TarChoice.toString(), Desc:LabDesc.toString(), env:"Lab", map:LabMap, roomId:PCIA20Room, repoName:repoName, repoHTMLUrl:repoHTMLUrl, lastCommitUserEmail:lastCommitUserEmail)        
   }
    
   stage('deploy to BTS') {
           if (isPRBuild() || isMaterBranch()) {
                    sparkDeploy(roomId: PCIA20Room ,repoName: repoName, repoHTMLUrl: repoHTMLUrl, env:"BTS")
                } else {
                    sparkDeploy(toPersonEmail: lastCommitUserEmail ,repoName: repoName, repoHTMLUrl: repoHTMLUrl, env:"BTS")
                }
      generateInputParameters(TarChoice:TarChoice.toString(), Desc:BTSDesc.toString(), env:"BTS", map:BTSMap, roomId:PCIA20Room, repoName:repoName, repoHTMLUrl:repoHTMLUrl, lastCommitUserEmail:lastCommitUserEmail)
   }
   
   stage('deploy to Prod') {
                if (isPRBuild() || isMaterBranch()) {
                    sparkDeploy(roomId: PCIA20Room ,repoName: repoName, repoHTMLUrl:repoHTMLUrl, env:"Prod")
                } else {
                    sparkDeploy(toPersonEmail: lastCommitUserEmail ,repoName: repoName, repoHTMLUrl:repoHTMLUrl, env:"Prod")
               }
      generateInputParameters(TarChoice:TarChoice.toString(), Desc:ProdDesc.toString(), env:"Prod", map:ProdMap, roomId:PCIA20Room, repoName:repoName, repoHTMLUrl:repoHTMLUrl, lastCommitUserEmail:lastCommitUserEmail)
   }
}
