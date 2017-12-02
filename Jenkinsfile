node {
   stage('Git') {
      git 'https://sqbu-github.cisco.com/SAP/Gungnir.git'
   }
   stage('Build') {
       def builds = [:]
       builds['scala'] = {
           // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-0.13.13'
           sh "${tool name: 'sbt-1.0.3', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt compile test"
       }
       builds['others'] = {
           echo 'building others'
       }
     parallel builds
   }
   stage('Results') {
      junit '**/target/test-reports/*.xml'
   }
}