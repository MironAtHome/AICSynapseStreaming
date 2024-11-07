import scala.xml._
import java.io.File
import java.nio.file.CopyOption
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.StandardCopyOption

val printDeps = taskKey[Unit]("Print Dependencies")
val buildRelease = taskKey[Unit]("Build Release")
val buildDebug = taskKey[Unit]("Build Debug")

ThisBuild / scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

fork := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled", "-Djna.nosys=true")

lazy val sbc = (project in file("."))
  .settings(name := "Spark SQL Streaming Scaled")
  .settings(resolvers += Resolver.sonatypeRepo("public"))
  .settings(resolvers += DefaultMavenRepository) 
  .settings(libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.4")
  .settings(libraryDependencies += "org.apache.spark"  % "spark-sql_2.11"  % "2.4.4")
  .settings(libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.4")
  .settings(libraryDependencies += "org.apache.httpcomponents" % "httpasyncclient" % "4.1.4")
  .settings(libraryDependencies += "org.apache.httpcomponents" % "httpcore-nio" % "4.4.13")
  .settings(libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12")
  .settings(libraryDependencies += "org.apache.httpcomponents" % "httpcore" % "4.4.13")
  .settings(libraryDependencies += "commons-codec" % "commons-codec" % "1.11")
  .settings(libraryDependencies += "commons-io" % "commons-io" % "2.7")
  .settings(libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6")
  .settings(libraryDependencies += "com.microsoft.hyperspace" %% "hyperspace-core" % "0.1.0")
// https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
  .settings(libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "8.3.1.jre8-preview")
  .settings(libraryDependencies += "com.azure" % "azure-core-test" % "1.3.1" % Test )
  .settings(libraryDependencies += "org.jmockit" % "jmockit" % "1.34" % Test)
  .settings(printDeps := {
    libraryDependencies.value.foreach((x) =>
      if (! x.organization.equals("org.apache.spark") && !x.organization.equals("org.scala-lang") && !x.toString().endsWith("test"))
        println(x.toString()))})
  .settings(buildRelease := {
    val fs = FileSystems.getDefault()
    val packDirPath = fs.getPath(baseDirectory.value.toString(), "target", "pack", "lib")
    val releaseDirPath = fs.getPath(baseDirectory.value.toString(), "target", "release")
    val xmlDepsDirPath = fs.getPath(baseDirectory.value.toString(), ".idea", "libraries")
    val xmlDepsDir = new File(xmlDepsDirPath.toString())
    val packDir = new File(packDirPath.toString())
    val releaseDir = new File(releaseDirPath.toString())

    if (!releaseDir.exists()) {
      releaseDir.mkdir()
    }

    xmlDepsDir.listFiles("*.xml").foreach((file) =>
      libraryDependencies.value.foreach((x) => {
        if (!x.organization.equals("org.apache.spark") && !x.organization.equals("org.scala-lang") && !x.toString().endsWith("test")) {
          val xml = XML.loadFile(file)
          val moduleNameString =  (xml \\ "component" \\ "library" \\ "@name").text
          if (moduleNameString.contains(x.toString())) {
            val jarFileDecoratedString: String = (xml \\ "component" \\ "library" \\ "CLASSES" \\ "root" \\ "@url").text
            val jarFilePathArray: Array[String] = jarFileDecoratedString.split("/")
            val jarFileName = jarFilePathArray(jarFilePathArray.length - 1).replace("!", "")
            val jarFilePackPath = fs.getPath(packDir.getPath(), jarFileName)
            val jarFileReleasePath = fs.getPath(releaseDir.getPath(), jarFileName)
            val jarFilePack = new File(jarFilePackPath.toString())
            if (!jarFilePack.exists()) {
              throw new Exception(s"File ${jarFilePackPath.toString()} does not exist. Please run pack command")
            } else {
              val jarFileRelease = new File(jarFileReleasePath.toString())
              if(!jarFileRelease.exists()) {
                val option :CopyOption = StandardCopyOption.REPLACE_EXISTING
                Files.copy(jarFilePackPath, jarFileReleasePath, option)
              }
            }
          }
        }
      })
    )
    val (art, artFile) = (Compile / packageBin / packagedArtifact).value
    if (!artFile.exists()) {
      throw new Exception("File ${artFile.getPath()} does not exist. Please run package command")
    } else {
      val option :CopyOption = StandardCopyOption.REPLACE_EXISTING
      val releaseArtPath = fs.getPath(releaseDir.getPath(),artFile.getName())
      Files.copy(fs.getPath(artFile.getPath()), releaseArtPath, option)
  }})
  .settings(buildDebug := {
    val fs = FileSystems.getDefault()
    val packDirPath = fs.getPath(baseDirectory.value.toString(), "target", "pack", "lib")
    val debugDirPath = fs.getPath(baseDirectory.value.toString(), "target", "debug")
    val xmlDepsDirPath = fs.getPath(baseDirectory.value.toString(), ".idea", "libraries")
    val xmlDepsDir = new File(xmlDepsDirPath.toString())
    val packDir = new File(packDirPath.toString())
    val debugDir = new File(debugDirPath.toString())

    if (!debugDir.exists()) {
      debugDir.mkdir()
    }

    xmlDepsDir.listFiles("*.xml").foreach((file) =>
      libraryDependencies.value.foreach((x) => {
        if (!x.organization.equals("org.apache.spark") && !x.organization.equals("org.scala-lang")) {
          val xml = XML.loadFile(file)
          val moduleNameString =  (xml \\ "component" \\ "library" \\ "@name").text
          if (moduleNameString.contains(x.toString())) {
            val jarFileDecoratedString: String = (xml \\ "component" \\ "library" \\ "CLASSES" \\ "root" \\ "@url").text
            val jarFilePathArray: Array[String] = jarFileDecoratedString.split("/")
            val jarFileName = jarFilePathArray(jarFilePathArray.length - 1).replace("!", "")
            val jarFilePackPath = fs.getPath(packDir.getPath(), jarFileName)
            val jarFileDebugPath = fs.getPath(debugDir.getPath(), jarFileName)
            val jarFilePack = new File(jarFilePackPath.toString())
            if (!jarFilePack.exists()) {
              throw new Exception(s"File ${jarFilePackPath.toString()} does not exist. Please run pack command")
            } else {
              val jarFileDebug = new File(jarFileDebugPath.toString())
              if(!jarFileDebug.exists()) {
                val option :CopyOption = StandardCopyOption.REPLACE_EXISTING
                Files.copy(jarFilePackPath, jarFileDebugPath, option)
              }
            }
          }
        }
      })
    )
    val (art, artFile) = (Compile / packageBin / packagedArtifact).value
    if (!artFile.exists()) {
      throw new Exception(s"File ${artFile.getPath()} does not exist. Please run package command")
    } else {
      val option :CopyOption = StandardCopyOption.REPLACE_EXISTING
      val debugArtPath = fs.getPath(debugDir.getPath(),artFile.getName())
      Files.copy(fs.getPath(artFile.getPath()), debugArtPath, option)
    }})
  .enablePlugins(PackPlugin)