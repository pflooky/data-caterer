import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.scoverage.ScoverageExtension

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala application project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.5.1/userguide/building_java_projects.html
 * This project uses @Incubating APIs which are subject to change.
 */
val scalaVersion: String by project
val scalaSpecificVersion: String by project
val sparkVersion: String by project


plugins {
    scala
    application

    id("org.scoverage") version "8.0.3"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("com.github.jk1.dependency-license-report") version "2.5"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        url = uri("https://plugins.gradle.org/m2/")
    }
}

val basicImpl: Configuration by configurations.creating
val advancedImpl: Configuration by configurations.creating

configurations {
    implementation {
        extendsFrom(basicImpl)
        extendsFrom(advancedImpl)
    }
}

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaSpecificVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    compileOnly(project(":api"))

    // additional spark
    basicImpl("org.apache.spark:spark-avro_$scalaVersion:$sparkVersion")
    basicImpl("org.apache.spark:spark-protobuf_$scalaVersion:$sparkVersion")
    basicImpl("org.apache.spark:spark-hadoop-cloud_$scalaVersion:$sparkVersion")

    // connectors
    // jdbc
    basicImpl("org.postgresql:postgresql:42.6.0")
    advancedImpl("mysql:mysql-connector-java:8.0.33")
    // cassandra
    advancedImpl("com.datastax.spark:spark-cassandra-connector_$scalaVersion:3.3.0")
    // http
    advancedImpl("org.dispatchhttp:dispatch-core_$scalaVersion:1.2.0")  //TODO switch to https://github.com/AsyncHttpClient/async-http-client
    advancedImpl("io.swagger.parser.v3:swagger-parser-v3:2.1.16")
    // kafka
    advancedImpl("org.apache.spark:spark-sql-kafka-0-10_$scalaVersion:$sparkVersion")
    // jms
    //TODO advancedImpl("jakarta.jms:jakarta.jms-api:3.1.0") jms 3.x
    advancedImpl("javax.jms:javax.jms-api:2.0.1")
    advancedImpl("com.solacesystems:sol-jms:10.21.0")

    // data generation helpers
    basicImpl("net.datafaker:datafaker:1.9.0")
    basicImpl("org.reflections:reflections:0.10.2")

    // misc
    basicImpl("joda-time:joda-time:2.12.5")
    basicImpl("com.google.guava:guava:31.1-jre")
    basicImpl("com.github.pureconfig:pureconfig_$scalaVersion:0.17.2")
    basicImpl("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.2")
    basicImpl("com.fasterxml.jackson.module:jackson-module-scala_$scalaVersion:2.15.2")
    basicImpl("org.scala-lang.modules:scala-xml_$scalaVersion:2.2.0")
}

testing {
    suites {
        // Configure the built-in test suite
        val test by getting(JvmTestSuite::class) {
            // Use JUnit4 test framework
            useJUnit("4.13.2")

            dependencies {
                // Use Scalatest for testing our library
                implementation("org.scalatest:scalatest_$scalaVersion:3.2.17")
                implementation("org.scalatestplus:junit-4-13_$scalaVersion:3.2.17.0")
                implementation("org.scalamock:scalamock_$scalaVersion:5.2.0")
                implementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
                implementation("org.apache.spark:spark-avro_$scalaVersion:$sparkVersion")
                implementation("org.apache.spark:spark-protobuf_$scalaVersion:$sparkVersion")
                implementation(project(":api"))

                // Need scala-xml at test runtime
                runtimeOnly("org.scala-lang.modules:scala-xml_$scalaVersion:1.2.0")
            }
        }
    }
}

application {
    // Define the main class for the application.
    mainClass.set("com.github.pflooky.datagen.App")
}

sourceSets {
    test {
        resources {
            setSrcDirs(listOf("src/test/resources"))
        }
    }
}

tasks.shadowJar {
    isZip64 = true
    relocate("com.google.common", "shadow.com.google.common")
}

tasks.register<ShadowJar>("basicJar") {
    from(project.sourceSets.main.get().output)
    configurations = listOf(basicImpl)
    archiveBaseName.set("datacaterer")
    archiveAppendix.set("basic")
    archiveVersion.set(project.version.toString())
    isZip64 = true
    manifest = tasks.shadowJar.get().manifest
    exclude("META-INF/INDEX.LIST", "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "module-info.class")
    dependencies {
        exclude(dependency(project.dependencies.gradleApi()))
    }
    relocate("com.google.guava", "shadow.com.google.guava")
}

tasks.register<ShadowJar>("advancedJar") {
    from(project.sourceSets.main.get().output)
    configurations = listOf(basicImpl, advancedImpl)
    archiveBaseName.set("datacaterer")
    archiveAppendix.set("advanced")
    archiveVersion.set(project.version.toString())
    isZip64 = true
    exclude("META-INF/INDEX.LIST", "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "module-info.class")
    dependencies {
        exclude(dependency(project.dependencies.gradleApi()))
    }
    relocate("com.google.common", "shadow.com.google.common")
}

tasks.register("defineApplicationType") {
    val applicationType = providers.gradleProperty("applicationType").getOrElse("basic")
    if (applicationType.lowercase() == "advanced") setApplicationFlags(false) else setApplicationFlags(true)
}

tasks.compileScala {
    dependsOn(tasks["defineApplicationType"])
}

tasks.test {
    finalizedBy(tasks.reportScoverage)
}

configure<ScoverageExtension> {
    scoverageScalaVersion.set(scalaSpecificVersion)
    excludedFiles.add(".*CombinationCalculator.*")
    excludedPackages.add("com.github.pflooky.datagen.core.exception.*")
}

fun setApplicationFlags(isBasicBuild: Boolean) {
    val configParserFile = file("src/main/scala/com/github/pflooky/datagen/core/config/ConfigParser.scala")
    val mappedConfigParserLines = configParserFile.readLines()
        .map { line ->
            if (line.contains("val applicationType") && line.contains("advanced") && isBasicBuild) {
                line.replace("advanced", "basic")
            } else if (line.contains("val applicationType") && line.contains("basic") && !isBasicBuild) {
                line.replace("basic", "advanced")
            } else line
        }
    configParserFile.writeText(mappedConfigParserLines.joinToString("\n"))

    val dataSourceRegisterFile = file("src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister")
    if (isBasicBuild) {
        dataSourceRegisterFile.writeText("")
    } else {
        dataSourceRegisterFile.writeText("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    }
}

//tasks.create("depsize") {
//    listConfigurationDependencies(configurations.default.get())
//}

fun listConfigurationDependencies(configuration: Configuration) {
    val size = configuration.sumOf { it.length() / (1024 * 1024) }

    val out = StringBuffer()
    out.append("\nConfiguration name: \"${configuration.name}\"\n")
    if (size > 0) {
        out.append("Total dependencies size:".padEnd(65))
        out.append("$size Mb\n\n")

        configuration.sortedBy { -it.length() }
            .take(10)
            .forEach {
                out.append(it.name.padEnd(65))
                out.append("${it.length() / 1024} kb\n")
            }
    } else {
        out.append("No dependencies found")
    }
    println(out)
}

