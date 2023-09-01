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
    `maven-publish`

    id("org.scoverage") version "8.0.3"
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
    compileOnly("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.2")
    compileOnly("com.fasterxml.jackson.module:jackson-module-scala_$scalaVersion:2.15.2")
    compileOnly("org.dispatchhttp:dispatch-core_$scalaVersion:1.2.0")

    implementation("com.softwaremill.quicklens:quicklens_$scalaVersion:1.9.6")
}

testing {
    suites {
        // Configure the built-in test suite
        val test by getting(JvmTestSuite::class) {
            // Use JUnit4 test framework
            useJUnit("4.13.2")

            dependencies {
                // Use Scalatest for testing our library
                implementation("org.scalatest:scalatest_$scalaVersion:3.2.10")
                implementation("org.scalatestplus:junit-4-13_$scalaVersion:3.2.2.0")
                implementation("org.scalamock:scalamock_$scalaVersion:5.2.0")

                // Need scala-xml at test runtime
                runtimeOnly("org.scala-lang.modules:scala-xml_$scalaVersion:1.2.0")
            }
        }
    }
}

sourceSets {
    test {
        resources {
            setSrcDirs(listOf("src/test/resources"))
        }
    }
}

tasks.test {
    finalizedBy(tasks.reportScoverage)
}

configure<ScoverageExtension> {
    scoverageScalaVersion.set(scalaSpecificVersion)
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/pflooky/data-caterer")
            credentials {
                username = "pflooky"
                password = System.getenv("PACKAGE_TOKEN")
            }
        }
    }
    publications {
        create<MavenPublication>("mavenScala") {
            from(components["java"])
            groupId = "org.data-catering"
            artifactId = "data-caterer-api"
            pom {
                name.set("Data Caterer API")
                description.set("API for discovering, generating and validating data using Data Caterer")
                url.set("https://pflooky.github.io/data-caterer-docs/")
                developers {
                    developer {
                        id.set("pflooky")
                        name.set("Peter Flook")
                        email.set("peter.flook@data.catering")
                    }
                }
            }
        }
    }
}
