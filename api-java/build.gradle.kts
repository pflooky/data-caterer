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


plugins {
    java
    `maven-publish`

    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        url = uri("https://plugins.gradle.org/m2/")
    }
}

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaSpecificVersion")

    implementation(project(":api"))
}

testing {
    suites {
        // Configure the built-in test suite
        val test by getting(JvmTestSuite::class) {
            // Use JUnit4 test framework
            useJUnit("4.13.2")

            dependencies {
                implementation("org.scala-lang:scala-library:$scalaSpecificVersion")
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

tasks.shadowJar {
    archiveBaseName.set("datacaterer")
    archiveAppendix.set("api-java")
    archiveVersion.set(project.version.toString())
    archiveClassifier.set("")
    isZip64 = true
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
        create<MavenPublication>("mavenJava") {
            artifact(tasks.shadowJar)
            groupId = "org.data-catering"
            artifactId = "data-caterer-api-java"
            pom {
                name.set("Data Caterer API - Java")
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
