plugins {
	application

	id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.github.pflooky"
version = "0.0.1-SNAPSHOT"

java {
	sourceCompatibility = JavaVersion.VERSION_17
	toolchain {
		languageVersion.set(JavaLanguageVersion.of(17))
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("com.slack.api:bolt:1.36.1")
	implementation("com.slack.api:bolt-servlet:1.36.1")
	implementation("com.slack.api:bolt-jetty:1.36.1")
	implementation("org.slf4j:slf4j-simple:2.0.5")
	implementation("joda-time:joda-time:2.12.5")
}

application {
	mainClass.set("com.github.pflooky.datacatererutils.DataCatererUtilsApplication")
}

tasks.withType<Jar> {
	manifest {
		attributes["Main-Class"] = "com.github.pflooky.datacatererutils.DataCatererUtilsApplication"
	}
}
