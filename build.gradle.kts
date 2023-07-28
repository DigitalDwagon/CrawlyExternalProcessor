import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.JavaVersion.VERSION_17
import kotlin.text.Charsets.UTF_8

plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("io.freefair.lombok") version "8.0.1"
}

group = "dev.digitaldragon"
version = "1.0-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.mongodb:mongodb-driver-sync:4.9.0")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
}

tasks {
    build {
        dependsOn(shadowJar)
    }
    compileJava {
        options.encoding = UTF_8.name()

        // Set the release flag. This configures what version bytecode the compiler will emit, as well as what JDK APIs are usable.
        // See https://openjdk.java.net/jeps/247 for more information.
        options.release.set(17)
    }
    shadowJar {
        archiveFileName.set("external_crawly_processor-$version-shadow.jar")
        manifest.attributes["Main-Class"] = "dev.digitaldragon.ExternalCrawlyProcessor"
    }
    javadoc {
        options.encoding = UTF_8.name() // We want UTF-8 for everything
    }
    processResources {
        filteringCharset = UTF_8.name() // We want UTF-8 for everything
    }
}