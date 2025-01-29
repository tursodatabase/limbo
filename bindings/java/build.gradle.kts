import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
    application
    id("net.ltgt.errorprone") version "3.1.0"

    // If you're stuck on JRE 8, use id 'com.diffplug.spotless' version '6.13.0' or older.
    id("com.diffplug.spotless") version "6.13.0"
}

group = "org.github.tursodatabase"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("ch.qos.logback:logback-classic:1.2.13")
    implementation("ch.qos.logback:logback-core:1.2.13")

    errorprone("com.uber.nullaway:nullaway:0.10.26") // maximum version which supports java 8
    errorprone("com.google.errorprone:error_prone_core:2.10.0") // maximum version which supports java 8

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.0")
}

application {
    mainClass.set("org.github.tursodatabase.Main")

    val limboSystemLibraryPath = System.getenv("LIMBO_SYSTEM_PATH")
    if (limboSystemLibraryPath != null) {
        applicationDefaultJvmArgs = listOf(
            "-Djava.library.path=${System.getProperty("java.library.path")}:$limboSystemLibraryPath"
        )
    }
}

tasks.test {
    useJUnitPlatform()
    // In order to find rust built file under resources, we need to set it as system path
    systemProperty(
        "java.library.path",
        "${System.getProperty("java.library.path")}:$projectDir/src/test/resources/limbo/debug"
    )

    // For our fancy test logging
    testLogging {
        // set options for log level LIFECYCLE
        events(
            TestLogEvent.FAILED,
            TestLogEvent.PASSED,
            TestLogEvent.SKIPPED,
            TestLogEvent.STANDARD_OUT
        )
        exceptionFormat = TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true

        // set options for log level DEBUG and INFO
        debug {
            events(
                TestLogEvent.STARTED,
                TestLogEvent.FAILED,
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.STANDARD_ERROR,
                TestLogEvent.STANDARD_OUT
            )
            exceptionFormat = TestExceptionFormat.FULL
        }
        info.events = debug.events
        info.exceptionFormat = debug.exceptionFormat

        afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ desc, result ->
            if (desc.parent == null) { // will match the outermost suite
                val output = "Results: ${result.resultType} (${result.testCount} tests, ${result.successfulTestCount} passed, ${result.failedTestCount} failed, ${result.skippedTestCount} skipped)"
                val startItem = "|  "


                val endItem = "  |"
                val repeatLength = startItem.length + output.length + endItem.length
                println("\n" + "-".repeat(repeatLength) + "\n" + startItem + output + endItem + "\n" + "-".repeat(repeatLength))
            }
        }))
    }
}

tasks.withType<JavaCompile> {
    options.errorprone {
        // Let's select which checks to perform. NullAway is enough for now.
        disableAllChecks = true
        check("NullAway", CheckSeverity.ERROR)

        option("NullAway:AnnotatedPackages", "org.github.tursodatabase")
        option(
            "NullAway:CustomNullableAnnotations",
            "org.github.tursodatabase.annotations.Nullable,org.github.tursodatabase.annotations.SkipNullableCheck"
        )
    }
    if (name.lowercase().contains("test")) {
        options.errorprone {
            disable("NullAway")
        }
    }
}

spotless {
    java {
        target("**/*.java")
        targetExclude(layout.buildDirectory.dir("**/*.java").get().asFile)
        removeUnusedImports()
        googleJavaFormat("1.7") // or use eclipse().configFile("path/to/eclipse-format.xml")
    }
}
