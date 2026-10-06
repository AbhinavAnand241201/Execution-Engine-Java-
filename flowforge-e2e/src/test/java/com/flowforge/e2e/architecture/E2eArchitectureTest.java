package com.flowforge.e2e.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.flowforge.core.testing.ArchitectureRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class E2eArchitectureTest {

    // This module is test code only, so tests are imported too.
    private static final JavaClasses E2E = new ClassFileImporter().importPackages("com.flowforge.e2e");

    @Test
    @DisplayName("T00.2 e2e tests have no package cycles")
    void noPackageCycles() {
        ArchitectureRules.noPackageCycles().check(E2E);
    }

    @Test
    @DisplayName("T00.2 e2e tests are black-box: they use only the REST API, no FlowForge classes")
    void isBlackBox() {
        // The architecture package itself uses the shared rules from the core test-jar; everything else is black-box.
        noClasses().that().resideInAPackage("com.flowforge.e2e..")
            .and().resideOutsideOfPackage("com.flowforge.e2e.architecture..")
            .should().dependOnClassesThat().resideInAnyPackage(
                "com.flowforge.core..", "com.flowforge.runtime..", "com.flowforge.orchestrator..",
                "com.flowforge.worker..", "com.flowforge.systemtests..")
            .allowEmptyShould(true)
            .check(E2E);
    }
}
