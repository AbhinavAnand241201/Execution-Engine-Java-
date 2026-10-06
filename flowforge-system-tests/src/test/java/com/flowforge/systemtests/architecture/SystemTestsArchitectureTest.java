package com.flowforge.systemtests.architecture;

import com.flowforge.core.testing.ArchitectureRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SystemTestsArchitectureTest {

    // This module is test code only, so tests are imported too.
    private static final JavaClasses SYSTEM_TESTS = new ClassFileImporter().importPackages("com.flowforge.systemtests");

    @Test
    @DisplayName("T00.2 system tests have no package cycles")
    void noPackageCycles() {
        ArchitectureRules.noPackageCycles().check(SYSTEM_TESTS);
    }
}
