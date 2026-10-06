package com.flowforge.worker.architecture;

import com.flowforge.core.testing.ArchitectureRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WorkerArchitectureTest {

    private static final JavaClasses WORKER = ArchitectureRules.productionClasses("com.flowforge.worker");

    @Test
    @DisplayName("T00.2 worker has no package cycles")
    void noPackageCycles() {
        ArchitectureRules.noPackageCycles().check(WORKER);
    }

    @Test
    @DisplayName("T00.2 worker depends only on core and runtime")
    void dependsOnlyOnCoreAndRuntime() {
        ArchitectureRules.moduleDoesNotDependOn("worker", "orchestrator", "systemtests", "e2e").check(WORKER);
    }

    @Test
    @DisplayName("T00.2 worker api does not use persistence")
    void apiDoesNotUsePersistence() {
        ArchitectureRules.apiDoesNotDependOnPersistence().check(WORKER);
    }
}
