package com.flowforge.orchestrator.architecture;

import com.flowforge.core.testing.ArchitectureRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OrchestratorArchitectureTest {

    private static final JavaClasses ORCHESTRATOR = ArchitectureRules.productionClasses("com.flowforge.orchestrator");

    @Test
    @DisplayName("T00.2 orchestrator has no package cycles")
    void noPackageCycles() {
        ArchitectureRules.noPackageCycles().check(ORCHESTRATOR);
    }

    @Test
    @DisplayName("T00.2 orchestrator depends only on core and runtime")
    void dependsOnlyOnCoreAndRuntime() {
        ArchitectureRules.moduleDoesNotDependOn("orchestrator", "worker", "systemtests", "e2e").check(ORCHESTRATOR);
    }

    @Test
    @DisplayName("T00.2 orchestrator api does not use persistence")
    void apiDoesNotUsePersistence() {
        ArchitectureRules.apiDoesNotDependOnPersistence().check(ORCHESTRATOR);
    }
}
