package com.flowforge.runtime.architecture;

import com.flowforge.core.testing.ArchitectureRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RuntimeArchitectureTest {

    private static final JavaClasses RUNTIME = ArchitectureRules.productionClasses("com.flowforge.runtime");

    @Test
    @DisplayName("T00.2 runtime has no package cycles")
    void noPackageCycles() {
        ArchitectureRules.noPackageCycles().check(RUNTIME);
    }

    @Test
    @DisplayName("T00.2 runtime has no Spring, Kafka or Redis client dependencies")
    void hasNoFrameworkDependencies() {
        ArchitectureRules.runtimeHasNoFrameworkDependencies().check(RUNTIME);
    }

    @Test
    @DisplayName("T00.2 runtime depends only on core")
    void dependsOnlyOnCore() {
        ArchitectureRules.moduleDoesNotDependOn("runtime", "orchestrator", "worker", "systemtests", "e2e").check(RUNTIME);
    }
}
