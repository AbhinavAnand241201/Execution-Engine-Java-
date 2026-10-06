package com.flowforge.core.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.architecture.fixture.api.FixtureController;
import com.flowforge.core.architecture.fixture.jdbc.UsesDataSource;
import com.flowforge.core.architecture.fixture.jdbc.UsesJdbc;
import com.flowforge.core.testing.ArchitectureRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CoreArchitectureTest {

    private static final JavaClasses CORE = ArchitectureRules.productionClasses("com.flowforge.core");

    @Test
    @DisplayName("T00.1 core has no Spring, JPA, JDBC, Kafka or Redis dependencies")
    void coreHasNoForbiddenDependencies() {
        ArchitectureRules.coreHasNoForbiddenDependencies().check(CORE);
    }

    @Test
    @DisplayName("T00.1 forbidden-dependency rule reports a core class that uses java.sql")
    void forbiddenDependencyRuleDetectsViolation() {
        JavaClasses fixture = new ClassFileImporter().importClasses(UsesJdbc.class);

        assertThat(ArchitectureRules.coreHasNoForbiddenDependencies().evaluate(fixture).hasViolation()).isTrue();
    }

    @Test
    @DisplayName("T00.1 forbidden-dependency rule reports a core class that uses javax.sql")
    void forbiddenDependencyRuleDetectsJavaxSql() {
        JavaClasses fixture = new ClassFileImporter().importClasses(UsesDataSource.class);

        assertThat(ArchitectureRules.coreHasNoForbiddenDependencies().evaluate(fixture).hasViolation()).isTrue();
    }

    @Test
    @DisplayName("T00.2 core has no package cycles")
    void coreHasNoPackageCycles() {
        ArchitectureRules.noPackageCycles().check(CORE);
    }

    @Test
    @DisplayName("T00.2 core depends on no other FlowForge module")
    void coreDependsOnNoOtherModule() {
        ArchitectureRules.moduleDoesNotDependOn("core", "runtime", "orchestrator", "worker", "systemtests", "e2e")
            .check(CORE);
    }

    @Test
    @DisplayName("T00.2 cycle rule reports two packages that depend on each other")
    void cycleRuleDetectsViolation() {
        JavaClasses fixture = new ClassFileImporter().importPackages("com.flowforge.core.architecture.fixture.cycle");

        assertThat(ArchitectureRules.noPackageCycles()
            .evaluate(fixture).hasViolation()).isTrue();
    }

    @Test
    @DisplayName("T00.2 module-direction rule reports a dependency on a forbidden package")
    void moduleRuleDetectsViolation() {
        JavaClasses fixture = new ClassFileImporter().importClasses(FixtureController.class);

        // Module names are package segments under com.flowforge, so fixture packages stand in for modules here.
        assertThat(ArchitectureRules.moduleDoesNotDependOn(
                "core.architecture.fixture.api", "core.architecture.fixture.infrastructure")
            .evaluate(fixture).hasViolation()).isTrue();
    }

    @Test
    @DisplayName("T00.2 layering rule reports an api class that uses persistence")
    void apiRuleDetectsViolation() {
        JavaClasses fixture = new ClassFileImporter().importClasses(FixtureController.class);

        assertThat(ArchitectureRules.apiDoesNotDependOnPersistence().evaluate(fixture).hasViolation()).isTrue();
    }
}
