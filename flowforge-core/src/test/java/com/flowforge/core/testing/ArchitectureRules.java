package com.flowforge.core.testing;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import java.util.Arrays;

/**
 * The architecture rules, defined once and checked by every module's
 * {@code *ArchitectureTest}. Keeping them in the core test-jar means a rule can't silently drift between modules.
 *
 * <p>Rules allow an empty "should" because in early features some modules have no classes in the matched
 * packages yet. {@code CoreArchitectureTest} proves each rule still reports violations on fixture classes.
 */
public final class ArchitectureRules {

    /** Packages the pure-Java core must never touch. */
    public static final String[] CORE_FORBIDDEN_PACKAGES = {
        "org.springframework..",
        "jakarta.persistence..",
        "java.sql..",
        "javax.sql..",
        "jakarta.transaction..",
        "org.hibernate..",
        "org.apache.kafka..",
        "io.lettuce..",
        "redis.clients..",
        "org.springframework.data.redis..",
    };

    /** Runtime is plain Java + JDBC shared by both apps: no Spring, Kafka or Redis clients. */
    public static final String[] RUNTIME_FORBIDDEN_PACKAGES = {
        "org.springframework..",
        "org.apache.kafka..",
        "io.lettuce..",
        "redis.clients..",
    };

    private static final String ROOT = "com.flowforge";

    private ArchitectureRules() {
    }

    /** Imports the production classes of the given packages; test classes and fixtures are excluded. */
    public static JavaClasses productionClasses(String... packages) {
        return new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(packages);
    }

    /** T00.1: flowforge-core stays framework-free so the domain can be unit-tested and reused by every app. */
    public static ArchRule coreHasNoForbiddenDependencies() {
        return moduleDoesNotUse("core", CORE_FORBIDDEN_PACKAGES, "flowforge-core is pure Java");
    }

    /** T00.2: runtime stays framework-free so the orchestrator and worker can both embed it. */
    public static ArchRule runtimeHasNoFrameworkDependencies() {
        return moduleDoesNotUse("runtime", RUNTIME_FORBIDDEN_PACKAGES, "flowforge-runtime is plain Java + JDBC");
    }

    /**
     * T00.2: no two packages may depend on each other, directly or transitively. Every package under
     * {@code com.flowforge} is its own slice, including a module's root package (where the Spring Boot
     * application classes live).
     */
    public static ArchRule noPackageCycles() {
        return slices().matching(ROOT + ".(**)")
            .should().beFreeOfCycles()
            .allowEmptyShould(true);
    }

    /**
     * T00.2: module dependency direction. {@code module} is the package segment after
     * {@code com.flowforge}, e.g. {@code "runtime"}.
     */
    public static ArchRule moduleDoesNotDependOn(String module, String... forbiddenModules) {
        String[] forbiddenPackages = Arrays.stream(forbiddenModules)
            .map(forbidden -> ROOT + "." + forbidden + "..")
            .toArray(String[]::new);
        return noClasses().that().resideInAPackage(ROOT + "." + module + "..")
            .should().dependOnClassesThat().resideInAnyPackage(forbiddenPackages)
            .because("module dependencies point inwards: core <- runtime <- apps")
            .allowEmptyShould(true);
    }

    private static ArchRule moduleDoesNotUse(String module, String[] forbiddenPackages, String reason) {
        return noClasses().that().resideInAPackage(ROOT + "." + module + "..")
            .should().dependOnClassesThat().resideInAnyPackage(forbiddenPackages)
            .because(reason)
            .allowEmptyShould(true);
    }

    /**
     * T00.2: controllers go through application services, never persistence: neither our own
     * persistence package nor Spring Data repositories, {@code JdbcTemplate} or JPA. Paging types
     * ({@code org.springframework.data.domain}) and JDBC exception types stay allowed.
     */
    public static ArchRule apiDoesNotDependOnPersistence() {
        return noClasses().that().resideInAPackage("..api..")
            .should().dependOnClassesThat().resideInAnyPackage("..infrastructure.persistence..",
                "org.springframework.data.repository..", "org.springframework.jdbc.core..", "jakarta.persistence..")
            .because("controllers call application services only")
            .allowEmptyShould(true);
    }
}
