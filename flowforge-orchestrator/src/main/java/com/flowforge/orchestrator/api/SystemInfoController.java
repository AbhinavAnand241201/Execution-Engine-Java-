package com.flowforge.orchestrator.api;

import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Public "what is running" endpoint: lets a deploy check confirm the expected build is live. */
@RestController
@RequestMapping("/api/v1/system")
@SecurityRequirements // public: no bearer token in the API docs
public class SystemInfoController {

    /** {@code commit} is "unknown" when the build had no git metadata (e.g. built outside a checkout). */
    public record SystemInfo(String name, String version, String commit) {
    }

    private final SystemInfo info;

    public SystemInfoController(@Value("${spring.application.name}") String name, ObjectProvider<BuildProperties> build,
                                ObjectProvider<GitProperties> git) {
        BuildProperties buildProperties = build.getIfAvailable();
        GitProperties gitProperties = git.getIfAvailable();
        this.info = new SystemInfo(name,
            buildProperties == null ? "dev" : buildProperties.getVersion(),
            gitProperties == null || gitProperties.getShortCommitId() == null ? "unknown" : gitProperties.getShortCommitId());
    }

    @GetMapping("/info")
    public SystemInfo info() {
        return info;
    }
}
