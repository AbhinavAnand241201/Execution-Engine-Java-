package com.flowforge.orchestrator.api;

import com.flowforge.orchestrator.application.Role;
import com.flowforge.orchestrator.application.UserAdminService;
import com.flowforge.orchestrator.application.UserView;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Account administration. Protected twice on purpose: by URL in {@code SecurityConfig} and by {@code @PreAuthorize}
 * here, so moving the endpoint can't silently make it public.
 */
@RestController
@RequestMapping("/api/v1/admin/users")
public class AdminController {

    public record RoleChange(@NotNull Role role) {
    }

    private final UserAdminService users;

    public AdminController(UserAdminService users) {
        this.users = users;
    }

    @PatchMapping("/{id}/role")
    @PreAuthorize("hasRole('ADMIN')")
    public UserView changeRole(@PathVariable UUID id, @Valid @RequestBody RoleChange change) {
        return users.changeRole(id, change.role());
    }
}
