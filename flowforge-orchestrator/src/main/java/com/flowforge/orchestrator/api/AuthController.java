package com.flowforge.orchestrator.api;

import com.flowforge.orchestrator.application.AccessToken;
import com.flowforge.orchestrator.application.AuthService;
import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.UserView;
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** Registration, login and "who am I". Registration always creates a USER; higher roles are granted by an ADMIN. */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    /** Password length is checked by the service, which reports WEAK_PASSWORD rather than a generic validation error. */
    public record Credentials(@NotBlank @Email @Size(max = 254) String email, @NotBlank @Size(max = 200) String password) {
    }

    private final AuthService auth;

    public AuthController(AuthService auth) {
        this.auth = auth;
    }

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    @SecurityRequirements
    public UserView register(@Valid @RequestBody Credentials credentials) {
        return auth.register(credentials.email(), credentials.password());
    }

    @PostMapping("/login")
    @SecurityRequirements
    public AccessToken login(@Valid @RequestBody Credentials credentials) {
        return auth.login(credentials.email(), credentials.password());
    }

    @GetMapping("/me")
    public UserView me(@CurrentUser AuthenticatedUser caller) {
        return auth.me(caller);
    }
}
