package com.flowforge.orchestrator.api;

import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.Role;
import java.util.List;
import java.util.UUID;
import org.springframework.core.MethodParameter;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * Builds the {@link AuthenticatedUser} for {@code @CurrentUser} parameters from the already-verified JWT, so no
 * controller touches Spring Security types and services get a plain record.
 */
public class CurrentUserArgumentResolver implements HandlerMethodArgumentResolver {

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.hasParameterAnnotation(CurrentUser.class)
            && parameter.getParameterType().equals(AuthenticatedUser.class);
    }

    @Override
    public AuthenticatedUser resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                             NativeWebRequest webRequest, WebDataBinderFactory binderFactory) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !(authentication.getPrincipal() instanceof Jwt jwt)) {
            throw new AuthenticationCredentialsNotFoundException("no authenticated user");
        }
        // Only we can sign tokens, but a malformed one must still be a 401, not a 500.
        try {
            List<String> roles = jwt.getClaimAsStringList("roles");
            return new AuthenticatedUser(UUID.fromString(jwt.getSubject()), jwt.getClaimAsString("email"),
                Role.valueOf(roles.get(0)));
        } catch (RuntimeException malformed) {
            throw new BadCredentialsException("malformed token claims");
        }
    }
}
