package com.flowforge.core.state;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * A state machine defined as data: a table from each state to the states it may move to. The whole lifecycle is
 * readable in one place (and rendered into docs/state-machines.md), and checking a transition is a set lookup.
 * The GoF State pattern would spread the same rules across one class per state (see ADR-010).
 *
 * <p>This check runs in one JVM. With many JVMs acting on the same row, the database enforces the same rule with a
 * conditional {@code UPDATE ... WHERE status = :from}; this class decides whether to try at all.
 *
 * @param <S> the status enum
 */
public final class StateMachine<S extends Enum<S>> {

    private final String entityType;
    private final Class<S> stateType;
    private final Map<S, Set<S>> allowed;

    private StateMachine(String entityType, Class<S> stateType, Map<S, Set<S>> allowed) {
        this.entityType = entityType;
        this.stateType = stateType;
        this.allowed = allowed;
    }

    public static <S extends Enum<S>> Builder<S> builder(String entityType, Class<S> stateType) {
        return new Builder<>(entityType, stateType);
    }

    public boolean canTransition(S from, S to) {
        return allowedFrom(from).contains(Objects.requireNonNull(to, "to"));
    }

    /**
     * Returns {@code to} if the move is allowed; otherwise throws {@link IllegalStateTransitionException}. Intended
     * use with the database:
     * <pre>{@code
     * TaskStatus next = TaskStateMachine.INSTANCE.transition(id, current, SUCCEEDED);
     * int rows = jdbc.update("UPDATE task_executions SET status = ? WHERE id = ? AND status = ?", next, id, current);
     * // rows == 0: someone else moved the task first; treat as a no-op
     * }</pre>
     */
    public S transition(UUID entityId, S from, S to) {
        if (!canTransition(from, to)) {
            throw new IllegalStateTransitionException(entityType, entityId, from, to);
        }
        return to;
    }

    /** States this one may move to (unmodifiable). */
    public Set<S> allowedFrom(S from) {
        return allowed.get(Objects.requireNonNull(from, "from"));
    }

    /** A terminal state has no way out. */
    public boolean isTerminal(S state) {
        return allowedFrom(state).isEmpty();
    }

    public Set<S> terminalStates() {
        EnumSet<S> terminal = EnumSet.noneOf(stateType);
        for (S state : stateType.getEnumConstants()) {
            if (isTerminal(state)) {
                terminal.add(state);
            }
        }
        return Collections.unmodifiableSet(terminal);
    }

    /** All states in declaration order. */
    public List<S> states() {
        return List.of(stateType.getEnumConstants());
    }

    public String entityType() {
        return entityType;
    }

    /** Collects the table; every state starts with no outgoing transitions (terminal) until {@link #allow} adds some. */
    public static final class Builder<S extends Enum<S>> {

        private final String entityType;
        private final Class<S> stateType;
        private final Map<S, EnumSet<S>> allowed;

        private Builder(String entityType, Class<S> stateType) {
            this.entityType = entityType;
            this.stateType = stateType;
            this.allowed = new EnumMap<>(stateType);
            for (S state : stateType.getEnumConstants()) {
                allowed.put(state, EnumSet.noneOf(stateType));
            }
        }

        @SafeVarargs
        public final Builder<S> allow(S from, S... to) {
            for (S target : to) {
                allowed.get(from).add(target);
            }
            return this;
        }

        public StateMachine<S> build() {
            Map<S, Set<S>> frozen = new EnumMap<>(stateType);
            allowed.forEach((from, to) -> frozen.put(from, Collections.unmodifiableSet(EnumSet.copyOf(to))));
            return new StateMachine<>(entityType, stateType, Collections.unmodifiableMap(frozen));
        }
    }
}
