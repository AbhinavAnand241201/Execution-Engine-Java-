package com.flowforge.core.state;

import static com.flowforge.core.state.TaskStatus.CANCELLED;
import static com.flowforge.core.state.TaskStatus.FAILED_PERMANENTLY;
import static com.flowforge.core.state.TaskStatus.PENDING;
import static com.flowforge.core.state.TaskStatus.QUEUED;
import static com.flowforge.core.state.TaskStatus.READY;
import static com.flowforge.core.state.TaskStatus.RETRYING;
import static com.flowforge.core.state.TaskStatus.RUNNING;
import static com.flowforge.core.state.TaskStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flowforge.core.error.ErrorCode;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StateMachineTest {

    /** The lifecycle tables, written out independently of the production table so a typo in either one fails the test. */
    private static final Map<TaskStatus, Set<TaskStatus>> TASK_TABLE = Map.of(
        PENDING, EnumSet.of(READY, CANCELLED),
        READY, EnumSet.of(QUEUED, CANCELLED),
        QUEUED, EnumSet.of(RUNNING, CANCELLED),
        RUNNING, EnumSet.of(SUCCEEDED, RETRYING, FAILED_PERMANENTLY, CANCELLED),
        RETRYING, EnumSet.of(READY, CANCELLED),
        FAILED_PERMANENTLY, EnumSet.of(READY),
        SUCCEEDED, EnumSet.noneOf(TaskStatus.class),
        CANCELLED, EnumSet.noneOf(TaskStatus.class));

    private static final Map<WorkflowStatus, Set<WorkflowStatus>> WORKFLOW_TABLE = Map.of(
        WorkflowStatus.CREATED, EnumSet.of(WorkflowStatus.RUNNING),
        WorkflowStatus.RUNNING, EnumSet.of(WorkflowStatus.PAUSED, WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLING),
        WorkflowStatus.PAUSED, EnumSet.of(WorkflowStatus.RUNNING, WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLING),
        WorkflowStatus.CANCELLING, EnumSet.of(WorkflowStatus.CANCELLED),
        WorkflowStatus.FAILED, EnumSet.of(WorkflowStatus.RUNNING, WorkflowStatus.CANCELLED),
        WorkflowStatus.COMPLETED, EnumSet.noneOf(WorkflowStatus.class),
        WorkflowStatus.CANCELLED, EnumSet.noneOf(WorkflowStatus.class));

    private static final UUID ID = UUID.fromString("00000000-0000-4000-8000-000000000042");

    static Stream<Arguments> allowedTransitions() {
        return Stream.concat(
            pairs(TASK_TABLE, TaskStatus.values(), true).map(p -> Arguments.of(TaskStateMachine.INSTANCE, p[0], p[1])),
            pairs(WORKFLOW_TABLE, WorkflowStatus.values(), true).map(p -> Arguments.of(WorkflowStateMachine.INSTANCE, p[0], p[1])));
    }

    static Stream<Arguments> forbiddenTransitions() {
        return Stream.concat(
            pairs(TASK_TABLE, TaskStatus.values(), false).map(p -> Arguments.of(TaskStateMachine.INSTANCE, p[0], p[1])),
            pairs(WORKFLOW_TABLE, WorkflowStatus.values(), false).map(p -> Arguments.of(WorkflowStateMachine.INSTANCE, p[0], p[1])));
    }

    @ParameterizedTest(name = "{1} → {2}")
    @MethodSource("allowedTransitions")
    @DisplayName("T03.1 every allowed transition succeeds")
    <S extends Enum<S>> void allowedTransitionSucceeds(StateMachine<S> machine, S from, S to) {
        assertThat(machine.canTransition(from, to)).isTrue();
        assertThat(machine.transition(ID, from, to)).isEqualTo(to);
    }

    @ParameterizedTest(name = "{1} → {2}")
    @MethodSource("forbiddenTransitions")
    @DisplayName("T03.2 every pair not in the table throws (exhaustive S × S, including self transitions)")
    <S extends Enum<S>> void forbiddenTransitionThrows(StateMachine<S> machine, S from, S to) {
        assertThat(machine.canTransition(from, to)).isFalse();
        assertThatThrownBy(() -> machine.transition(ID, from, to))
            .isInstanceOfSatisfying(IllegalStateTransitionException.class, e -> {
                assertThat(e.errorCode()).isEqualTo(ErrorCode.ILLEGAL_STATE_TRANSITION);
                assertThat(e.from()).isEqualTo(from);
                assertThat(e.to()).isEqualTo(to);
            });
    }

    @Test
    @DisplayName("T03.1/T03.2 oracle check: the production tables are exactly the expected tables (no extra or missing edges)")
    void tablesMatchSpec() {
        assertThat(TASK_TABLE.keySet()).as("add the new state to the oracle table").isEqualTo(EnumSet.allOf(TaskStatus.class));
        assertThat(WORKFLOW_TABLE.keySet()).as("add the new state to the oracle table")
            .isEqualTo(EnumSet.allOf(WorkflowStatus.class));
        for (TaskStatus from : TaskStatus.values()) {
            assertThat(TaskStateMachine.INSTANCE.allowedFrom(from)).as("task %s", from).isEqualTo(TASK_TABLE.get(from));
        }
        for (WorkflowStatus from : WorkflowStatus.values()) {
            assertThat(WorkflowStateMachine.INSTANCE.allowedFrom(from)).as("workflow %s", from).isEqualTo(WORKFLOW_TABLE.get(from));
        }
    }

    @Test
    @DisplayName("T03.3 SUCCEEDED, CANCELLED, COMPLETED are terminal; failure states only exit to READY / RUNNING (or CANCELLED)")
    void terminalAndFailureStates() {
        assertThat(TaskStateMachine.INSTANCE.isTerminal(SUCCEEDED)).isTrue();
        assertThat(TaskStateMachine.INSTANCE.isTerminal(CANCELLED)).isTrue();
        assertThat(WorkflowStateMachine.INSTANCE.isTerminal(WorkflowStatus.COMPLETED)).isTrue();
        assertThat(WorkflowStateMachine.INSTANCE.isTerminal(WorkflowStatus.CANCELLED)).isTrue();

        assertThat(TaskStateMachine.INSTANCE.isTerminal(FAILED_PERMANENTLY)).isFalse();
        assertThat(TaskStateMachine.INSTANCE.allowedFrom(FAILED_PERMANENTLY)).containsExactly(READY);
        // A FAILED workflow can be retried (→ RUNNING) or given up on (→ CANCELLED); nothing else.
        assertThat(WorkflowStateMachine.INSTANCE.allowedFrom(WorkflowStatus.FAILED))
            .containsExactlyInAnyOrder(WorkflowStatus.RUNNING, WorkflowStatus.CANCELLED);

        assertThat(TaskStateMachine.INSTANCE.terminalStates()).containsExactlyInAnyOrder(SUCCEEDED, CANCELLED);
        assertThat(WorkflowStateMachine.INSTANCE.terminalStates())
            .containsExactlyInAnyOrder(WorkflowStatus.COMPLETED, WorkflowStatus.CANCELLED);
    }

    @Test
    @DisplayName("T03.4 the exception carries entity type, id, from and to, and maps to ILLEGAL_STATE_TRANSITION")
    void exceptionCarriesContext() {
        assertThatThrownBy(() -> WorkflowStateMachine.INSTANCE.transition(ID, WorkflowStatus.COMPLETED,
            WorkflowStatus.PAUSED))
            .isInstanceOfSatisfying(IllegalStateTransitionException.class, e -> {
                assertThat(e.entityType()).isEqualTo("workflow execution");
                assertThat(e.entityId()).isEqualTo(ID);
                assertThat(e.from()).isEqualTo(WorkflowStatus.COMPLETED);
                assertThat(e.to()).isEqualTo(WorkflowStatus.PAUSED);
                assertThat(e.errorCode()).isEqualTo(ErrorCode.ILLEGAL_STATE_TRANSITION);
                assertThat(e.getMessage())
                    .isEqualTo("Cannot move workflow execution " + ID + " from COMPLETED to PAUSED");
            });
    }

    @Test
    @DisplayName("T03.extra-1 attempt outcomes: TIMED_OUT, WORKER_LOST, WORKER_SHUTDOWN are always retryable")
    void alwaysRetryableOutcomes() {
        assertThat(EnumSet.allOf(AttemptOutcome.class).stream().filter(AttemptOutcome::isAlwaysRetryable))
            .containsExactlyInAnyOrder(AttemptOutcome.TIMED_OUT, AttemptOutcome.WORKER_LOST, AttemptOutcome.WORKER_SHUTDOWN);
    }

    private static <S extends Enum<S>> Stream<Object[]> pairs(Map<S, Set<S>> table, S[] states, boolean allowed) {
        List<Object[]> pairs = new ArrayList<>();
        for (S from : states) {
            for (S to : states) {
                if (table.get(from).contains(to) == allowed) {
                    pairs.add(new Object[] {from, to});
                }
            }
        }
        return pairs.stream();
    }
}
