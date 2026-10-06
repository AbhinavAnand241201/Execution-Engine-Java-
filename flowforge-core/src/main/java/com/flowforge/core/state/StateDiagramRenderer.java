package com.flowforge.core.state;

/**
 * Renders a {@link StateMachine} as a Mermaid {@code stateDiagram-v2}, so the diagrams in the docs are produced from
 * the same table the code enforces and can't drift from it.
 */
public final class StateDiagramRenderer {

    private StateDiagramRenderer() {
    }

    /** Edges in declaration order of the enum, so the output is stable. */
    public static <S extends Enum<S>> String render(StateMachine<S> machine, S initial) {
        StringBuilder out = new StringBuilder("stateDiagram-v2\n");
        out.append("    [*] --> ").append(initial.name()).append('\n');
        for (S from : machine.states()) {
            for (S to : machine.states()) {
                if (machine.canTransition(from, to)) {
                    out.append("    ").append(from.name()).append(" --> ").append(to.name()).append('\n');
                }
            }
        }
        for (S state : machine.states()) {
            if (machine.isTerminal(state)) {
                out.append("    ").append(state.name()).append(" --> [*]\n");
            }
        }
        return out.toString();
    }
}
