package com.flowforge.core.architecture.fixture.cycle.b;

import com.flowforge.core.architecture.fixture.cycle.a.CycleA;

/** Deliberate violation: the other half of a two-package cycle. Used only to prove the rule fires. */
public class CycleB {

    public CycleA back() {
        return new CycleA();
    }
}
