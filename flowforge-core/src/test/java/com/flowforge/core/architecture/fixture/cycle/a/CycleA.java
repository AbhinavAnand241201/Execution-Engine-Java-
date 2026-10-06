package com.flowforge.core.architecture.fixture.cycle.a;

import com.flowforge.core.architecture.fixture.cycle.b.CycleB;

/** Deliberate violation: half of a two-package cycle. Used only to prove the rule fires. */
public class CycleA {

    public CycleB next() {
        return new CycleB();
    }
}
