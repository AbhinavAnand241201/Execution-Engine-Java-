package com.flowforge.core.architecture.fixture.api;

import com.flowforge.core.architecture.fixture.infrastructure.persistence.FixtureRepository;

/** Deliberate violation: an api class reaching into persistence. Used only to prove the rule fires. */
public class FixtureController {

    private final FixtureRepository repository = new FixtureRepository();

    public String load() {
        return repository.find();
    }
}
