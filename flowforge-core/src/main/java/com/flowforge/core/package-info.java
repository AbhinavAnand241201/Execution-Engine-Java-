/**
 * FlowForge's domain, kept in plain Java so it can be unit-tested without a container and shared by the
 * orchestrator and the worker. Spring, JPA, JDBC, Kafka and Redis are forbidden here (enforced by ArchUnit);
 * infrastructure is reached only through interfaces implemented by the apps.
 */
package com.flowforge.core;
