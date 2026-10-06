/**
 * Task execution that both the orchestrator (local dispatch mode) and the worker need: executors that touch the
 * network or the database, claiming, and input loading. Plain Java + JDBC, depends only on flowforge-core.
 */
package com.flowforge.runtime;
