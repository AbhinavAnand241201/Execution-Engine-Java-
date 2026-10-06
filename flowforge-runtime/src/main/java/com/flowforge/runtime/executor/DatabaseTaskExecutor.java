package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.ConfigChecks.invalid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * Runs one of a fixed set of named, read-only queries. Users pick a query by name and pass parameters; they never
 * send SQL, so a workflow author can't read other tables or write anything. Defence in depth: the connection is
 * read-only, the transaction is {@code READ ONLY} (PostgreSQL rejects writes even if a configured query tried one),
 * every statement has a {@code statement_timeout}, and at most {@code maxRows} rows are fetched.
 */
public final class DatabaseTaskExecutor implements TaskExecutor {

    private static final Set<String> FIELDS = Set.of("query", "params");
    /** PostgreSQL SQLSTATE classes/codes we can classify. */
    private static final String QUERY_CANCELED = "57014";
    private static final String CONNECTION_EXCEPTION_CLASS = "08";
    private static final String INSUFFICIENT_RESOURCES_CLASS = "53";
    /** Admin shutdown, crash shutdown, cannot connect now, serialization failure, deadlock. */
    private static final Set<String> TRANSIENT_STATES = Set.of("57P01", "57P02", "57P03", "40001", "40P01");

    private final DataSource dataSource;
    private final DatabaseExecutorSettings settings;
    private final Map<String, NamedQuery> queries;
    private final ObjectMapper json;

    public DatabaseTaskExecutor(DataSource dataSource, DatabaseExecutorSettings settings, ObjectMapper json) {
        this.dataSource = dataSource;
        this.settings = settings;
        this.json = json;
        this.queries = settings.queries().entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> NamedQuery.parse(e.getValue())));
    }

    @Override
    public TaskType supportedType() {
        return TaskType.DATABASE;
    }

    @Override
    public List<ValidationError> validateConfig(JsonNode config) {
        List<ValidationError> errors = new ArrayList<>();
        String name = ConfigChecks.requiredText(config, "query", errors);
        NamedQuery query = name == null ? null : queries.get(name);
        if (name != null && query == null) {
            errors.add(invalid("query", "unknown query '" + name + "'; known: " + new TreeSet<>(queries.keySet())));
        }
        JsonNode params = config.get("params");
        if (params != null && !params.isObject()) {
            errors.add(invalid("params", "params must be an object"));
        } else if (query != null) {
            Set<String> needed = new LinkedHashSet<>(query.parameters());
            for (String parameter : needed) {
                JsonNode value = params == null ? null : params.get(parameter);
                if (value == null) {
                    errors.add(invalid("params." + parameter, "parameter '" + parameter + "' is required by query '" + name
                        + "'"));
                } else if (!value.isValueNode() || value.isNull()) {
                    errors.add(invalid("params." + parameter, "parameter values must be strings, numbers or booleans"));
                }
            }
            if (params != null) {
                params.fieldNames().forEachRemaining(field -> {
                    if (!needed.contains(field)) {
                        errors.add(invalid("params." + field, "query '" + name + "' has no parameter '" + field + "'"));
                    }
                });
            }
        }
        ConfigChecks.rejectUnknownFields(config, FIELDS, errors);
        return errors;
    }

    @Override
    public TaskResult execute(TaskContext context) throws InterruptedException {
        List<ValidationError> problems = validateConfig(context.config());
        if (!problems.isEmpty()) {
            return TaskResult.failure("INVALID_TASK_CONFIG", problems.getFirst().message(), false);
        }
        NamedQuery query = queries.get(context.config().get("query").asText());
        JsonNode params = context.config().path("params");
        try (Connection connection = dataSource.getConnection()) {
            connection.setReadOnly(true);
            connection.setAutoCommit(false);
            try {
                try (Statement setup = connection.createStatement()) {
                    setup.execute("SET TRANSACTION READ ONLY");
                    setup.execute("SET LOCAL statement_timeout = " + settings.statementTimeout().toMillis());
                }
                ObjectNode output = run(connection, query, params, context);
                if (output == null) {
                    return TaskResult.cancelled();
                }
                return TaskResult.success(output);
            } finally {
                rollbackQuietly(connection);
            }
        } catch (SQLException e) {
            if (context.cancellation().isCancelled()) {
                return TaskResult.cancelled();
            }
            return classify(e);
        }
    }

    private ObjectNode run(Connection connection, NamedQuery query, JsonNode params, TaskContext context)
        throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(query.jdbcSql())) {
            context.cancellation().onCancel(() -> cancelQuietly(statement));
            if (context.cancellation().isCancelled()) {
                return null;   // statement.cancel() does nothing on an idle statement
            }
            statement.setMaxRows(settings.maxRows() + 1);
            for (int i = 0; i < query.parameters().size(); i++) {
                bind(statement, i + 1, params.get(query.parameters().get(i)));
            }
            ArrayNode rows = json.createArrayNode();
            boolean truncated = false;
            try (ResultSet resultSet = statement.executeQuery()) {
                ResultSetMetaData meta = resultSet.getMetaData();
                while (resultSet.next()) {
                    if (rows.size() == settings.maxRows()) {
                        truncated = true;
                        break;
                    }
                    ObjectNode row = rows.addObject();
                    for (int c = 1; c <= meta.getColumnCount(); c++) {
                        // TIMESTAMPTZ as OffsetDateTime (an instant); other types as the driver maps them.
                        Object value = meta.getColumnType(c) == java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                            || "timestamptz".equals(meta.getColumnTypeName(c))
                            ? resultSet.getObject(c, java.time.OffsetDateTime.class) : resultSet.getObject(c);
                        put(row, meta.getColumnLabel(c), value);
                    }
                }
            }
            ObjectNode output = json.createObjectNode();
            output.set("rows", rows);
            output.put("rowCount", rows.size());
            if (truncated) {
                output.put("truncated", true);
            }
            return output;
        }
    }

    private static void bind(PreparedStatement statement, int index, JsonNode value) throws SQLException {
        if (value.isIntegralNumber() && value.canConvertToLong()) {
            statement.setLong(index, value.asLong());
        } else if (value.isNumber()) {
            statement.setBigDecimal(index, value.decimalValue());
        } else if (value.isBoolean()) {
            statement.setBoolean(index, value.asBoolean());
        } else {
            // Text is sent untyped so PostgreSQL infers the type from the query (a date, a status, ...).
            statement.setObject(index, value.asText(), java.sql.Types.OTHER);
        }
    }

    private static void put(ObjectNode row, String column, Object value) {
        switch (value) {
            case null -> row.putNull(column);
            case Integer i -> row.put(column, i);
            case Long l -> row.put(column, l);
            case Short s -> row.put(column, s);
            case BigDecimal d -> row.put(column, d);
            case Double d -> row.put(column, d);
            case Float f -> row.put(column, f);
            case Boolean b -> row.put(column, b);
            case Timestamp t -> row.put(column, t.toLocalDateTime().toString());   // TIMESTAMP: no zone to apply
            case java.time.OffsetDateTime t -> row.put(column, t.toInstant().toString());
            default -> row.put(column, value.toString());
        }
    }

    /**
     * Retryable = the same query could succeed later: timeouts, lost connections, a restarting or overloaded server,
     * pool exhaustion, serialization failures and deadlocks. Everything else (read-only violation 25006, bad SQL,
     * type errors) fails the same way every time.
     */
    static TaskResult classify(SQLException e) {
        String state = e.getSQLState() == null ? "" : e.getSQLState();
        if (state.equals(QUERY_CANCELED)) {
            return TaskResult.failure("TIMEOUT", "query exceeded the statement timeout", true);
        }
        if (e instanceof SQLTransientException || e instanceof SQLRecoverableException
            || state.startsWith(CONNECTION_EXCEPTION_CLASS) || state.startsWith(INSUFFICIENT_RESOURCES_CLASS)
            || TRANSIENT_STATES.contains(state)) {
            return TaskResult.failure("DATABASE_UNAVAILABLE", "database temporarily unavailable (SQLSTATE " + state + ")",
                true);
        }
        return TaskResult.failure("DATABASE_ERROR", "query failed (SQLSTATE " + state + ")", false);
    }

    /** Read-only, so nothing to undo; a failing rollback must not hide the real outcome. */
    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // The connection is broken; the pool discards it.
        }
    }

    private static void cancelQuietly(Statement statement) {
        try {
            statement.cancel();
        } catch (SQLException ignored) {
            // The statement already finished or was closed; nothing to cancel.
        }
    }
}
