package com.flowforge.runtime.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A configured query with {@code :name} parameters turned into JDBC {@code ?} placeholders. PostgreSQL casts
 * ({@code ::date}) are left alone. Parameter values are always bound, never concatenated into the SQL.
 *
 * <p>Deliberately simple, because only operators write these queries: {@code :word} inside string literals or array
 * slices ({@code arr[1:n]}) would be read as a parameter, and a literal {@code ?} (a jsonb operator) as a placeholder.
 */
record NamedQuery(String jdbcSql, List<String> parameters) {

    private static final Pattern PARAMETER = Pattern.compile("(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)");

    static NamedQuery parse(String sql) {
        Matcher matcher = PARAMETER.matcher(sql);
        StringBuilder jdbc = new StringBuilder();
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            names.add(matcher.group(1));
            matcher.appendReplacement(jdbc, "?");
        }
        matcher.appendTail(jdbc);
        return new NamedQuery(jdbc.toString(), List.copyOf(names));
    }
}
