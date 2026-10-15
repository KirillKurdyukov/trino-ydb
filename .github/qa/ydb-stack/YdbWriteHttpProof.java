/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.tests.product.ydb.YdbEnvironment;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class YdbWriteHttpProof
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<Map<String, Object>> QUERIES = new ArrayList<>();

    public static void main(String[] args)
            throws Exception
    {
        Path evidencePath = Path.of(args[0]);
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("sourceSha", System.getenv("SOURCE_SHA"));
        evidence.put("queries", QUERIES);
        YdbEnvironment environment = new YdbEnvironment();
        Throwable failure = null;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()) {
            environment.start();
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            expectError(client, statement, "CREATE TABLE ydb.default.missing_key (id bigint)", "INVALID_TABLE_PROPERTY");
            success(client, statement, "CREATE TABLE ydb.default.stack_write (payload varchar, tenant bigint, event_id bigint) WITH (primary_key = ARRAY['event_id', 'tenant'])");
            expectUpdate(client, statement, "INSERT INTO ydb.default.stack_write VALUES ('ok', 1, 10), ('nullable-key', 2, NULL)", 2);
            QueryResult created = success(client, statement, "SELECT payload, tenant, event_id FROM ydb.default.stack_write ORDER BY tenant");
            require(created.rows().equals(List.of(JSON.readTree("[\"ok\",1,10]"), JSON.readTree("[\"nullable-key\",2,null]"))), "CREATE and INSERT must preserve composite nullable keys and column order");
            try (Connection connection = environment.createYdbConnection();
                    ResultSet keys = connection.getMetaData().getPrimaryKeys(null, null, "stack_write")) {
                Map<Short, String> orderedKeys = new java.util.TreeMap<>();
                while (keys.next()) {
                    orderedKeys.put(keys.getShort("KEY_SEQ"), keys.getString("COLUMN_NAME"));
                }
                require(new ArrayList<>(orderedKeys.values()).equals(List.of("event_id", "tenant")), "Physical key metadata must preserve property order");
                evidence.put("nativePrimaryKeys", orderedKeys);
            }
            expectError(client, statement, "INSERT INTO ydb.default.stack_write VALUES ('must-rollback', 3, 30), ('duplicate', 1, 10)", "JDBC_ERROR");
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement();
                    ResultSet rows = nativeStatement.executeQuery("SELECT payload, tenant, event_id FROM stack_write ORDER BY tenant")) {
                require(rows.next() && rows.getString(1).equals("ok") && rows.getLong(2) == 1 && rows.getLong(3) == 10, "Existing row must survive failed staged INSERT");
                require(rows.next() && rows.getString(1).equals("nullable-key") && rows.getLong(2) == 2 && rows.getObject(3) == null && !rows.next(), "Failed staged INSERT must not publish its nonduplicate row");
                evidence.put("nativeAtomicInsert", Map.of("rows", 2, "unpublishedTenant", 3));
            }
            expectUpdate(client, statement, "CREATE TABLE ydb.default.stack_ctas (id, payload) WITH (primary_key = ARRAY['id']) AS VALUES (BIGINT '1', 'ctas-a'), (BIGINT '2', 'ctas-b')", 2);
            QueryResult ctas = success(client, statement, "SELECT id, payload FROM ydb.default.stack_ctas ORDER BY id");
            require(ctas.rows().equals(List.of(JSON.readTree("[1,\"ctas-a\"]"), JSON.readTree("[2,\"ctas-b\"]"))), "CTAS must publish exactly its rows");
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement()) {
                nativeStatement.execute("CREATE TABLE stack_native (id Int64, u Uint64, b String, d Date32, t Timestamp64, PRIMARY KEY (id))");
            }
            expectUpdate(client, statement, "INSERT INTO ydb.default.stack_native VALUES (1, DECIMAL '18446744073709551615', X'0080FF', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999'), (2, NULL, NULL, NULL, NULL)", 2);
            QueryResult edges = success(client, statement, "SELECT CAST(u AS varchar), to_hex(b), CAST(d AS varchar), CAST(t AS varchar) FROM ydb.default.stack_native ORDER BY id");
            require(edges.rows().equals(List.of(JSON.readTree("[\"18446744073709551615\",\"0080FF\",\"1969-12-31\",\"1969-12-31 23:59:59.999999\"]"), JSON.readTree("[null,null,null,null]"))), "Native writes must retain unsigned, binary, temporal and NULL values");
            for (String value : List.of("-1", "18446744073709551616")) {
                expectError(client, statement, "INSERT INTO ydb.default.stack_native (id, u) VALUES (3, DECIMAL '" + value + "')", "JDBC_ERROR");
            }
            QueryResult nativeCount = success(client, statement, "SELECT count(*) FROM ydb.default.stack_native");
            require(nativeCount.rows().size() == 1 && nativeCount.rows().getFirst().get(0).asLong() == 2, "Rejected unsigned writes must not add rows");
            QueryResult nontransactional = execute(client, statement, "INSERT INTO ydb.default.stack_write VALUES ('explicit-opt-in', 4, 40)", "ydb.non_transactional_insert=true");
            require(nontransactional.error() == null && nontransactional.updateCount() != null && nontransactional.updateCount() == 1, "Explicit nontransactional INSERT must work");
            expectUpdate(client, statement, "INSERT INTO ydb.default.stack_write (payload, tenant) VALUES ('implicit-null-key', 3)", 1);
            QueryResult partialDirect = execute(client, statement, "INSERT INTO ydb.default.stack_write (payload, tenant) VALUES ('must-not-appear', 5)", "ydb.non_transactional_insert=true");
            require(partialDirect.error() != null && partialDirect.error().path("errorName").asText().equals("NOT_SUPPORTED"), "Direct INSERT must reject missing physical key columns before writing");
            success(client, statement, "CREATE TABLE ydb.default.stack_not_null (id bigint NOT NULL, flag boolean, required_value varchar NOT NULL) WITH (primary_key = ARRAY['id'])");
            expectUpdate(client, statement, "INSERT INTO ydb.default.stack_not_null VALUES (1, true, 'first'), (2, NULL, 'second')", 2);
            QueryResult booleans = success(client, statement, "SELECT id, flag, required_value FROM ydb.default.stack_not_null ORDER BY id");
            require(booleans.rows().equals(List.of(JSON.readTree("[1,true,\"first\"]"), JSON.readTree("[2,null,\"second\"]"))), "Staged writes must preserve NOT NULL and typed boolean NULL values");
            for (String sql : List.of(
                    "UPDATE ydb.default.stack_write SET event_id = 11 WHERE tenant = 1",
                    "UPDATE ydb.default.stack_write SET payload = 'changed' WHERE tenant = 1",
                    "DELETE FROM ydb.default.stack_write WHERE tenant = 1",
                    "MERGE INTO ydb.default.stack_write t USING (VALUES (1, 'changed')) s(tenant, payload) ON t.tenant = s.tenant WHEN MATCHED THEN UPDATE SET payload = s.payload")) {
                expectError(client, statement, sql, "NOT_SUPPORTED");
            }
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement();
                    ResultSet rows = nativeStatement.executeQuery("SELECT payload, tenant FROM stack_write ORDER BY tenant")) {
                require(rows.next() && rows.getString(1).equals("ok") && rows.getLong(2) == 1, "Unsupported DML must preserve original row");
                require(rows.next() && rows.getString(1).equals("nullable-key") && rows.getLong(2) == 2, "Nullable-key row must remain");
                require(rows.next() && rows.getString(1).equals("implicit-null-key") && rows.getLong(2) == 3, "Missing nullable key must be supplied by staged publication");
                require(rows.next() && rows.getString(1).equals("explicit-opt-in") && rows.getLong(2) == 4 && !rows.next(), "Native read must confirm nontransactional insert and no extra rows");
                evidence.put("nativeFinalRows", 4);
            }
            for (String table : List.of("stack_write", "stack_ctas", "stack_native", "stack_not_null")) {
                success(client, statement, "DROP TABLE ydb.default." + table);
                try (Connection connection = environment.createYdbConnection();
                        ResultSet tables = connection.getMetaData().getTables(null, null, table, null)) {
                    require(!tables.next(), "Dropped table must be absent from native metadata: " + table);
                }
            }
            evidence.put("result", "PASS");
        }
        catch (Throwable caught) {
            failure = caught;
            evidence.put("result", "FAIL");
            evidence.put("failure", caught.toString());
        }
        finally {
            try {
                environment.close();
                evidence.put("environmentClosed", !environment.isRunning());
            }
            catch (Throwable cleanupFailure) {
                evidence.put("cleanupFailure", cleanupFailure.toString());
                if (failure == null) {
                    failure = cleanupFailure;
                }
                else {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            Files.createDirectories(evidencePath.getParent());
            Files.writeString(evidencePath, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
        }
        if (failure != null) {
            throw new RuntimeException("CREATE/INSERT HTTP proof failed", failure);
        }
        System.out.println("PASS: CREATE, composite keys, staged INSERT atomicity, CTAS, native types, opt-in INSERT, rejected DML and DROP");
    }

    private static QueryResult success(HttpClient client, URI endpoint, String sql)
            throws Exception
    {
        QueryResult result = execute(client, endpoint, sql);
        require(result.error() == null, "SQL must succeed: " + sql + "; error: " + result.error());
        return result;
    }

    private static void expectUpdate(HttpClient client, URI endpoint, String sql, long expected)
            throws Exception
    {
        QueryResult result = success(client, endpoint, sql);
        require(result.updateCount() != null && result.updateCount() == expected, "Affected count must equal " + expected + ": " + sql);
    }

    private static void expectError(HttpClient client, URI endpoint, String sql, String errorName)
            throws Exception
    {
        QueryResult result = execute(client, endpoint, sql);
        require(result.error() != null && result.error().path("errorName").asText().equals(errorName), "Expected " + errorName + ": " + sql + "; actual: " + result.error());
    }

    private static QueryResult execute(HttpClient client, URI endpoint, String sql)
            throws Exception
    {
        return execute(client, endpoint, sql, "");
    }

    private static QueryResult execute(HttpClient client, URI endpoint, String sql, String session)
            throws Exception
    {
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("sql", sql);
        List<Map<String, Object>> responses = new ArrayList<>();
        query.put("responses", responses);
        QUERIES.add(query);
        List<JsonNode> rows = new ArrayList<>();
        JsonNode error = null;
        Long updateCount = null;
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofMinutes(2))
                .header("X-Trino-User", "ulw-qa")
                .header("Content-Type", "text/plain; charset=utf-8")
                .header("X-Trino-Session", session)
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build();
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (true) {
            require(System.nanoTime() < deadline, "Query exceeded its bounded execution deadline");
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            responses.add(Map.of("status", response.statusCode(), "headers", response.headers().map(), "body", response.body()));
            require(response.statusCode() == 200, "Trino Statement API must return HTTP 200");
            JsonNode body = JSON.readTree(response.body());
            if (body.has("data")) {
                body.get("data").forEach(rows::add);
            }
            if (body.hasNonNull("updateCount")) {
                updateCount = body.get("updateCount").asLong();
            }
            if (body.hasNonNull("error")) {
                error = body.get("error");
            }
            if (!body.hasNonNull("nextUri")) {
                break;
            }
            request = HttpRequest.newBuilder(URI.create(body.get("nextUri").asText()))
                    .timeout(Duration.ofMinutes(2))
                    .header("X-Trino-User", "ulw-qa")
                    .GET()
                    .build();
        }
        query.put("rowCount", rows.size());
        query.put("updateCount", updateCount);
        return new QueryResult(rows, error, updateCount);
    }

    private static void require(boolean condition, String message)
    {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private record QueryResult(List<JsonNode> rows, JsonNode error, Long updateCount) {}
}
