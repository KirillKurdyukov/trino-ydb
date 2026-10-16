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
import java.util.TreeMap;

public final class YdbDmlHttpProof
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<Map<String, Object>> QUERIES = new ArrayList<>();

    public static void main(String[] args)
            throws Exception
    {
        Path evidencePath = Path.of(args[0]);
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("sourceSha", System.getenv("SOURCE_SHA"));
        evidence.put("upstreamBaseSha", System.getenv("UPSTREAM_BASE_SHA"));
        evidence.put("integrationTreeSha", System.getenv("INTEGRATION_TREE_SHA"));
        evidence.put("queries", QUERIES);
        YdbEnvironment environment = new YdbEnvironment();
        Throwable failure = null;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()) {
            environment.start();
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement()) {
                nativeStatement.execute("CREATE TABLE dml_edges (value Int64, tenant Int64, id Int64, flag Bool, label Utf8, PRIMARY KEY (tenant, id))");
                nativeStatement.execute("INSERT INTO dml_edges (value, tenant, id, flag, label) VALUES " +
                        "(10, 1, 11, true, 'A'u), (20, NULL, 12, NULL, 'B'u), (30, 2, NULL, false, NULL), (40, NULL, NULL, true, 'C'u)");
                Map<Short, String> keys = new TreeMap<>();
                try (ResultSet rows = connection.getMetaData().getPrimaryKeys(null, null, "dml_edges")) {
                    while (rows.next()) {
                        keys.put(rows.getShort("KEY_SEQ"), rows.getString("COLUMN_NAME"));
                    }
                }
                require(new ArrayList<>(keys.values()).equals(List.of("tenant", "id")), "Physical composite-key order must come from metadata");
                evidence.put("nativePrimaryKeys", keys);
            }
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            success(client, statement, "CREATE TABLE ydb.default.stack_write (id bigint, value varchar) WITH (primary_key = ARRAY['id'])");
            expectUpdate(client, statement, "INSERT INTO ydb.default.stack_write VALUES (1, 'ok')", 1);
            expectUpdate(client, statement, "UPDATE ydb.default.stack_write SET value = 'updated' WHERE id = 1", 1);
            require(success(client, statement, "SELECT value FROM ydb.default.stack_write WHERE id = 1").rows()
                    .equals(List.of(JSON.readTree("[\"updated\"]"))), "The requested UPDATE example must retain the exact value");
            expectUpdate(client, statement, "DELETE FROM ydb.default.stack_write WHERE id = 1", 1);
            require(success(client, statement, "SELECT count(*) FROM ydb.default.stack_write").rows()
                    .equals(List.of(JSON.readTree("[0]"))), "The requested DELETE example must remove exactly its row");
            success(client, statement, "DROP TABLE ydb.default.stack_write");
            evidence.put("requestedDmlExamplesVerified", true);
            expectUpdate(client, statement, "UPDATE ydb.default.dml_edges SET value = 99 WHERE tenant IS NULL", 2);
            QueryResult nullableKeys = success(client, statement, "SELECT value, tenant, id FROM ydb.default.dml_edges WHERE tenant IS NULL ORDER BY id NULLS FIRST");
            require(nullableKeys.rows().equals(List.of(JSON.readTree("[99,null,null]"), JSON.readTree("[99,null,12]"))), "RETURNING NULL keys must still count both updated rows");
            expectUpdate(client, statement, "UPDATE ydb.default.dml_edges SET value = 10 WHERE id = 11", 1);
            expectUpdate(client, statement, "UPDATE ydb.default.dml_edges SET value = 77 WHERE id = -1", 0);
            expectUpdate(client, statement, "UPDATE ydb.default.dml_edges SET value = NULL, flag = false, label = 'it''s \u00E9\uD83D\uDE00' WHERE id = 12 AND tenant IS NULL", 1);
            require(success(client, statement, "SELECT value, flag, label FROM ydb.default.dml_edges WHERE id = 12").rows()
                    .equals(List.of(JSON.readTree("[null,false,\"it's \u00E9\uD83D\uDE00\"]"))), "Multiple assignments must retain NULL, boolean, Unicode and quote bindings");
            expectError(client, statement, "UPDATE ydb.default.dml_edges SET tenant = 3 WHERE value = 10", "NOT_SUPPORTED");
            expectError(client, statement, "UPDATE ydb.default.dml_edges SET id = 3 WHERE value = 10", "NOT_SUPPORTED");
            expectError(client, statement, "UPDATE ydb.default.dml_edges SET value = value + 1", "NOT_SUPPORTED");
            expectError(client, statement, "DELETE FROM ydb.default.dml_edges WHERE label LIKE '%A%'", "NOT_SUPPORTED");
            expectError(client, statement, "MERGE INTO ydb.default.dml_edges t USING (VALUES (1, 11, 50)) s(tenant, id, value) ON t.tenant = s.tenant AND t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value", "NOT_SUPPORTED");
            require(success(client, statement, "SELECT count(*) FROM ydb.default.dml_edges").rows().equals(List.of(JSON.readTree("[4]"))), "Rejected changes must preserve every row");
            expectUpdate(client, statement, "DELETE FROM ydb.default.dml_edges WHERE tenant IS NULL", 2);
            expectUpdate(client, statement, "DELETE FROM ydb.default.dml_edges WHERE id = -1", 0);
            expectUpdate(client, statement, "DELETE FROM ydb.default.dml_edges WHERE value = 10", 1);
            expectUpdate(client, statement, "DELETE FROM ydb.default.dml_edges", 1);
            expectUpdate(client, statement, "DELETE FROM ydb.default.dml_edges", 0);
            expectUpdate(client, statement, "UPDATE ydb.default.dml_edges SET value = 1", 0);
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement();
                    ResultSet rows = nativeStatement.executeQuery("SELECT count(*) FROM dml_edges")) {
                require(rows.next() && rows.getLong(1) == 0 && !rows.next(), "Native state must confirm all counted deletes");
                evidence.put("nativeFinalRows", 0);
            }
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement()) {
                nativeStatement.execute("CREATE TABLE dml_native (id Int64, u Uint64, d Date32, ts Timestamp64, label Utf8, PRIMARY KEY (id))");
                nativeStatement.execute("INSERT INTO dml_native (id) VALUES (1)");
            }
            expectUpdate(client, statement, "UPDATE ydb.default.dml_native SET u = DECIMAL '18446744073709551615', d = DATE '1969-12-31', ts = TIMESTAMP '1969-12-31 23:59:59.999999', label = 'it''s \u00E9\uD83D\uDE00' WHERE id = 1", 1);
            QueryResult nativeTypes = success(client, statement, "SELECT CAST(u AS varchar), CAST(d AS varchar), CAST(ts AS varchar), label FROM ydb.default.dml_native");
            require(nativeTypes.rows().equals(List.of(JSON.readTree("[\"18446744073709551615\",\"1969-12-31\",\"1969-12-31 23:59:59.999999\",\"it's \u00E9\uD83D\uDE00\"]"))), "Pushed assignments must preserve native unsigned and temporal values");
            expectUpdate(client, statement, "DELETE FROM ydb.default.dml_native WHERE id = 1", 1);
            evidence.put("returningCountsVerified", true);
            evidence.put("unsupportedChangesPreservedRows", true);
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
            throw new RuntimeException("Pushed DML HTTP proof failed", failure);
        }
        System.out.println("PASS: native RETURNING update/delete counts, nullable composite keys, zero rows, typed assignments and rejected unsupported changes");
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
        require(result.updateCount() != null && result.updateCount() == expected, "Expected affected rows " + expected + ": " + sql + "; actual: " + result.updateCount());
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
        query.put("session", session);
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
