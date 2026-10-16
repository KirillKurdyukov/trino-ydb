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

public final class YdbMergeHttpProof
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
                nativeStatement.execute("CREATE TABLE merge_edges (value Int64, tenant Int64, id Int64, flag Bool, label Utf8, PRIMARY KEY (tenant, id))");
                nativeStatement.execute("INSERT INTO merge_edges (value, tenant, id, flag, label) VALUES " +
                        "(10, 1, 11, true, 'A'u), (20, NULL, 12, NULL, 'B'u), (30, 2, NULL, false, NULL), (40, NULL, NULL, true, 'C'u)");
                Map<Short, String> keys = new TreeMap<>();
                try (ResultSet rows = connection.getMetaData().getPrimaryKeys(null, null, "merge_edges")) {
                    while (rows.next()) {
                        keys.put(rows.getShort("KEY_SEQ"), rows.getString("COLUMN_NAME"));
                    }
                }
                require(List.copyOf(keys.values()).equals(List.of("tenant", "id")), "Composite-key order must come from native metadata");
                evidence.put("nativePrimaryKeys", keys);
            }
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            String defaultMerge = "MERGE INTO ydb.default.merge_edges t USING (VALUES (1, 11, 20)) s(tenant, id, value) " +
                    "ON t.tenant = s.tenant AND t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value";
            expectError(client, statement, defaultMerge, "NOT_SUPPORTED");
            require(success(client, statement, "SELECT value FROM ydb.default.merge_edges WHERE tenant = 1 AND id = 11").rows()
                    .equals(List.of(JSON.readTree("[10]"))), "Default opt-in rejection must preserve target data");
            expectRowChange(client, statement, "MERGE INTO ydb.default.merge_edges t " +
                    "USING (VALUES (1,11,0,'keep','N'),(NULL,12,200,'updated','U'),(2,NULL,300,'unused','F')," +
                    "(NULL,NULL,0,'deleted','D'),(3,13,300,'inserted','I')) s(tenant,id,value,label,action) " +
                    "ON t.tenant IS NOT DISTINCT FROM s.tenant AND t.id IS NOT DISTINCT FROM s.id " +
                    "WHEN MATCHED AND s.action='D' THEN DELETE " +
                    "WHEN MATCHED AND s.action='U' THEN UPDATE SET value=s.value,label=s.label " +
                    "WHEN MATCHED AND s.action='F' THEN UPDATE SET flag=true " +
                    "WHEN NOT MATCHED AND s.action='I' THEN INSERT(value,tenant,id,flag,label) VALUES(s.value,s.tenant,s.id,false,s.label)", 4);
            require(success(client, statement, "SELECT value,tenant,id,flag,label FROM ydb.default.merge_edges ORDER BY value").rows()
                    .equals(List.of(JSON.readTree("[10,1,11,true,\"A\"]"), JSON.readTree("[30,2,null,true,null]"),
                            JSON.readTree("[200,null,12,null,\"updated\"]"), JSON.readTree("[300,3,13,false,\"inserted\"]"))), "Selective cases must preserve unassigned columns and nullable keys");
            expectRowChange(client, statement, "UPDATE ydb.default.merge_edges SET value=value+1 WHERE tenant=1", 1);
            expectRowChange(client, statement, "DELETE FROM ydb.default.merge_edges WHERE label LIKE '%insert%'", 1);
            expectRowChange(client, statement, "MERGE INTO ydb.default.merge_edges t USING (SELECT BIGINT '1' tenant,BIGINT '11' id WHERE false) s " +
                    "ON t.tenant=s.tenant AND t.id=s.id WHEN MATCHED THEN DELETE", 0);
            expectOptedError(client, statement, "MERGE INTO ydb.default.merge_edges t USING (VALUES (1,11)) s(tenant,id) " +
                    "ON t.tenant=s.tenant AND t.id=s.id WHEN MATCHED THEN UPDATE SET tenant=9", "NOT_SUPPORTED");
            expectOptedError(client, statement, "UPDATE ydb.default.merge_edges SET id=id+1 WHERE tenant=1", "NOT_SUPPORTED");
            require(success(client, statement, "SELECT value,tenant,id,flag,label FROM ydb.default.merge_edges ORDER BY value").rows()
                    .equals(List.of(JSON.readTree("[11,1,11,true,\"A\"]"), JSON.readTree("[30,2,null,true,null]"),
                            JSON.readTree("[200,null,12,null,\"updated\"]"))), "Row changes and rejected key updates must preserve complete expected state");
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement();
                    ResultSet rows = nativeStatement.executeQuery("SELECT count(*) FROM merge_edges")) {
                require(rows.next() && rows.getLong(1) == 3 && !rows.next(), "Native state must confirm exactly three surviving rows");
                evidence.put("nativeFinalRows", 3);
            }
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement()) {
                nativeStatement.execute("CREATE TABLE merge_native (label Utf8, u Uint64, d Date32, ts Timestamp64, PRIMARY KEY (u))");
            }
            success(client, statement, "INSERT INTO ydb.default.merge_native (u) VALUES (DECIMAL '18446744073709551615')");
            expectRowChange(client, statement, "MERGE INTO ydb.default.merge_native t " +
                    "USING (VALUES (DECIMAL '18446744073709551615', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999999', 'it''s \u00E9\uD83D\uDE00')) s(u,d,ts,label) " +
                    "ON t.u=s.u WHEN MATCHED THEN UPDATE SET d=s.d,ts=s.ts,label=s.label", 1);
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement();
                    ResultSet rows = nativeStatement.executeQuery("SELECT CAST(u AS String),CAST(d AS Int32),CAST(ts AS Int64),label FROM merge_native")) {
                require(rows.next(), "Typed MERGE must retain its unsigned key row");
                require(rows.getString(1).equals("18446744073709551615") && rows.getInt(2) == -1 && rows.getLong(3) == -1 &&
                        rows.getString(4).equals("it's \u00E9\uD83D\uDE00"), "Native values must confirm unsigned, negative epochs and Unicode bindings");
                require(!rows.next(), "Typed MERGE must not create extra rows");
                evidence.put("nativeTypedBindingsVerified", true);
            }
            expectRowChange(client, statement, "MERGE INTO ydb.default.merge_native t USING (VALUES (DECIMAL '18446744073709551615')) s(u) " +
                    "ON t.u=s.u WHEN MATCHED THEN DELETE", 1);
            require(success(client, statement, "SELECT count(*) FROM ydb.default.merge_native").rows().equals(List.of(JSON.readTree("[0]"))), "Unsigned-key delete must remove the native row");
            evidence.put("explicitOptInRequired", true);
            evidence.put("mixedCasesAndRowCountsVerified", true);
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
            throw new RuntimeException("MERGE HTTP proof failed", failure);
        }
        System.out.println("PASS: explicit opt-in MERGE, mixed selective cases, nullable composite and unsigned keys, native values and row counts");
    }

    private static QueryResult success(HttpClient client, URI endpoint, String sql)
            throws Exception
    {
        QueryResult result = execute(client, endpoint, sql);
        require(result.error() == null, "SQL must succeed: " + sql + "; error: " + result.error());
        return result;
    }

    private static void expectRowChange(HttpClient client, URI endpoint, String sql, long expected)
            throws Exception
    {
        QueryResult result = execute(client, endpoint, sql, "ydb.non_transactional_merge=true");
        require(result.error() == null && result.updateCount() != null && result.updateCount() == expected,
                "Expected row-change count " + expected + ": " + sql + "; error: " + result.error() + "; count: " + result.updateCount());
    }

    private static void expectOptedError(HttpClient client, URI endpoint, String sql, String code)
            throws Exception
    {
        QueryResult result = execute(client, endpoint, sql, "ydb.non_transactional_merge=true");
        require(result.error() != null && result.error().path("errorName").asText().equals(code), "Expected opted-in error " + code + ": " + sql + "; actual: " + result.error());
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
