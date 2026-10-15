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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class YdbReadOnlyHttpProof
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
            seed(environment);
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            QueryResult count = execute(client, statement, "SELECT COUNT(*) FROM ydb.default.nation");
            require(count.error() == null && count.rows().size() == 1 && count.rows().getFirst().get(0).asLong() == 25,
                    "Native fixture must be read through the packaged connector as 25 rows");
            QueryResult edges = execute(
                    client,
                    statement,
                    "SELECT CAST(u AS varchar), to_hex(b), CAST(d AS varchar), CAST(t AS varchar) FROM ydb.default.read_edges ORDER BY id");
            require(edges.error() == null && edges.rows().size() == 2, "Native edge rows must be readable");
            JsonNode first = edges.rows().getFirst();
            require(first.get(0).asText().equals("18446744073709551615") && first.get(1).asText().equals("0080FF") &&
                            first.get(2).asText().equals("1969-12-31") && first.get(3).asText().equals("1969-12-31 23:59:59.999999"),
                    "Unsigned, binary and pre-epoch temporal values must retain their native meaning");
            for (JsonNode value : edges.rows().get(1)) {
                require(value.isNull(), "Nullable native values must remain NULL");
            }
            for (String sql : List.of(
                    "CREATE TABLE ydb.default.should_not_exist (id bigint)",
                    "INSERT INTO ydb.default.guarded_rows VALUES (2, 'new')",
                    "UPDATE ydb.default.guarded_rows SET value = 'changed' WHERE id = 1",
                    "DELETE FROM ydb.default.guarded_rows WHERE id = 1",
                    "MERGE INTO ydb.default.guarded_rows t USING (VALUES (1, 'changed')) s(id, value) ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value",
                    "ALTER TABLE ydb.default.guarded_rows ADD COLUMN added bigint",
                    "ALTER TABLE ydb.default.guarded_rows RENAME TO ydb.default.should_not_exist",
                    "DROP TABLE ydb.default.guarded_rows")) {
                QueryResult rejected = execute(client, statement, sql);
                require(rejected.error() != null && rejected.error().path("errorName").asText().equals("NOT_SUPPORTED"),
                        "Read-only operation must fail with NOT_SUPPORTED: " + sql);
            }
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement();
                    ResultSet rows = nativeStatement.executeQuery("SELECT id, value FROM guarded_rows")) {
                require(rows.next() && rows.getLong(1) == 1 && rows.getString(2).equals("original") && !rows.next(),
                        "Native state must be unchanged after all rejected writes");
                evidence.put("nativeGuard", Map.of("id", 1, "value", "original", "rows", 1));
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
            throw new RuntimeException("Read-only HTTP proof failed", failure);
        }
        System.out.println("PASS: read-only HTTP rows, native edges, eight rejected mutations and unchanged native state");
    }

    private static void seed(YdbEnvironment environment)
            throws Exception
    {
        try (Connection connection = environment.createYdbConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE nation (nationkey Int64, name Utf8, PRIMARY KEY (nationkey))");
            try (PreparedStatement insert = connection.prepareStatement("UPSERT INTO nation (nationkey, name) VALUES (?, ?)")) {
                for (int key = 0; key < 25; key++) {
                    insert.setLong(1, key);
                    insert.setString(2, "nation_" + key);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            statement.execute("CREATE TABLE guarded_rows (id Int64, value Utf8, PRIMARY KEY (id))");
            statement.execute("UPSERT INTO guarded_rows (id, value) VALUES (1, 'original'u)");
            statement.execute("CREATE TABLE read_edges (id Int64, u Uint64, b String, d Date32, t Timestamp64, PRIMARY KEY (id))");
            statement.execute("UPSERT INTO read_edges (id, u, b, d, t) VALUES " +
                    "(1, 18446744073709551615ul, String('\\x00\\x80\\xff'), Date32('1969-12-31'), Timestamp64('1969-12-31T23:59:59.999999Z')), " +
                    "(2, NULL, NULL, NULL, NULL)");
        }
    }

    private static QueryResult execute(HttpClient client, URI endpoint, String sql)
            throws Exception
    {
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("sql", sql);
        List<Map<String, Object>> responses = new ArrayList<>();
        query.put("responses", responses);
        QUERIES.add(query);
        List<JsonNode> rows = new ArrayList<>();
        JsonNode error = null;
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofMinutes(2))
                .header("X-Trino-User", "ulw-qa")
                .header("Content-Type", "text/plain; charset=utf-8")
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
        return new QueryResult(rows, error);
    }

    private static void require(boolean condition, String message)
    {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private record QueryResult(List<JsonNode> rows, JsonNode error) {}
}
