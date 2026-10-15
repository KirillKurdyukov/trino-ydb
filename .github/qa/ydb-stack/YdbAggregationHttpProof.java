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
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class YdbAggregationHttpProof
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
                nativeStatement.execute("CREATE TABLE nation (nationkey Int64, name Utf8, PRIMARY KEY (nationkey))");
                try (PreparedStatement insert = connection.prepareStatement("INSERT INTO nation (nationkey, name) VALUES (?, ?)")) {
                    for (int key = 0; key < 25; key++) {
                        insert.setLong(1, key);
                        insert.setString(2, key == 1 ? "ARGENTINA" : "nation_" + key);
                        insert.executeUpdate();
                    }
                }
                nativeStatement.execute("CREATE TABLE aggregation_edges (id Int64, n Int64, s Utf8, f Double, r Float, u Uint64, PRIMARY KEY (id))");
                nativeStatement.execute("INSERT INTO aggregation_edges (id, n, s, f, r, u) VALUES " +
                        "(1, 1, 'A'u, 1.5, 1.5f, 1ul), (2, 1, 'a'u, 2.5, 2.5f, 2ul), " +
                        "(3, 2, 'A'u, NULL, NULL, 18446744073709551615ul), (4, NULL, NULL, NULL, NULL, NULL)");
            }
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            String countSql = "SELECT count(*) FROM ydb.default.nation";
            require(success(client, statement, countSql).rows().equals(List.of(JSON.readTree("[25]"))), "Built-server count must return 25");
            String countPlan = success(client, statement, "EXPLAIN " + countSql).rows().getFirst().get(0).asText();
            require(countPlan.contains("count(*)") && !countPlan.contains("Aggregate"), "Native count must be selected by the planner");
            String sql = "SELECT count(*), count(n), count(DISTINCT n), min(n), max(n), count(DISTINCT s), min(s), max(s) FROM ydb.default.aggregation_edges";
            QueryResult counts = success(client, statement, sql);
            require(counts.rows().equals(List.of(JSON.readTree("[4,3,2,1,2,2,\"A\",\"a\"]"))), "Count, distinct, extrema and NULL semantics must match independent expected values");
            QueryResult disabled = execute(client, statement, sql, "ydb.aggregation_pushdown_enabled=false");
            require(disabled.error() == null && disabled.rows().equals(counts.rows()), "Disabled pushdown must return identical aggregate rows");
            QueryResult disabledPlan = execute(client, statement, "EXPLAIN " + countSql, "ydb.aggregation_pushdown_enabled=false");
            require(disabledPlan.error() == null && disabledPlan.rows().getFirst().get(0).asText().contains("Aggregate"), "Disabled count must remain in Trino");
            QueryResult floating = success(client, statement, "SELECT sum(f), avg(f), sum(r), avg(r), count(f) FROM ydb.default.aggregation_edges");
            require(floating.rows().equals(List.of(JSON.readTree("[4.0,2.0,4.0,2.0,2]"))), "Floating sum, average and non-NULL count must match expected values");
            QueryResult empty = success(client, statement, "SELECT count(*), count(n), min(n), max(n), sum(f), avg(f) FROM ydb.default.aggregation_edges WHERE id < 0");
            require(empty.rows().equals(List.of(JSON.readTree("[0,0,null,null,null,null]"))), "Empty global aggregation must return one row with zero counts");
            for (String expression : List.of("min(u), max(u)", "s, count(*)", "f, count(*)", "min(f), max(r)", "sum(n), avg(n)", "count(DISTINCT f)", "sum(DISTINCT f)")) {
                String grouping = expression.equals("s, count(*)") ? " GROUP BY s ORDER BY s NULLS FIRST" :
                        expression.equals("f, count(*)") ? " GROUP BY f ORDER BY f NULLS FIRST" : "";
                String comparisonSql = "SELECT " + expression + " FROM ydb.default.aggregation_edges" + grouping;
                QueryResult actual = success(client, statement, comparisonSql);
                QueryResult expected = execute(client, statement, comparisonSql, "ydb.aggregation_pushdown_enabled=false");
                require(expected.error() == null && actual.rows().equals(expected.rows()), "Unsigned extrema, grouping and restricted aggregates must match Trino: " + comparisonSql);
            }
            String fallbackPlan = success(client, statement, "EXPLAIN SELECT sum(n) FROM ydb.default.aggregation_edges").rows().getFirst().get(0).asText();
            require(fallbackPlan.contains("Aggregate"), "Integral sum must remain in Trino");
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement()) {
                nativeStatement.execute("UPSERT INTO aggregation_edges (id, n) VALUES (1, CAST('9223372036854775807' AS Int64))");
            }
            expectError(client, statement, "SELECT sum(n) FROM ydb.default.aggregation_edges", "NUMERIC_VALUE_OUT_OF_RANGE");
            evidence.put("nativeCountSelected", true);
            evidence.put("disabledPushdownMatches", true);
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
            throw new RuntimeException("Aggregation HTTP proof failed", failure);
        }
        System.out.println("PASS: count, distinct, extrema, floating aggregates, native EXPLAIN, disabled comparison, empty input and overflow");
    }

    private static QueryResult success(HttpClient client, URI endpoint, String sql)
            throws Exception
    {
        QueryResult result = execute(client, endpoint, sql);
        require(result.error() == null, "SQL must succeed: " + sql + "; error: " + result.error());
        return result;
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
