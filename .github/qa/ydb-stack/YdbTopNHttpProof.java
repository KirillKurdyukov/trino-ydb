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
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class YdbTopNHttpProof
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
                nativeStatement.execute("CREATE TABLE topn_floating (id Int64, f Double, r Float, PRIMARY KEY (id))");
                try (PreparedStatement insert = connection.prepareStatement("INSERT INTO topn_floating (id, f, r) VALUES (?, ?, ?)")) {
                    Double[] values = {Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY, Double.NaN, null};
                    for (int index = 0; index < values.length; index++) {
                        insert.setLong(1, index + 1);
                        if (values[index] == null) {
                            insert.setNull(2, Types.DOUBLE);
                            insert.setNull(3, Types.REAL);
                        }
                        else {
                            insert.setDouble(2, values[index]);
                            insert.setFloat(3, values[index].floatValue());
                        }
                        insert.executeUpdate();
                    }
                }
                nativeStatement.execute("CREATE TABLE topn_types (id Int64, u Uint64, s Utf8, d Date32, ts Timestamp64, PRIMARY KEY (id))");
                nativeStatement.execute("INSERT INTO topn_types (id, u, s, d, ts) VALUES " +
                        "(1, 18446744073709551615ul, '\u00E9'u, Date32('1969-12-31'), Timestamp64('1969-12-31T23:59:59.999999Z')), " +
                        "(2, 0ul, 'A'u, Date32('1970-01-01'), Timestamp64('1970-01-01T00:00:00Z')), " +
                        "(3, 9223372036854775808ul, '\uD83D\uDE00'u, Date32('2000-01-01'), Timestamp64('2000-01-01T00:00:00Z')), " +
                        "(4, NULL, NULL, NULL, NULL)");
            }
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            for (String column : List.of("f", "r", "u", "s", "d", "ts")) {
                String table = column.equals("f") || column.equals("r") ? "topn_floating" : "topn_types";
                int limit = table.equals("topn_floating") ? 5 : 3;
                for (String order : List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
                    String sql = "SELECT id, " + column + " FROM ydb.default." + table + " ORDER BY " + column + " " + order + ", id LIMIT " + limit;
                    QueryResult actual = success(client, statement, sql);
                    QueryResult expected = execute(client, statement, sql, "ydb.topn_pushdown_enabled=false");
                    require(expected.error() == null && actual.rows().equals(expected.rows()), "Native TopN must match Trino for " + column + " " + order);
                }
            }
            String sql = "SELECT id, f FROM ydb.default.topn_floating ORDER BY f ASC NULLS FIRST, id LIMIT 5";
            String plan = success(client, statement, "EXPLAIN " + sql).rows().getFirst().get(0).asText();
            require(plan.contains("sortOrder=") && plan.contains("limit=5") && !plan.contains("TopN"), "Default TopN must be selected for remote execution");
            QueryResult disabledPlan = execute(client, statement, "EXPLAIN " + sql, "ydb.topn_pushdown_enabled=false");
            require(disabledPlan.error() == null && disabledPlan.rows().getFirst().get(0).asText().contains("TopN"), "Disabled TopN must remain in Trino");
            QueryResult limit = success(client, statement, "SELECT id FROM ydb.default.topn_types LIMIT 10");
            require(limit.rows().size() == 4, "Pushed limit must preserve all rows below the bound");
            String limitPlan = success(client, statement, "EXPLAIN SELECT id FROM ydb.default.topn_types LIMIT 10").rows().getFirst().get(0).asText();
            require(limitPlan.contains("limit=10") && !limitPlan.contains("Limit["), "Plain limit must be selected for remote execution");
            evidence.put("nativeTopNSelected", true);
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
            throw new RuntimeException("TopN HTTP proof failed", failure);
        }
        System.out.println("PASS: TopN and limit selected, all NULL orders, floating special values, unsigned, Unicode and temporal ordering");
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
