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
import java.util.Set;

public final class YdbJoinHttpProof
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
                nativeStatement.execute("CREATE TABLE nation (nationkey Int64, regionkey Int64, PRIMARY KEY (nationkey))");
                nativeStatement.execute("CREATE TABLE region (regionkey Int64, PRIMARY KEY (regionkey))");
                try (PreparedStatement nations = connection.prepareStatement("INSERT INTO nation (nationkey, regionkey) VALUES (?, ?)");
                        PreparedStatement regions = connection.prepareStatement("INSERT INTO region (regionkey) VALUES (?)")) {
                    for (int id = 0; id < 25; id++) {
                        nations.setLong(1, id);
                        nations.setLong(2, id % 5);
                        nations.addBatch();
                    }
                    nations.executeBatch();
                    for (int id = 0; id < 5; id++) {
                        regions.setLong(1, id);
                        regions.addBatch();
                    }
                    regions.executeBatch();
                }
                for (String table : List.of("join_left", "join_right")) {
                    nativeStatement.execute("CREATE TABLE " + table + " (id Int64, k Int64, second_key Int64, f Double, u Uint64, s Utf8, PRIMARY KEY (id))");
                }
                nativeStatement.execute("INSERT INTO join_left (id, k, second_key, f, u, s) VALUES " +
                        "(1, 7, 1, Double('0'), 18446744073709551615ul, 'A'u), " +
                        "(2, 7, 2, Double('-0'), 18446744073709551615ul, 'a'u), " +
                        "(3, NULL, 1, Double('nan'), NULL, NULL), (4, 8, 1, Double('inf'), 0ul, '\u00E9'u)");
                nativeStatement.execute("INSERT INTO join_right (id, k, second_key, f, u, s) VALUES " +
                        "(10, 7, 1, Double('0'), 18446744073709551615ul, 'A'u), " +
                        "(11, 7, 2, Double('-0'), 18446744073709551615ul, 'a'u), " +
                        "(12, NULL, 1, Double('nan'), NULL, NULL), (13, 9, 1, Double('inf'), 1ul, 'other'u)");
            }
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            String enabled = "ydb.join_pushdown_enabled=true";
            String disabled = "ydb.join_pushdown_enabled=false";
            String requestedJoin = "SELECT n.nationkey,r.regionkey FROM ydb.default.nation n JOIN ydb.default.region r ON n.regionkey=r.regionkey";
            QueryResult requestedRows = execute(client, statement, requestedJoin, enabled);
            QueryResult requestedLocal = execute(client, statement, requestedJoin, disabled);
            List<JsonNode> expectedNationRegionRows = new ArrayList<>();
            for (int id = 0; id < 25; id++) {
                expectedNationRegionRows.add(JSON.readTree("[" + id + "," + (id % 5) + "]"));
            }
            require(requestedRows.error() == null && requestedRows.rows().size() == 25 &&
                            Set.copyOf(requestedRows.rows()).equals(Set.copyOf(expectedNationRegionRows)),
                    "The requested native nation/region JOIN must return all 25 expected rows");
            require(requestedLocal.error() == null && requestedLocal.rows().size() == 25 &&
                            Set.copyOf(requestedRows.rows()).equals(Set.copyOf(requestedLocal.rows())),
                    "The requested JOIN must match disabled pushdown");
            QueryResult requestedExplain = execute(client, statement, "EXPLAIN " + requestedJoin, enabled);
            require(requestedExplain.error() == null, "The requested JOIN EXPLAIN must succeed");
            String requestedPlan = requestedExplain.rows().getFirst().get(0).asText();
            require(requestedPlan.contains("JOIN") && !requestedPlan.contains("Join") &&
                            requestedPlan.lines().filter(line -> line.contains("[table = ydb:")).count() == 1,
                    "The requested enabled JOIN must use one remote source scan");
            evidence.put("requestedNationRegionRows", 25);
            for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
                String sql = "SELECT l.id, r.id FROM ydb.default.join_left l " + kind + " JOIN ydb.default.join_right r ON l.k = r.k ORDER BY l.id NULLS FIRST, r.id NULLS FIRST";
                QueryResult pushed = execute(client, statement, sql, enabled);
                QueryResult local = execute(client, statement, sql, disabled);
                int expectedRows = switch (kind) {
                    case "INNER" -> 4;
                    case "LEFT", "RIGHT" -> 6;
                    case "FULL" -> 8;
                    default -> throw new AssertionError("Unknown fixture join kind");
                };
                require(pushed.error() == null && local.error() == null && pushed.rows().equals(local.rows()) && pushed.rows().size() == expectedRows,
                        "Native " + kind + " join must preserve duplicate and NULL keys");
            }
            for (String column : List.of("f", "u", "s")) {
                for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
                    String sql = "SELECT l.id, r.id FROM ydb.default.join_left l " + kind + " JOIN ydb.default.join_right r ON l." + column + " = r." + column + " ORDER BY l.id NULLS FIRST, r.id NULLS FIRST";
                    QueryResult pushed = execute(client, statement, sql, enabled);
                    QueryResult local = execute(client, statement, sql, disabled);
                    require(pushed.error() == null && local.error() == null && pushed.rows().equals(local.rows()), "Unsigned, case-sensitive text, NaN and signed-zero joins must match Trino: " + column + " " + kind);
                }
            }
            String composite = "SELECT l.id, r.id FROM ydb.default.join_left l JOIN ydb.default.join_right r ON l.k = r.k AND l.second_key = r.second_key ORDER BY l.id";
            QueryResult compositeResult = execute(client, statement, composite, enabled);
            require(compositeResult.error() == null && compositeResult.rows().equals(List.of(JSON.readTree("[1,10]"), JSON.readTree("[2,11]"))), "Composite equality keys must preserve their ownership");
            String parameterized = "SELECT l.id, r.id FROM (SELECT id, k FROM ydb.default.join_left WHERE id > 1 AND id < 3) l JOIN (SELECT id, k FROM ydb.default.join_right WHERE id > 10 AND id < 12) r ON l.k = r.k";
            QueryResult parameters = execute(client, statement, parameterized, enabled);
            require(parameters.error() == null && parameters.rows().equals(List.of(JSON.readTree("[2,11]"))), "Prepared query parameters must retain left/right order");
            String bareJoin = "SELECT l.id, r.id FROM ydb.default.join_left l JOIN ydb.default.join_right r ON l.k = r.k";
            QueryResult pushedExplain = execute(client, statement, "EXPLAIN " + bareJoin, enabled);
            require(pushedExplain.error() == null, "Enabled JOIN EXPLAIN must succeed");
            String pushedPlan = pushedExplain.rows().getFirst().get(0).asText();
            require(pushedPlan.contains("JOIN") && pushedPlan.split("TableScan", -1).length == 2 && !pushedPlan.contains("Join"), "Enabled equality JOIN must be one remote scan");
            String defaultPlan = success(client, statement, "EXPLAIN " + bareJoin).rows().getFirst().get(0).asText();
            require(defaultPlan.contains("Join") && defaultPlan.lines().filter(line -> line.contains("[table = ydb:")).count() == 2, "Default JOIN must stay in Trino with two source scans");
            QueryResult complexPlan = execute(client, statement, "EXPLAIN " + bareJoin, enabled + ",ydb.complex_join_pushdown_enabled=true");
            require(complexPlan.error() == null && complexPlan.rows().getFirst().get(0).asText().contains("Join"), "Expression-based JOIN must decline unsupported source ownership");
            for (String condition : List.of("l.k < r.k", "l.k IS NOT DISTINCT FROM r.k", "l.k + 1 = r.k")) {
                String sql = "EXPLAIN SELECT l.id, r.id FROM ydb.default.join_left l JOIN ydb.default.join_right r ON " + condition;
                QueryResult fallback = execute(client, statement, sql, enabled);
                require(fallback.error() == null && fallback.rows().getFirst().get(0).asText().contains("Join"), "Unsupported JOIN condition must remain in Trino: " + condition);
            }
            evidence.put("nativeJoinSelectedOnlyWhenEnabled", true);
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
            throw new RuntimeException("JOIN HTTP proof failed", failure);
        }
        System.out.println("PASS: opt-in equality JOIN, default and complex fallback, duplicate and NULL keys, typed keys, composite conditions and parameters");
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
