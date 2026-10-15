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

public final class YdbExpressionHttpProof
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
                nativeStatement.execute("CREATE TABLE expression_edges (id Int64, n Int64, other Int64, s Utf8, PRIMARY KEY (id))");
                nativeStatement.execute("INSERT INTO expression_edges (id, n, other, s) VALUES " +
                        "(1, 11, 11, 'Hello\uD83D\uDE00'u), (2, -11, 2, 'M\u00DCNCHEN'u), " +
                        "(3, CAST('-9223372036854775808' AS Int64), -1, NULL), " +
                        "(4, CAST('9223372036854775807' AS Int64), 0, ''u), (5, NULL, 1, NULL)");
            }
            URI jdbc = URI.create(environment.getTrinoJdbcUrl().substring("jdbc:".length()));
            URI statement = URI.create("http://" + jdbc.getRawAuthority() + "/v1/statement");
            String countrySql = "SELECT lower(name) FROM ydb.default.nation WHERE nationkey = 1";
            require(success(client, statement, countrySql).rows().equals(List.of(JSON.readTree("[\"argentina\"]"))), "Built-server expression query must return argentina");
            String countryPlan = success(client, statement, "EXPLAIN " + countrySql).rows().getFirst().get(0).asText();
            require(countryPlan.contains("Unicode::ToLower") && !countryPlan.contains("Project"), "Country projection must be selected for remote execution");
            String unicodeSql = "SELECT id, lower(s), upper(s), strpos(s, '\uD83D\uDE00') FROM ydb.default.expression_edges ORDER BY id";
            QueryResult unicode = success(client, statement, unicodeSql);
            require(unicode.rows().equals(List.of(
                    JSON.readTree("[1,\"hello\uD83D\uDE00\",\"HELLO\uD83D\uDE00\",6]"),
                    JSON.readTree("[2,\"m\u00FCnchen\",\"M\u00DCNCHEN\",0]"),
                    JSON.readTree("[3,null,null,null]"), JSON.readTree("[4,\"\",\"\",0]"), JSON.readTree("[5,null,null,null]"))),
                    "Unicode case, code-point positions and NULLs must match independent expected rows");
            QueryResult disabled = execute(client, statement, unicodeSql, "ydb.complex_expression_pushdown=false");
            require(disabled.error() == null && disabled.rows().equals(unicode.rows()), "Disabled expression pushdown must return identical rows");
            QueryResult concatenated = success(client, statement, "SELECT s || '!' FROM ydb.default.expression_edges ORDER BY id");
            require(concatenated.rows().equals(List.of(JSON.readTree("[\"Hello\uD83D\uDE00!\"]"), JSON.readTree("[\"M\u00DCNCHEN!\"]"),
                    JSON.readTree("[null]"), JSON.readTree("[\"!\"]"), JSON.readTree("[null]"))), "Varchar concatenation must preserve Unicode, empty strings and NULL");
            // Trino 484 ConcatFunction limits results to PageBuilderStatus's 1 MiB.
            require(success(client, statement, "SELECT length(lpad('', 1048576, 'x'))").rows().equals(List.of(JSON.readTree("[1048576]"))), "The concatenation input must be a valid 1 MiB varchar");
            expectError(client, statement, "SELECT s || lpad('', 1048576, 'x') FROM ydb.default.expression_edges WHERE id = 1", "INVALID_FUNCTION_ARGUMENT");
            String pushedPlan = success(client, statement, "EXPLAIN " + unicodeSql).rows().getFirst().get(0).asText();
            require(pushedPlan.contains("Unicode::ToLower") && pushedPlan.contains("Unicode::ToUpper") && pushedPlan.contains("Unicode::Find"), "EXPLAIN must expose actual remote Unicode functions");
            QueryResult disabledExplain = execute(client, statement, "EXPLAIN " + unicodeSql, "ydb.complex_expression_pushdown=false");
            require(disabledExplain.error() == null && !disabledExplain.rows().getFirst().get(0).asText().contains("Unicode::"), "Disabled plan must not contain native Unicode expressions");
            try (Connection connection = environment.createYdbConnection();
                    Statement nativeStatement = connection.createStatement()) {
                nativeStatement.execute("UPSERT INTO expression_edges (id, n, other, s) VALUES " +
                        "(1, 11, 11, '\u00DF\u0130\u03A3'u), (2, -11, 2, '\u039F\u0394\u039F\u03A3'u)");
            }
            String caseSql = "SELECT id, lower(s), upper(s) FROM ydb.default.expression_edges ORDER BY id";
            QueryResult caseMapping = success(client, statement, caseSql);
            QueryResult trinoCaseMapping = execute(client, statement, caseSql, "ydb.complex_expression_pushdown=false");
            require(trinoCaseMapping.error() == null && caseMapping.rows().equals(trinoCaseMapping.rows()), "Native and Trino case mappings must agree on difficult Unicode values");
            QueryResult predicate = success(client, statement, "SELECT id FROM ydb.default.expression_edges WHERE n = other");
            require(predicate.rows().equals(List.of(JSON.readTree("[1]"))), "Column comparison predicate must preserve NULL semantics");
            QueryResult arithmetic = success(client, statement, "SELECT n / 2, n % 2 FROM ydb.default.expression_edges ORDER BY id");
            require(arithmetic.rows().equals(List.of(
                    JSON.readTree("[5,1]"), JSON.readTree("[-5,-1]"), JSON.readTree("[-4611686018427387904,0]"),
                    JSON.readTree("[4611686018427387903,1]"), JSON.readTree("[null,null]"))), "Safe integral arithmetic must preserve signed minimum, maximum and NULL values");
            QueryResult modulo = success(client, statement, "SELECT n % BIGINT '-1' FROM ydb.default.expression_edges ORDER BY id");
            require(modulo.rows().equals(List.of(JSON.readTree("[0]"), JSON.readTree("[0]"), JSON.readTree("[0]"), JSON.readTree("[0]"), JSON.readTree("[null]"))), "Signed-minimum modulo -1 must fall back and return zero");
            QueryResult nullIf = success(client, statement, "SELECT nullif(n, BIGINT '11') FROM ydb.default.expression_edges ORDER BY id");
            require(nullIf.rows().equals(List.of(JSON.readTree("[null]"), JSON.readTree("[-11]"), JSON.readTree("[-9223372036854775808]"), JSON.readTree("[9223372036854775807]"), JSON.readTree("[null]"))), "NULLIF must preserve repeated-parameter order and signed bounds");
            expectError(client, statement, "SELECT n / 0 FROM ydb.default.expression_edges WHERE id = 1", "DIVISION_BY_ZERO");
            expectError(client, statement, "SELECT n / other FROM ydb.default.expression_edges WHERE id = 4", "DIVISION_BY_ZERO");
            expectError(client, statement, "SELECT n / BIGINT '-1' FROM ydb.default.expression_edges WHERE id = 3", "NUMERIC_VALUE_OUT_OF_RANGE");
            expectError(client, statement, "SELECT n + BIGINT '1' FROM ydb.default.expression_edges WHERE id = 4", "NUMERIC_VALUE_OUT_OF_RANGE");
            evidence.put("nativeFunctionsVisibleInPlan", true);
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
            throw new RuntimeException("Expression HTTP proof failed", failure);
        }
        System.out.println("PASS: expression results, native EXPLAIN, disabled comparison, Unicode, NULLs, integer bounds and Trino arithmetic errors");
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
