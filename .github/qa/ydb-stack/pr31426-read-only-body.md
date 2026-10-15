Closes #28110.

This PR introduces a read-only YDB connector using Trino's JDBC framework.
The initial scope is deliberately small so that the connector can be reviewed
independently of its write paths and query optimizations.

The initial connector includes:

- Catalog configuration, table and column discovery, and reads from the default
  schema.
- Native YDB column mappings, including lossless `Uint64` reads, binary values,
  nullable values, and the supported temporal and decimal types.
- Explicit `NOT_SUPPORTED` errors for writes and table or schema mutations.
- Native JDBC fixture loading, so connector read tests do not depend on
  unimplemented Trino write support.
- Server distribution registration, connector documentation, and a packaged
  server smoke test against a real YDB instance.

The following capabilities are reserved for separate, sequential PRs:

1. `CREATE TABLE` and `INSERT`, primary-key properties, and write staging.
2. Expression, predicate-expression, and projection pushdown.
3. Aggregation pushdown.
4. TopN and limit pushdown.
5. Join pushdown.
6. Pushed `UPDATE` and `DELETE`, affected-row counts, and primary-key guards.
7. Opt-in nontransactional `MERGE` and row-level write lifecycle handling.

The corresponding implementations and tests are preserved outside this initial
diff. This PR does not advertise those deferred capabilities.

Validation for the initial connector uses the complete connector suite, Error
Prone, the documentation build, and a server image built from the same source.
An independent Statement API check reads 25 natively seeded rows, exercises
native type boundaries, and verifies that rejected mutations leave the native
YDB fixture unchanged.
