# Overview of the current state of the query optimizer in Limbo

Query optimization is obviously an important part of any SQL-based database engine. This document is an overview of what we currently do.

## Join reordering and optimal index selection

**The goals of query optimization are at least the following:**

1. Do as little page I/O as possible
2. Do as little CPU work as possible
3. Retain query correctness.

**The most important ways to achieve #1 and #2 are:**

1. Choose the optimal access method for each table (e.g. an index or a rowid-based seek, or a full table scan if all else fails).
2. Choose the best or near-best way to reorder the tables in the query so that those optimal access methods can be used.
3. Also factor in whether the chosen join order and indexes allow removal of any sort operations that are necessary for query correctness.

## Limbo's optimizer

Limbo's optimizer is an implementation of an extremely traditional [IBM System R](https://www.cs.cmu.edu/~15721-f24/slides/02-Selinger-SystemR-opt.pdf) style optimizer,
i.e. straight from the 70s! The main ideas are:

1. Find the best (least `cost`) way to access any single table in the query (n=1). Estimate the `output cardinality` (row count) for this table.
    - For example, if there is a WHERE clause condition `t1.x = 5` and we have an index on `t1.x`, that index is potentially going to be the best way to access `t1`. Assuming `t1` has `1,000,000` rows, we might estimate that the output cardinality of this will be `10,000` after all the filters on `t1` have been applied.
2. For each result of #1, find the best way to join that result with each other table (n=2). Use the output cardinality of the previous step as the `input cardinality` of this step.
3. For each result of #2, find the best way to join the result of that 2-way join with each other table (n=3)
...
n. Find the best way to join each (n-1)-way join with the remaining table.

The intermediate steps of the above algorithm are memoized, and finally the join order and access methods with the least cumulative cost is chosen.

### Estimation of cost and cardinalities + a note on table statistics

Currently, in the absence of `ANALYZE`, `sqlite_stat1` etc. we assume the following:

1. Each table has `1,000,000` rows.
2. Each equality (`=`) filter will filter out some percentage of the result set.
3. Each nonequality (e.g. `>`) will filter out some smaller percentage of the result set.
4. Each `4096` byte database page holds `50` rows, i.e. roughly `80` bytes per row 
5. Sort operations have some CPU cost dependent on the number of input rows to the sort operation.

From the above, we derive the following formula for estimating the cost of joining `t1` with `t2`

```
JOIN_COST = COST(t1.rows) + t1.rows * COST(t2.rows) + E

where
  COST(rows) = PAGE_IO(rows)
  and
  E = one-time cost to build ephemeral index if needed (usually 0)
```

For example, let's take the query `SELECT * FROM t1 JOIN t2 USING(foo) WHERE t2.foo > 10`. Let's assume the following:

- `t1` has `6400` rows and `t2` has `8000` rows
- there are no indexes at all
- let's ignore the CPU cost from the equation for simplicity.

The best access method for both is a full table scan. The output cardinality of `t1` is the full table, because nothing is filtering it. Hence, the cost of `t1 JOIN t2` becomes:

```
JOIN_COST = COST(t1.input_rows) + t1.output_rows * COST(t2.input_rows)

// plugging in the values:

JOIN_COST = COST(6400) + 6400 * COST(8000)
JOIN_COST = 80 + 6400 * 100 = 640080
```

Now let's consider `t2 JOIN t1`. The best access method for both is still a full scan, but since we can filter on `t2.foo > 10`, its output cardinality decreases. Let's assume only 1/4 of the rows of `t2` match the condition `t2.foo > 10`. Hence, the cost of `t2 join t1` becomes:

```
JOIN_COST = COST(t2.input_rows) + t2.output_rows * COST(t1.input_rows)

// plugging in the values:

JOIN_COST = COST(8000) + 1/4 * 8000 * COST(6400)
JOIN_COST = 100 + 2000 * 80 = 160100
```

Even though `t2` is a larger table, because we were able to reduce the input set to the join operation, it's dramatically cheaper.

#### Statistics

Since we don't support `ANALYZE`, nor can we assume that users will call `ANALYZE` anyway, we use simple magic constants to estimate the selectivity of join predicates, row count of tables, and so on. When we have support for `ANALYZE`, we should plug the statistics from `sqlite_stat1` and friends into the optimizer to make more informed decisions.

### Estimating the output cardinality of a join

The output cardinality (output row count) of an operation is as follows:

```
OUTPUT_CARDINALITY_JOIN = INPUT_CARDINALITY_RHS * OUTPUT_CARDINALITY_RHS

where

INPUT_CARDINALITY_RHS = OUTPUT_CARDINALITY_LHS
```

example:

```
SELECT * FROM products p JOIN order_lines o ON p.id = o.product_id
```
Assuming there are 100 products, i.e. just selecting all products would yield 100 rows:

```
OUTPUT_CARDINALITY_LHS = 100
INPUT_CARDINALITY_RHS = 100
```

Assuming p.id = o.product_id will return three orders per each product:

```
OUTPUT_CARDINALITY_RHS = 3

OUTPUT_CARDINALITY_JOIN = 100 * 3 = 300
```
i.e. the join is estimated to return 300 rows, 3 for each product.

Again, in the absence of statistics, we use magic constants to estimate these cardinalities.

Estimating them is important because in multi-way joins the output cardinality of the previous join becomes the input cardinality of the next one.