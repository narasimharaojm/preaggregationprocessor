# Pre-Aggregation Processor

## Description

The Pre-Aggregation Processor is a stateful metrics processor designed to reduce metric cardinality by aggregating metrics over configurable time windows. It performs statistical aggregation (min/max/sum/count/avg) with flexible routing strategies. This processor is ideal for scenarios where high-frequency metrics need to be downsampled while preserving their statistical properties.

**Key Features:**
- Statistical aggregations (min/max/sum/count/avg) with flexible routing
- Time window-based aggregation with configurable temporality
- Metric filtering with include/exclude patterns
- Type-based and pattern-based routing strategies
- Automatic state cleanup to prevent memory leaks

## Processing Mode

### Statistical Mode (`mode: statistical`)
Aggregates metrics using min/max/sum/count/avg operations. Metrics are grouped by their identity (metric name + resource attributes + datapoint attributes) and aggregated within configurable time windows.

**How it works:**
- Metrics are routed to specific aggregation functions based on routing rules (patterns or type)
- Each unique metric identity maintains its own aggregation state
- At window boundaries, aggregated values are emitted and state is reset (for delta temporality) or preserved (for cumulative)

## Configuration

```yaml
processors:
  preaggregation:
    # Processing mode: "statistical"
    mode: statistical

    # Window configuration
    window:
      duration: 60s  # Time window for aggregation
      aggregation_temporality: delta  # or "cumulative"

    # Metric filtering (optional)
    include:
      match_type: glob  # "glob" (easy), "regexp" (advanced), or "strict" (exact)
      patterns:
        - "pp.app.*"
        - "custom.metrics.*"

    exclude:
      match_type: glob
      patterns:
        - "*.internal.*"

    # Routing patterns for statistical mode
    routing_patterns:
      # Pattern matching type (glob is recommended for simplicity)
      match_type: glob  # "glob" (easy), "regexp" (advanced), or "strict" (exact)

      # Pattern-based routing (recommended)
      min_patterns:
        - "*.min"
        - "*.minimum"
        - "*.lowest"
      max_patterns:
        - "*.max"
        - "*.maximum"
        - "*.peak"
      sum_patterns:
        - "*.sum"
        - "*.total"
      count_patterns:
        - "*.count"
        - "*.occurrences"
      avg_patterns:
        - "*.avg"
        - "*.average"
        - "*.mean"

      # Type-based routing (default aggregation by metric type)
      enable_type_based: true
      gauge_default_agg: "avg"        # Default aggregation for gauge metrics
      counter_default_agg: "both"     # "sum", "count", or "both" for counter metrics

    # State management
    max_staleness: 1h  # Remove metrics not seen within this duration
```

## Configuration Options

### Core Configuration

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `mode` | string | `statistical` | No | Processing mode: `statistical` |
| `max_staleness` | duration | `1h` | No | Maximum time before removing stale metric state. Used to prevent memory leaks. |

### Window Configuration

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `window.duration` | duration | `60s` | Yes | Aggregation window duration. Must be positive. |
| `window.aggregation_temporality` | string | `delta` | No | Temporality for emitted metrics: `delta` (reset at window) or `cumulative` (running total) |

### Metric Filtering

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `include.match_type` | string | `glob` | If patterns set | Match type for include patterns: `glob` (recommended), `regexp`, or `strict` |
| `include.patterns` | []string | `[]` | No | Metric name patterns to include. Only metrics matching these patterns will be processed. |
| `exclude.match_type` | string | `glob` | If patterns set | Match type for exclude patterns: `glob` (recommended), `regexp`, or `strict` |
| `exclude.patterns` | []string | `[]` | No | Metric name patterns to exclude. Metrics matching these patterns will be skipped. |

**Note:** Include filters are applied first, then exclude filters. A metric must match include patterns (if set) AND not match exclude patterns (if set) to be processed.

#### Match Types

| Match Type | Syntax | Description | Example Pattern | Matches | Best For |
|------------|--------|-------------|----------------|---------|----------|
| `glob` | Shell-style wildcards | Simple, user-friendly pattern matching using `*` (any chars) and `?` (one char) | `*.min`<br>`cpu.*`<br>`test.?.value` | `api.latency.min`<br>`cpu.usage`<br>`test.1.value` | **Recommended** for most use cases. Easy to write and understand. |
| `regexp` | Regular expressions | Advanced pattern matching with full regex syntax | `.*\.min$`<br>`^cpu\..*`<br>`test\.\d+\.value` | `api.latency.min`<br>`cpu.usage`<br>`test.123.value` | Complex patterns requiring advanced regex features. |
| `strict` | Exact matching | Literal string matching (no wildcards) | `api.latency.min` | Only `api.latency.min` (exact match) | Performance-critical scenarios with known metric names. |

**Glob Pattern Examples:**
```yaml
routing_patterns:
  match_type: glob
  min_patterns:
    - "*.min"              # Matches: api.latency.min, cpu.usage.min
    - "*_min"              # Matches: api_latency_min, cpu_usage_min
    - "cpu.*.min"          # Matches: cpu.usage.min, cpu.idle.min
    - "temp.sensor.?.min"  # Matches: temp.sensor.1.min, temp.sensor.a.min
```

**Regex Pattern Examples:**
```yaml
routing_patterns:
  match_type: regexp
  min_patterns:
    - ".*\\.min$"                    # Ends with .min
    - "^(cpu|memory)\\..*\\.min$"    # cpu.*.min or memory.*.min
    - ".*\\.(min|minimum)$"          # Ends with .min OR .minimum
```

**Strict Pattern Examples:**
```yaml
routing_patterns:
  match_type: strict
  min_patterns:
    - "api.latency.min"    # Only matches this exact string
    - "cpu.usage.min"      # Only matches this exact string
```

### Routing Patterns

#### Pattern-Based Routing (Recommended)
Pattern-based routing uses patterns (glob or regex) to match metric names and route them to specific aggregation types.

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `routing_patterns.match_type` | string | `glob` | No | Pattern matching type: `glob` (recommended), `regexp`, or `strict`. |
| `routing_patterns.min_patterns` | []string | `[]` | No | Patterns for metrics to aggregate as minimum. Metric name is preserved in output. |
| `routing_patterns.max_patterns` | []string | `[]` | No | Patterns for metrics to aggregate as maximum. Metric name is preserved in output. |
| `routing_patterns.sum_patterns` | []string | `[]` | No | Patterns for metrics to aggregate as sum. Metric name is preserved in output. |
| `routing_patterns.count_patterns` | []string | `[]` | No | Patterns for metrics to aggregate as count. Metric name is preserved in output. |
| `routing_patterns.avg_patterns` | []string | `[]` | No | Patterns for metrics to aggregate as average. Metric name is preserved in output. |

**Glob Pattern Examples (Recommended):**
- `"*.min"` - Matches metrics ending with `.min` (e.g., `api.latency.min`)
- `"cpu.*"` - Matches metrics starting with `cpu.` (e.g., `cpu.usage`, `cpu.idle`)
- `"*.p99"` - Matches metrics ending with `.p99` (e.g., `response.time.p99`)
- `"temp_*_avg"` - Matches metrics with `temp_` prefix and `_avg` suffix

**Regex Pattern Examples (Advanced):**
- `".*\\.min$"` - Matches metrics ending with `.min`
- `"^system\\.cpu\\..*"` - Matches metrics starting with `system.cpu.`
- `".*\\.(min|minimum)$"` - Matches metrics ending with `.min` OR `.minimum`

#### Type-Based Routing

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `routing_patterns.enable_type_based` | bool | `false` | No | Enable automatic routing based on metric data type when patterns don't match |
| `routing_patterns.gauge_default_agg` | string | `avg` | No | Default aggregation for gauge metrics: `avg` |
| `routing_patterns.counter_default_agg` | string | `sum` | No | Default aggregation for counter/sum metrics: `sum`, `count`, or `both` |

## Routing Priority

The processor uses the following priority order to determine how to aggregate metrics:

1. **Pattern-based routing** (Highest Priority): If a metric matches any of the `*_patterns` configurations, it will be aggregated accordingly. The metric name is preserved in the output.
   - Example: `api.latency.min` matches `".*\\.min$"` in `min_patterns` → aggregated as minimum, output as `api.latency.min`

2. **Type-based routing**: If `enable_type_based` is true and the metric doesn't match any pattern:
   - **Gauge metrics**: Aggregated using `gauge_default_agg` (default: `avg`)
   - **Counter/Sum metrics**: Aggregated using `counter_default_agg` (default: `sum`). If set to `both`, the processor emits both sum and count aggregations.

3. **Pass-through**: If none of the above match, the metric passes through unchanged.

## Configuration Scenarios

### Scenario 1: Simple Statistical Aggregation with Patterns

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 60s
      aggregation_temporality: delta
    routing_patterns:
      min_patterns:
        - ".*\\.min$"
      max_patterns:
        - ".*\\.max$"
      sum_patterns:
        - ".*\\.sum$"
      count_patterns:
        - ".*\\.count$"
      avg_patterns:
        - ".*\\.avg$"
    max_staleness: 2h
```

**Behavior:**
- `api.latency.min` → Aggregated as minimum, output as `api.latency.min` (name preserved)
- `api.latency.max` → Aggregated as maximum, output as `api.latency.max` (name preserved)
- `requests.count` → Aggregated as count, output as `requests.count` (name preserved)

### Scenario 2: Pattern-Based Routing

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 60s
    routing_patterns:
      avg_patterns:
        - "^system\\.cpu\\..*"
        - "^system\\.memory\\..*"
      sum_patterns:
        - "^http\\.requests\\.total$"
      max_patterns:
        - "^cache\\.hit\\.peak$"
```

**Behavior:**
- `system.cpu.utilization` → Aggregated as average (pattern match, name preserved)
- `http.requests.total` → Aggregated as sum (pattern match, name preserved)
- `cache.hit.peak` → Aggregated as maximum (pattern match, name preserved)

### Scenario 3: Type-Based Default Routing

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 60s
    routing_patterns:
      enable_type_based: true
      gauge_default_agg: "avg"
      counter_default_agg: "both"  # Emit both sum and count
```

**Behavior:**
- Any Gauge metric → Aggregated as average
- Any Counter metric → Two outputs: aggregated sum and aggregated count

### Scenario 4: Mixed Routing Strategy

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 60s
      aggregation_temporality: delta
    routing_patterns:
      min_patterns:
        - ".*\\.min$"
        - ".*\\.minimum$"
      max_patterns:
        - ".*\\.max$"
        - ".*\\.peak$"
      avg_patterns:
        - "^system\\.cpu\\..*"
        - "^system\\.memory\\..*"
      sum_patterns:
        - ".*\\.total$"
      enable_type_based: true
      gauge_default_agg: "avg"
      counter_default_agg: "both"
```

**Routing Examples:**
1. `api.latency.min` → min aggregation (pattern match, output: `api.latency.min`)
2. `system.cpu.usage` (Gauge) → avg aggregation (pattern match, output: `system.cpu.usage`)
3. `http.requests.total` (Counter) → sum aggregation (pattern match, output: `http.requests.total`)
4. `app.temperature` (Gauge, no patterns) → avg aggregation (type-based)
5. `page.views` (Counter, no patterns) → both sum and count (type-based)

### Scenario 5: Statistical Aggregation with Filtering

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 60s
      aggregation_temporality: delta
    include:
      match_type: regexp
      patterns:
        - "^pp\\.app\\..*"
        - "^custom\\.metrics\\..*"
    exclude:
      match_type: regexp
      patterns:
        - ".*\\.internal\\..*"
    routing_patterns:
      min_patterns:
        - ".*\\.min$"
      max_patterns:
        - ".*\\.max$"
      sum_patterns:
        - ".*\\.sum$"
      count_patterns:
        - ".*\\.count$"
      avg_patterns:
        - ".*\\.avg$"
      enable_type_based: true
      gauge_default_agg: "avg"
      counter_default_agg: "sum"
    max_staleness: 1h
```

**Behavior:**
- Metrics matching `pp.app.*` or `custom.metrics.*` are processed
- Metrics matching `*.internal.*` are excluded even if they match include patterns

## Detailed Examples

### Example 1: Statistical Aggregation (Min/Max)

**Input Metrics (arriving over 60 seconds):**
```
Time  Metric Name          Value  Attributes
0s    api.latency.min      10ms   {service=api, endpoint=/login}
10s   api.latency.min      8ms    {service=api, endpoint=/login}
20s   api.latency.min      12ms   {service=api, endpoint=/login}
30s   api.latency.max      100ms  {service=api, endpoint=/login}
45s   api.latency.max      150ms  {service=api, endpoint=/login}
50s   api.latency.max      120ms  {service=api, endpoint=/login}
```

**Output (emitted at 60s window boundary):**
```
api.latency.min (min aggregation) = 8ms   {service=api, endpoint=/login}
api.latency.max (max aggregation) = 150ms {service=api, endpoint=/login}
```

---

### Example 2: Statistical Aggregation (Sum/Count/Avg)

**Input Metrics (over 60 seconds):**
```
Time  Metric Name          Value  Attributes
0s    requests.count       50     {service=api}
15s   requests.count       75     {service=api}
30s   requests.count       60     {service=api}
45s   requests.count       90     {service=api}

0s    bytes.sent.sum       1024   {service=api}
20s   bytes.sent.sum       2048   {service=api}
40s   bytes.sent.sum       1536   {service=api}

10s   response.time.avg    45ms   {service=api}
25s   response.time.avg    52ms   {service=api}
50s   response.time.avg    48ms   {service=api}
```

**Output (at window boundary):**
```
requests.count       (count agg) = 275       {service=api}  # 50+75+60+90
bytes.sent.sum       (sum agg)   = 4608      {service=api}  # 1024+2048+1536
response.time.avg    (avg agg)   = 48.33ms   {service=api}  # (45+52+48)/3
```

---

### Example 3: Type-Based Routing with Counter

**Input Metrics:**
```
Time  Metric Name      Type     Value  Attributes
5s    http.requests    Counter  100    {endpoint=/api}
15s   http.requests    Counter  150    {endpoint=/api}
25s   http.requests    Counter  120    {endpoint=/api}
35s   http.requests    Counter  180    {endpoint=/api}
```

**Output (at window boundary):**
```
http.requests (sum aggregation)   = 550  {endpoint=/api}  # 100+150+120+180
http.requests (count aggregation) = 4    {endpoint=/api}  # 4 data points
```

## Implementation Notes

### State Management

**State Storage:**
- Each unique metric identity (name + resource attributes + datapoint attributes) maintains its own aggregation state
- State is stored in concurrent-safe `sync.Map` structures for thread safety

**State Lifecycle:**
1. **Creation:** State is created when a metric is first seen
2. **Update:** Each incoming data point updates the aggregation state
3. **Emission:** At window boundaries, aggregated values are emitted
4. **Reset/Cleanup:** Based on `aggregation_temporality`:
   - **Delta:** State is reset after emission
   - **Cumulative:** State continues accumulating
5. **Staleness:** State not updated within `max_staleness` duration is removed

**Memory Considerations:**
- Memory usage is proportional to the number of unique metric identities
- Use `max_staleness` to prevent unbounded memory growth
- Use `include` patterns to limit processed metrics

### Performance Optimizations

1. **Efficient Hashing:** Uses `pdatautil.MapHash` for fast attribute hashing
2. **Concurrent Access:** `sync.Map` enables concurrent read/write operations
3. **RemoveIf Pattern:** Minimizes metric copying during processing
4. **Bounded Memory:** Automatic cleanup of stale state prevents memory leaks
5. **Timer-Based Windows:** Window flush is independent of batch arrival patterns

**Performance Tips:**
- Use stricter include/exclude patterns to reduce processed metrics
- Adjust `window.duration` based on your aggregation needs
- Monitor memory usage and adjust `max_staleness` accordingly

### Statefulness and Distribution

**Important Considerations:**
- This processor is **stateful** and maintains aggregation state across batches
- State is local to each processor instance (not distributed)
- In distributed deployments, ensure consistent routing of metrics to the same processor instance
- For high-availability setups, consider the implications of processor restarts (state is lost)

### Window Flush Behavior

Windows are time-based and wall-clock aligned, independent of batch arrivals:
- A timer triggers at `window.duration` intervals aligned to wall-clock boundaries
- All accumulated state is emitted at window boundaries
- For `delta` temporality, state is reset after emission
- For `cumulative` temporality, state continues accumulating

**Example Timeline (60s window):**
```
T=0s:   Window starts
T=10s:  Metric batch arrives → state updated
T=25s:  Metric batch arrives → state updated
T=40s:  Metric batch arrives → state updated
T=60s:  Window ends → aggregated metrics emitted → state reset/preserved
T=60s:  New window starts
```

## Troubleshooting

### High Memory Usage

**Solutions:**
1. Reduce staleness duration: `max_staleness: 30m`
2. Use include patterns to limit processed metrics
3. Reduce window duration for more frequent emissions
4. Review metric dimensions (attributes) — each unique combination creates a separate state entry

### Missing Metrics

**Diagnostic Steps:**
1. Check include/exclude patterns — verify they match the metric names
2. Verify routing configuration — test patterns against actual metric names
3. Check processor logs for validation errors
4. Verify metric types for type-based routing

**Example Debug Configuration:**
```yaml
service:
  telemetry:
    logs:
      level: debug
```

### Incorrect Aggregations

**Solutions:**
1. Verify window duration matches expected period
2. Check aggregation temporality (`delta` vs `cumulative`)
3. Verify metric dimensions are consistent — `{service=api}` and `{service=API}` are different identities
4. Review routing priority — pattern-based (priority 1) → type-based (priority 2)

### Metrics Passing Through Unchanged

**Common Causes:**
1. No routing rules match the metric — metrics pass through unchanged by design
2. Include/exclude filters prevent processing

**Solution:** Enable type-based routing to catch all metrics:
```yaml
routing_patterns:
  enable_type_based: true
  gauge_default_agg: "avg"
  counter_default_agg: "sum"
```

### Performance Degradation

**Solutions:**
1. Reduce processed metric count with filters
2. Optimize pattern complexity — use simple glob patterns when possible
3. Adjust window duration — longer windows mean less frequent processing
4. Monitor state size — large number of unique identities increases processing time

## Common Use Cases

### Use Case 1: Downsampling High-Frequency Metrics
**Scenario:** Metrics arrive every second, but only minute-level granularity is needed for storage.

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 60s
      aggregation_temporality: delta
    routing_patterns:
      enable_type_based: true
      gauge_default_agg: "avg"
      counter_default_agg: "sum"
    max_staleness: 2h
```

**Benefit:** Reduces metric volume by 60x (60 samples → 1 aggregated metric per minute)

---

### Use Case 2: Multi-Source Aggregation
**Scenario:** Multiple application instances send the same metrics; aggregate before storage.

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 30s
      aggregation_temporality: delta
    routing_patterns:
      sum_patterns:
        - ".*\\.total$"
      avg_patterns:
        - ".*\\.avg$"
      max_patterns:
        - ".*\\.max$"
```

---

### Use Case 3: Cost Reduction for Cloud Monitoring
**Scenario:** Reduce costs by decreasing the number of metrics sent to expensive cloud monitoring services.

```yaml
processors:
  preaggregation:
    mode: statistical
    window:
      duration: 300s  # 5-minute aggregation
      aggregation_temporality: delta
    include:
      match_type: regexp
      patterns:
        - "^prod\\..*"
    routing_patterns:
      enable_type_based: true
      gauge_default_agg: "avg"
      counter_default_agg: "sum"
```

---

### Use Case 4: Cardinality Reduction
**Scenario:** High-cardinality metrics are causing storage issues.

```yaml
processors:
  attributes:
    actions:
      - key: instance_id
        action: delete  # Remove high-cardinality attribute

  preaggregation:
    mode: statistical
    window:
      duration: 60s
      aggregation_temporality: delta
    routing_patterns:
      enable_type_based: true
      gauge_default_agg: "avg"
      counter_default_agg: "both"
```

## Quick Reference

### Routing Decision Tree

```
For each incoming metric:
├─ Does it match include patterns? (if configured)
│  ├─ No → Pass through unchanged
│  └─ Yes → Continue
│
├─ Does it match exclude patterns? (if configured)
│  ├─ Yes → Pass through unchanged
│  └─ No → Continue
│
├─ Which routing rule matches?
│  ├─ 1. Pattern-based (RECOMMENDED — min_patterns, max_patterns, etc.)?
│  │   └─ Yes → Use that aggregation, PRESERVE metric name
│  │   Example: "api.latency.min" → min aggregation → output: "api.latency.min"
│  │
│  ├─ 2. Type-based (enable_type_based: true)?
│  │   ├─ Gauge   → Use gauge_default_agg
│  │   └─ Counter → Use counter_default_agg
│  │
│  └─ 3. None of the above → Pass through unchanged
```

### Configuration Cheat Sheet

| Goal | Configuration |
|------|---------------|
| Aggregate metrics ending in `.min` | `min_patterns: [".*\\.min$"]` |
| Aggregate metrics ending in `.max` | `max_patterns: [".*\\.max$"]` |
| Aggregate metrics ending in `.sum` | `sum_patterns: [".*\\.sum$"]` |
| Aggregate metrics ending in `.avg` | `avg_patterns: [".*\\.avg$"]` |
| Aggregate all gauges as average | `enable_type_based: true`<br>`gauge_default_agg: "avg"` |
| Aggregate all counters as sum | `enable_type_based: true`<br>`counter_default_agg: "sum"` |
| Get both sum and count for counters | `enable_type_based: true`<br>`counter_default_agg: "both"` |
| Process only specific metrics | `include.patterns: ["^app\\..*"]` |
| Skip specific metrics | `exclude.patterns: [".*\\.debug\\..*"]` |
| 1-minute aggregation windows | `window.duration: 60s` |
| Reset aggregations each window | `aggregation_temporality: delta` |
| Cumulative aggregations | `aggregation_temporality: cumulative` |
| Clean stale metrics after 1 hour | `max_staleness: 1h` |

**Pattern Syntax Tips:**
- `".*\\.min$"` - Matches anything ending with `.min`
- `"^system\\."` - Matches anything starting with `system.`
- `".*_p99$"` - Matches anything ending with `_p99`
- Escape dots with `\\.` to match literal dots in metric names

### Aggregation Type Reference

| Type | Description | Input Example | Output |
|------|-------------|---------------|--------|
| `min` | Minimum value in window | `[10, 8, 12, 9]` | `8` |
| `max` | Maximum value in window | `[10, 8, 12, 9]` | `12` |
| `sum` | Sum of all values | `[10, 8, 12, 9]` | `39` |
| `count` | Count of data points | `[10, 8, 12, 9]` | `4` |
| `avg` | Average of all values | `[10, 8, 12, 9]` | `9.75` |
| `both` | Sum + Count (counters only) | `[10, 8, 12, 9]` | `39 (sum), 4 (count)` |


## Status

This processor is in **development** status. For the latest stability information, see [metadata.yaml](metadata.yaml).
