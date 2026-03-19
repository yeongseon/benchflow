"""Publication-quality HTML report generation for BenchFlow benchmark results.

Produces a single self-contained HTML file suitable for inclusion in academic
papers (VLDB, SIGMOD, OSDI) and professional engineering evaluations.

Features:
- Paper theme (default, white bg, serif headings, booktabs tables) + dark theme toggle
- Okabe-Ito colorblind-safe palette
- ECDF, CI error-bar, and time-series charts via Plotly.js
- Aggregate statistics table for multi-iteration experiments
"""

from __future__ import annotations

from jinja2 import Template

from benchflow.core.result import RunResult

# ---------------------------------------------------------------------------
# Okabe-Ito colorblind-safe palette
# ---------------------------------------------------------------------------
OKABE_ITO = [
    "#E69F00",  # orange
    "#56B4E9",  # sky blue
    "#009E73",  # bluish green
    "#F0E442",  # yellow
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
    "#000000",  # black
]

# ---------------------------------------------------------------------------
# Jinja2 template — single self-contained HTML
# ---------------------------------------------------------------------------
REPORT_TEMPLATE = Template("""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BenchFlow Report — {{ result.scenario.name }}</title>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Crimson+Pro:wght@400;600;700&family=Source+Sans+3:ital,wght@0,300;0,400;0,600;1,400&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
/* ===== CSS Custom Properties ===== */
:root {
  /* Paper theme (default) */
  --bg: #ffffff;
  --surface: #ffffff;
  --border: #d0d0d0;
  --text: #1a1a1a;
  --text-secondary: #555555;
  --heading: #1a1a1a;
  --rule-thick: 2px;
  --rule-thin: 1px;
  --accent: #0072B2;
  --plotly-bg: #ffffff;
  --plotly-grid: #e8e8e8;
  --plotly-font: #1a1a1a;
  --badge-bg: #f0f0f0;
  --badge-text: #333333;
  --toggle-bg: #e0e0e0;
  --toggle-fg: #333333;
  --shadow: 0 1px 3px rgba(0,0,0,0.08);
}

[data-theme="dark"] {
  --bg: #0d1117;
  --surface: #161b22;
  --border: #30363d;
  --text: #c9d1d9;
  --text-secondary: #8b949e;
  --heading: #e6edf3;
  --rule-thick: 2px;
  --rule-thin: 1px;
  --accent: #58a6ff;
  --plotly-bg: #161b22;
  --plotly-grid: #30363d;
  --plotly-font: #c9d1d9;
  --badge-bg: #21262d;
  --badge-text: #c9d1d9;
  --toggle-bg: #30363d;
  --toggle-fg: #c9d1d9;
  --shadow: 0 1px 3px rgba(0,0,0,0.3);
}

/* ===== Reset & Base ===== */
*, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: 'Source Sans 3', 'Source Sans Pro', -apple-system, sans-serif;
  font-weight: 400;
  font-size: 15px;
  line-height: 1.6;
  background: var(--bg);
  color: var(--text);
  padding: 3rem 2rem;
  max-width: 1100px;
  margin: 0 auto;
  transition: background 0.3s ease, color 0.3s ease;
}

/* ===== Typography ===== */
h1, h2, h3 {
  font-family: 'Crimson Pro', 'Georgia', serif;
  font-weight: 700;
  color: var(--heading);
  letter-spacing: -0.01em;
}

h1 {
  font-size: 2rem;
  margin-bottom: 0.25rem;
  border-bottom: var(--rule-thick) solid var(--heading);
  padding-bottom: 0.5rem;
}

h2 {
  font-size: 1.35rem;
  margin-bottom: 1rem;
  color: var(--heading);
}

/* ===== Theme Toggle ===== */
.theme-toggle {
  position: fixed;
  top: 1.2rem;
  right: 1.5rem;
  z-index: 1000;
  background: var(--toggle-bg);
  border: 1px solid var(--border);
  color: var(--toggle-fg);
  padding: 0.35rem 0.75rem;
  border-radius: 6px;
  font-family: 'Source Sans 3', sans-serif;
  font-size: 0.8rem;
  cursor: pointer;
  transition: all 0.2s ease;
  box-shadow: var(--shadow);
}
.theme-toggle:hover { opacity: 0.85; }

/* ===== Metadata ===== */
.meta {
  color: var(--text-secondary);
  font-size: 0.9rem;
  margin-bottom: 2.5rem;
  line-height: 1.8;
}
.meta strong { color: var(--text); font-weight: 600; }

.badge {
  display: inline-block;
  background: var(--badge-bg);
  color: var(--badge-text);
  padding: 0.15rem 0.5rem;
  border-radius: 4px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.78rem;
  margin-left: 0.25rem;
}

/* ===== Sections ===== */
.section {
  margin-bottom: 2.5rem;
}

/* ===== Environment Grid ===== */
.env-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 0.6rem 1.5rem;
}
.env-item {
  font-size: 0.88rem;
  color: var(--text);
}
.env-label {
  color: var(--text-secondary);
  font-weight: 300;
  margin-right: 0.3rem;
}

/* ===== Experiment Config ===== */
.config-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 0.5rem 1.5rem;
}
.config-item {
  font-size: 0.88rem;
}
.config-value {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.82rem;
  color: var(--accent);
}

/* ===== Booktabs Tables ===== */
table {
  width: 100%;
  border-collapse: collapse;
  font-variant-numeric: tabular-nums;
  font-size: 0.88rem;
  margin-bottom: 0.5rem;
}

table thead tr {
  border-top: var(--rule-thick) solid var(--text);
  border-bottom: var(--rule-thin) solid var(--text-secondary);
}

table tbody tr:last-child {
  border-bottom: var(--rule-thick) solid var(--text);
}

table tbody tr {
  border-bottom: none;
}

th {
  font-weight: 600;
  text-align: left;
  padding: 0.5rem 0.75rem;
  color: var(--text-secondary);
  font-size: 0.82rem;
  text-transform: none;
  letter-spacing: 0.02em;
}

td {
  padding: 0.4rem 0.75rem;
  color: var(--text);
}

th.num, td.num { text-align: right; }

td.ci {
  font-size: 0.82rem;
  color: var(--text-secondary);
}

/* ===== Charts ===== */
.chart-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(480px, 1fr));
  gap: 2rem;
}

.chart-container {
  width: 100%;
}

.chart {
  width: 100%;
  height: 380px;
}

.chart-caption {
  font-size: 0.8rem;
  color: var(--text-secondary);
  text-align: center;
  margin-top: 0.4rem;
  font-style: italic;
}

/* ===== Footer ===== */
.footer {
  margin-top: 3rem;
  padding-top: 1rem;
  border-top: var(--rule-thin) solid var(--border);
  font-size: 0.78rem;
  color: var(--text-secondary);
  text-align: center;
}

/* ===== Print ===== */
@media print {
  .theme-toggle { display: none; }
  body { padding: 1rem; font-size: 11pt; }
  .chart { height: 300px; }
}
</style>
</head>
<body>

<button class="theme-toggle" id="theme-toggle" aria-label="Toggle dark mode">◑ Dark</button>

<!-- ===== Header ===== -->
<header class="section">
  <h1>BenchFlow Report</h1>
  <div class="meta">
    Scenario: <strong>{{ result.scenario.name }}</strong><br>
    Run ID: <span class="badge">{{ result.run_id }}</span>
    &emsp;{{ result.created_at[:19] }} UTC
    &emsp;Schema v{{ result.schema_version }}
    {% if result.benchflow.git_sha %}&emsp;Git <span class="badge">{{ result.benchflow.git_sha }}</span>{% endif %}
  </div>
</header>

<!-- ===== Environment ===== -->
<section class="section">
  <h2>Environment</h2>
  <div class="env-grid">
    <div class="env-item"><span class="env-label">Host:</span>{{ result.environment.hostname }}</div>
    <div class="env-item"><span class="env-label">OS:</span>{{ result.environment.os }}</div>
    <div class="env-item"><span class="env-label">CPUs:</span>{{ result.environment.cpu_count }}</div>
    {% if result.environment.cpu_model %}
    <div class="env-item"><span class="env-label">CPU:</span>{{ result.environment.cpu_model }}</div>
    {% endif %}
    {% if result.environment.memory_gb %}
    <div class="env-item"><span class="env-label">Memory:</span>{{ result.environment.memory_gb }} GB</div>
    {% endif %}
    <div class="env-item"><span class="env-label">Python:</span>{{ result.environment.python_version }}</div>
    <div class="env-item"><span class="env-label">Database:</span>{{ result.db.kind }}{% if result.db.server_version %} {{ result.db.server_version }}{% endif %}</div>
    <div class="env-item"><span class="env-label">BenchFlow:</span>v{{ result.benchflow.version }}</div>
  </div>
  {% if result.db.server_config %}
  <details style="margin-top: 0.8rem; font-size: 0.85rem;">
    <summary style="cursor: pointer; color: var(--text-secondary);">Database Configuration ({{ result.db.server_config|length }} parameters)</summary>
    <div style="margin-top: 0.5rem; columns: 2; column-gap: 2rem;">
      {% for key, val in result.db.server_config.items() %}
      <div class="env-item" style="break-inside: avoid;"><span class="env-label">{{ key }}:</span>{{ val }}</div>
      {% endfor %}
    </div>
  </details>
  {% endif %}
</section>

<!-- ===== Experiment Config (multi-iteration) ===== -->
{% if result.iterations_requested > 1 %}
<section class="section">
  <h2>Experiment Configuration</h2>
  <div class="config-grid">
    <div class="config-item"><span class="env-label">Iterations:</span> <span class="config-value">{{ result.iterations_requested }}</span></div>
    {% if result.experiment_seed is not none %}
    <div class="config-item"><span class="env-label">Seed:</span> <span class="config-value">{{ result.experiment_seed }}</span></div>
    {% endif %}
    <div class="config-item"><span class="env-label">Completed:</span> <span class="config-value">{{ result.iterations|length }}</span></div>
  </div>
</section>
{% endif %}

<!-- ===== Summary Table ===== -->
<section class="section">
  <h2>Summary</h2>
  <table>
    <thead>
      <tr>
        <th>Target</th>
        <th>Step</th>
        <th class="num">Ops</th>
        <th class="num">p50 (ms)</th>
        <th class="num">p95 (ms)</th>
        <th class="num">p99 (ms)</th>
        <th class="num">p99.9 (ms)</th>
        <th class="num">Throughput</th>
        <th class="num">Errors</th>
      </tr>
    </thead>
    <tbody>
      {% for target in result.targets %}
      {% for step in target.steps %}
      <tr>
        <td>{{ target.stack_id }}</td>
        <td>{{ step.name }}</td>
        <td class="num">{{ "{:,}".format(step.ops) }}</td>
        <td class="num">{{ "%.2f"|format(step.latency_summary.p50_ns / 1000000) }}</td>
        <td class="num">{{ "%.2f"|format(step.latency_summary.p95_ns / 1000000) }}</td>
        <td class="num">{{ "%.2f"|format(step.latency_summary.p99_ns / 1000000) }}</td>
        <td class="num">{{ "%.2f"|format(step.latency_summary.p999_ns / 1000000) }}</td>
        <td class="num">{{ "{:,.0f}".format(step.throughput_ops_s) }} ops/s</td>
        <td class="num">{{ step.errors }}</td>
      </tr>
      {% endfor %}
      {% endfor %}
    </tbody>
  </table>
</section>

<!-- ===== Aggregate Statistics Table (multi-iteration) ===== -->
{% if result.aggregate %}
<section class="section">
  <h2>Aggregate Statistics <span style="font-size: 0.85rem; font-weight: 400; color: var(--text-secondary);">(across {{ result.iterations|length }} iterations)</span></h2>
  <table>
    <thead>
      <tr>
        <th>Target</th>
        <th>Step</th>
        <th class="num">Throughput (ops/s)</th>
        <th class="num">p50 (ms)</th>
        <th class="num">p95 (ms)</th>
        <th class="num">p99 (ms)</th>
        <th class="num">CV%</th>
      </tr>
    </thead>
    <tbody>
      {% for agg_target in result.aggregate %}
      {% for agg_step in agg_target.steps %}
      <tr>
        <td>{{ agg_target.stack_id }}</td>
        <td>{{ agg_step.step_name }}</td>
        <td class="num">
          {{ "{:,.0f}".format(agg_step.throughput_ops_s.mean) }}
          <span class="ci">&plusmn; {{ "{:,.0f}".format((agg_step.throughput_ops_s.ci.high - agg_step.throughput_ops_s.ci.low) / 2) }}</span>
        </td>
        <td class="num">
          {{ "%.2f"|format(agg_step.p50_ns.mean / 1000000) }}
          <span class="ci">&plusmn; {{ "%.2f"|format((agg_step.p50_ns.ci.high - agg_step.p50_ns.ci.low) / 2 / 1000000) }}</span>
        </td>
        <td class="num">
          {{ "%.2f"|format(agg_step.p95_ns.mean / 1000000) }}
          <span class="ci">&plusmn; {{ "%.2f"|format((agg_step.p95_ns.ci.high - agg_step.p95_ns.ci.low) / 2 / 1000000) }}</span>
        </td>
        <td class="num">
          {{ "%.2f"|format(agg_step.p99_ns.mean / 1000000) }}
          <span class="ci">&plusmn; {{ "%.2f"|format((agg_step.p99_ns.ci.high - agg_step.p99_ns.ci.low) / 2 / 1000000) }}</span>
        </td>
        <td class="num">{{ "%.1f"|format(agg_step.throughput_ops_s.cv * 100) }}%</td>
      </tr>
      {% endfor %}
      {% endfor %}
    </tbody>
  </table>
  <p style="font-size: 0.78rem; color: var(--text-secondary); margin-top: 0.3rem;">
    Values shown as mean &plusmn; half-width of 95% bootstrap CI. CV = coefficient of variation of throughput.
  </p>
</section>
{% endif %}

<!-- ===== Charts ===== -->
<section class="section">
  <h2>Charts</h2>
  <div class="chart-grid">
    <div class="chart-container">
      <div id="latency-chart" class="chart"></div>
      <div class="chart-caption">Figure 1. Latency percentiles (p50, p95, p99){% if result.aggregate %} with 95% CI error bars{% endif %}.</div>
    </div>
    <div class="chart-container">
      <div id="throughput-chart" class="chart"></div>
      <div class="chart-caption">Figure 2. Throughput{% if result.aggregate %} with 95% CI error bars{% endif %}.</div>
    </div>
  </div>
</section>

{% if has_samples %}
<section class="section">
  <div id="ecdf-chart" class="chart"></div>
  <div class="chart-caption">Figure 3. Empirical CDF of latency distribution.</div>
</section>
{% endif %}

{% if has_time_series %}
<section class="section">
  <div id="timeseries-chart" class="chart" style="height: 420px;"></div>
  <div class="chart-caption">Figure {{ 4 if has_samples else 3 }}. Time-series: throughput (ops/s) and p95 latency over time.</div>
</section>
{% endif %}

<!-- ===== Footer ===== -->
<footer class="footer">
  Generated by BenchFlow v{{ result.benchflow.version }} &middot; {{ result.created_at[:19] }} UTC
</footer>

<!-- ===== JavaScript ===== -->
<script>
/* --- Theme toggle --- */
(function() {
  const btn = document.getElementById('theme-toggle');
  const saved = localStorage.getItem('benchflow-theme');
  if (saved) document.documentElement.setAttribute('data-theme', saved);

  function updateLabel() {
    const current = document.documentElement.getAttribute('data-theme');
    btn.textContent = current === 'dark' ? '◑ Light' : '◑ Dark';
  }
  updateLabel();

  btn.addEventListener('click', function() {
    const current = document.documentElement.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    if (next === 'light') {
      document.documentElement.removeAttribute('data-theme');
    } else {
      document.documentElement.setAttribute('data-theme', 'dark');
    }
    localStorage.setItem('benchflow-theme', next === 'light' ? '' : 'dark');
    updateLabel();
    replotAll();
  });
})();

/* --- Okabe-Ito palette --- */
const OI = {{ palette }};

/* --- Plotly layout helpers --- */
function getTheme() {
  return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
}

function baseLayout(opts) {
  const dark = getTheme() === 'dark';
  const bg = dark ? '#161b22' : '#ffffff';
  const grid = dark ? '#30363d' : '#e8e8e8';
  const font = dark ? '#c9d1d9' : '#1a1a1a';
  return Object.assign({
    paper_bgcolor: bg,
    plot_bgcolor: bg,
    font: { family: "'Source Sans 3', sans-serif", color: font, size: 13 },
    margin: { t: 30, b: 55, l: 65, r: 55 },
    xaxis: { gridcolor: grid, zeroline: false },
    yaxis: { gridcolor: grid, zeroline: false },
    legend: { orientation: 'h', y: -0.18 },
  }, opts || {});
}

var plotFunctions = [];

function replotAll() {
  plotFunctions.forEach(function(fn) { fn(); });
}

/* --- Data --- */
var labels = [
  {% for target in result.targets %}{% for step in target.steps %}'{{ target.stack_id }}\\n{{ step.name }}',{% endfor %}{% endfor %}
];

/* --- Chart 1: Latency Bar Chart --- */
plotFunctions.push(function() {
  var traces = [];
  {% for i, p in enumerate(['p50', 'p95', 'p99']) %}
  traces.push({
    x: labels,
    y: [{% for target in result.targets %}{% for step in target.steps %}{{ (step.latency_summary[p ~ '_ns'] / 1000000)|round(4) }},{% endfor %}{% endfor %}],
    name: '{{ p }}',
    type: 'bar',
    marker: { color: OI[{{ i }}] },
    {% if result.aggregate %}
    error_y: {
      type: 'data',
      array: [
        {% for agg_target in result.aggregate %}{% for agg_step in agg_target.steps %}{{ ((agg_step[p ~ '_ns'].ci.high - agg_step[p ~ '_ns'].mean) / 1000000)|round(4) }},{% endfor %}{% endfor %}
      ],
      arrayminus: [
        {% for agg_target in result.aggregate %}{% for agg_step in agg_target.steps %}{{ ((agg_step[p ~ '_ns'].mean - agg_step[p ~ '_ns'].ci.low) / 1000000)|round(4) }},{% endfor %}{% endfor %}
      ],
      visible: true,
      thickness: 1.5,
      width: 3,
      color: getTheme() === 'dark' ? '#e6edf3' : '#333333'
    },
    {% endif %}
  });
  {% endfor %}
  Plotly.newPlot('latency-chart', traces, baseLayout({
    barmode: 'group',
    yaxis: { title: 'Latency (ms)', gridcolor: getTheme() === 'dark' ? '#30363d' : '#e8e8e8', zeroline: false },
  }), { responsive: true, displayModeBar: false });
});

/* --- Chart 2: Throughput Bar Chart --- */
plotFunctions.push(function() {
  var trace = {
    x: labels,
    y: [{% for target in result.targets %}{% for step in target.steps %}{{ step.throughput_ops_s }},{% endfor %}{% endfor %}],
    type: 'bar',
    marker: { color: OI[4] },
    {% if result.aggregate %}
    error_y: {
      type: 'data',
      array: [
        {% for agg_target in result.aggregate %}{% for agg_step in agg_target.steps %}{{ (agg_step.throughput_ops_s.ci.high - agg_step.throughput_ops_s.mean)|round(2) }},{% endfor %}{% endfor %}
      ],
      arrayminus: [
        {% for agg_target in result.aggregate %}{% for agg_step in agg_target.steps %}{{ (agg_step.throughput_ops_s.mean - agg_step.throughput_ops_s.ci.low)|round(2) }},{% endfor %}{% endfor %}
      ],
      visible: true,
      thickness: 1.5,
      width: 3,
      color: getTheme() === 'dark' ? '#e6edf3' : '#333333'
    },
    {% endif %}
  };
  Plotly.newPlot('throughput-chart', [trace], baseLayout({
    yaxis: { title: 'ops/s', gridcolor: getTheme() === 'dark' ? '#30363d' : '#e8e8e8', zeroline: false },
  }), { responsive: true, displayModeBar: false });
});

/* --- Chart 3: ECDF --- */
{% if has_samples %}
plotFunctions.push(function() {
  var traces = [];
  var colorIdx = 0;
  {% for target in result.targets %}
  {% for step in target.steps %}
  {% if step.samples_ns %}
  (function() {
    var raw = [{% for s in step.samples_ns %}{{ (s / 1000000)|round(4) }},{% endfor %}];
    raw.sort(function(a,b){ return a-b; });
    var n = raw.length;
    var y = [];
    for (var i = 0; i < n; i++) { y.push((i + 1) / n); }
    traces.push({
      x: raw,
      y: y,
      name: '{{ target.stack_id }} / {{ step.name }}',
      type: 'scatter',
      mode: 'lines',
      line: { shape: 'hv', width: 1.8, color: OI[colorIdx % OI.length] },
    });
  })();
  {% endif %}
  colorIdx++;
  {% endfor %}
  {% endfor %}
  Plotly.newPlot('ecdf-chart', traces, baseLayout({
    xaxis: { title: 'Latency (ms)', gridcolor: getTheme() === 'dark' ? '#30363d' : '#e8e8e8', zeroline: false },
    yaxis: { title: 'Cumulative Probability', gridcolor: getTheme() === 'dark' ? '#30363d' : '#e8e8e8', zeroline: false, range: [0, 1.02] },
    legend: { orientation: 'h', y: -0.22 },
  }), { responsive: true, displayModeBar: false });
});
{% endif %}

/* --- Chart 4: Time-Series --- */
{% if has_time_series %}
plotFunctions.push(function() {
  var traces = [];
  var colorIdx = 0;
  {% for target in result.targets %}
  {% for step in target.steps %}
  {% if step.time_series %}
  // Throughput trace (primary y-axis)
  traces.push({
    x: [{% for tw in step.time_series %}{{ tw.second }},{% endfor %}],
    y: [{% for tw in step.time_series %}{{ tw.ops }},{% endfor %}],
    name: '{{ target.stack_id }}/{{ step.name }} — ops/s',
    type: 'scatter',
    mode: 'lines+markers',
    line: { width: 2, color: OI[colorIdx % OI.length] },
    marker: { size: 4 },
    yaxis: 'y',
  });
  // p95 latency trace (secondary y-axis)
  traces.push({
    x: [{% for tw in step.time_series %}{{ tw.second }},{% endfor %}],
    y: [{% for tw in step.time_series %}{{ (tw.p95_ns / 1000000)|round(4) }},{% endfor %}],
    name: '{{ target.stack_id }}/{{ step.name }} — p95 (ms)',
    type: 'scatter',
    mode: 'lines+markers',
    line: { width: 1.5, dash: 'dot', color: OI[(colorIdx + 1) % OI.length] },
    marker: { size: 3 },
    yaxis: 'y2',
  });
  {% endif %}
  colorIdx++;
  {% endfor %}
  {% endfor %}
  Plotly.newPlot('timeseries-chart', traces, baseLayout({
    xaxis: { title: 'Time (s)', gridcolor: getTheme() === 'dark' ? '#30363d' : '#e8e8e8', zeroline: false },
    yaxis: { title: 'Throughput (ops/s)', gridcolor: getTheme() === 'dark' ? '#30363d' : '#e8e8e8', zeroline: false },
    yaxis2: { title: 'p95 Latency (ms)', overlaying: 'y', side: 'right', gridcolor: 'transparent', zeroline: false },
    legend: { orientation: 'h', y: -0.22, font: { size: 11 } },
  }), { responsive: true, displayModeBar: false });
});
{% endif %}

/* --- Initial plot --- */
replotAll();
</script>

</body>
</html>
""")


def generate_html_report(result: RunResult) -> str:
    """Generate a publication-quality HTML report from benchmark results.

    The report is a single self-contained HTML file with:
    - Paper theme (default) with optional dark mode toggle
    - Booktabs-style tables, ECDF charts, CI error bars, time-series
    - Okabe-Ito colorblind-safe palette

    Args:
        result: A RunResult containing benchmark data.

    Returns:
        Complete HTML string ready for writing to a file.
    """
    has_samples = any(bool(step.samples_ns) for target in result.targets for step in target.steps)
    has_time_series = any(
        bool(step.time_series) for target in result.targets for step in target.steps
    )

    return REPORT_TEMPLATE.render(
        result=result,
        has_samples=has_samples,
        has_time_series=has_time_series,
        palette=list(OKABE_ITO),
        enumerate=enumerate,
    )
