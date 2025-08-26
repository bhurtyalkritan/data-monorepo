import React, { useEffect, useMemo, useRef, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Legend } from 'recharts';

// Types
interface MetricsResponse { latest_kpis: Record<string, any>; recent_events: Record<string, number>; }
interface QueryResponse { sql: string; results: Array<Record<string, any>>; explanation: string; plan?: string; chart_config?: { type: 'line' | 'bar' | 'scatter'; x: string; y: string; title?: string } | null; }

const API_URL = (process.env.REACT_APP_API_URL as string) || 'http://localhost:8000';

const cardStyle: React.CSSProperties = { border: '1px solid #e5e7eb', borderRadius: 8, padding: 16, background: '#fff', boxShadow: '0 1px 2px rgba(0,0,0,0.05)' };
const sectionTitle: React.CSSProperties = { fontSize: 16, fontWeight: 600, marginBottom: 8 };

const Metric: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, padding: 12, background: '#fff' }}>
    <div style={{ fontSize: 12, color: '#6b7280' }}>{label}</div>
    <div style={{ fontSize: 20, fontWeight: 700 }}>{value}</div>
  </div>
);

const Dashboard: React.FC = () => {
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const [dq, setDq] = useState<any>(null);
  const [health, setHealth] = useState<any>(null);
  const [alerts, setAlerts] = useState<any>(null);
  const [lineage, setLineage] = useState<{nodes:any[];edges:any[]}>({nodes:[],edges:[]});
  const [envDark, setEnvDark] = useState(false);

  const [question, setQuestion] = useState('Show conversion rate over time');
  const [queryData, setQueryData] = useState<QueryResponse | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Time travel
  const [asOf, setAsOf] = useState<string>('');
  const [conversionTrend, setConversionTrend] = useState<any[]>([]);
  const [revenueTrend, setRevenueTrend] = useState<any[]>([]);

  // SSE
  const sseRef = useRef<EventSource | null>(null);

  // Health + lineage
  useEffect(() => {
    const fetchHL = async () => {
      try {
        const [h, l] = await Promise.all([
          fetch(`${API_URL}/healthz`).then(r => r.json()),
          fetch(`${API_URL}/lineage/graph`).then(r => r.json()),
        ]);
        setHealth(h); setLineage(l);
      } catch {}
    };
    fetchHL();
    const id = setInterval(fetchHL, 15000);
    return () => clearInterval(id);
  }, []);

  // DQ + alerts
  useEffect(() => {
    const run = async () => {
      try {
        const [d, a] = await Promise.all([
          fetch(`${API_URL}/dq/status`).then(r => r.json()),
          fetch(`${API_URL}/alerts/state`).then(r => r.json()),
        ]);
        setDq(d); setAlerts(a);
      } catch {}
    };
    run();
    const id = setInterval(run, 10000);
    return () => clearInterval(id);
  }, []);

  // SSE metrics with fallback
  useEffect(() => {
    try {
      const es = new EventSource(`${API_URL}/sse/metrics`);
      sseRef.current = es;
      es.onmessage = (ev) => { try { setMetrics(JSON.parse(ev.data)); } catch {} };
      es.onerror = () => { es.close(); sseRef.current = null; };
      return () => { es.close(); };
    } catch {
      let mounted = true;
      const poll = async () => {
        try { const d = await fetch(`${API_URL}/metrics`).then(r => r.json()); if (mounted) setMetrics(d); } catch {}
      };
      poll();
      const id = setInterval(poll, 5000);
      return () => { mounted = false; clearInterval(id); };
    }
  }, []);

  // Trends (time-travel aware)
  useEffect(() => {
    const fetchTrends = async () => {
      const q = asOf ? `?limit=60&as_of=${encodeURIComponent(asOf)}` : `?limit=60`;
      try {
        const [c, r] = await Promise.all([
          fetch(`${API_URL}/trend/conversion${q}`).then(r => r.json()),
          fetch(`${API_URL}/trend/revenue${q}`).then(r => r.json()),
        ]);
        setConversionTrend(Array.isArray(c?.results) ? [...c.results].reverse() : []);
        setRevenueTrend(Array.isArray(r?.results) ? [...r.results].reverse() : []);
      } catch {}
    };
    fetchTrends();
  }, [asOf]);

  // Query executor
  const runQuery = async () => {
    try {
      setRunning(true); setError(null);
      const res = await fetch(`${API_URL}/query`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, include_chart: true })
      });
      if (res.status === 304) return;
      if (!res.ok) throw new Error(await res.text());
      const data: QueryResponse = await res.json();
      setQueryData(data);
    } catch (e: any) {
      setError(e?.message ?? 'Query failed');
    } finally { setRunning(false); }
  };

  // Derived data
  const eventBarData = useMemo(() => {
    if (!metrics?.recent_events) return [] as Array<{ name: string; count: number }>;
    return Object.entries(metrics.recent_events).map(([name, count]) => ({ name, count }));
  }, [metrics]);

  const conversionSeries = useMemo(() => (queryData?.results?.length ? queryData.results : conversionTrend) || [], [queryData, conversionTrend]);
  const revenueSeries = useMemo(() => revenueTrend || [], [revenueTrend]);

  const copySQL = () => { if (queryData?.sql) navigator.clipboard.writeText(queryData.sql); };

  // Simple lineage visual (no extra deps): render edges as pill rows
  const LineageList: React.FC = () => (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
      {(lineage.edges || []).map((e: any, i: number) => (
        <div key={i} style={{ border: '1px solid #e5e7eb', borderRadius: 9999, padding: '6px 10px', background: '#fff', fontSize: 12 }}>
          {e.from} → {e.to}
        </div>
      ))}
    </div>
  );

  return (
    <div style={{ padding: 20, fontFamily: 'Inter, system-ui, Arial, sans-serif', background: envDark ? '#0b1220' : '#f6f7f9', minHeight: '100vh', color: envDark ? '#e5e7eb' : '#111827' }}>
      <header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>RT‑Lakehouse Dashboard</h1>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <span style={{ color: '#6b7280' }}>Backend: {API_URL}</span>
          <button onClick={() => setEnvDark(v => !v)} style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #e5e7eb' }}>{envDark ? 'Light' : 'Dark'}</button>
        </div>
      </header>

      {/* Alert banner */}
      {alerts?.breach && (
        <div style={{ marginBottom: 12, padding: 12, borderRadius: 8, background: '#fef2f2', color: '#991b1b', border: '1px solid #fecaca' }}>
          Alert: Conversion rate below {(alerts.threshold*100).toFixed(1)}% for {alerts.window_minutes} minutes.
        </div>
      )}

      {/* Ops status */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12, marginBottom: 16 }}>
        <Metric label="DuckDB" value={health?.duckdb ? '✅' : '❌'} />
        <Metric label="Silver parts" value={health?.silver_parquet_parts ?? '—'} />
        <Metric label="Gold parts" value={health?.gold_parquet_parts ?? '—'} />
        <Metric label="Cache Hit/Miss" value={`${health?.cache_stats?.hits ?? 0} / ${health?.cache_stats?.misses ?? 0}`} />
      </div>

      {/* KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12, marginBottom: 16 }}>
        <div style={cardStyle}><div style={sectionTitle}>Orders (1m)</div><div style={{ fontSize: 24, fontWeight: 700 }}>{metrics?.latest_kpis?.orders ?? '—'}</div></div>
        <div style={cardStyle}><div style={sectionTitle}>GMV (1m)</div><div style={{ fontSize: 24, fontWeight: 700 }}>{metrics?.latest_kpis?.gmv?.toFixed ? `$${metrics.latest_kpis.gmv.toFixed(2)}` : '—'}</div></div>
        <div style={cardStyle}><div style={sectionTitle}>Conversion</div><div style={{ fontSize: 24, fontWeight: 700 }}>{metrics?.latest_kpis?.conversion_rate != null ? `${(metrics.latest_kpis.conversion_rate * 100).toFixed(2)}%` : '—'}</div></div>
        <div style={cardStyle}><div style={sectionTitle}>Active Users</div><div style={{ fontSize: 24, fontWeight: 700 }}>{metrics?.latest_kpis?.active_users ?? '—'}</div></div>
      </div>

      {/* Data Quality */}
      <div style={cardStyle}>
        <div style={sectionTitle}>Data Quality</div>
        <div style={{display:'grid',gridTemplateColumns:'repeat(3,1fr)',gap:12}}>
          <Metric label="Rows Quarantined (1h)" value={dq?.quarantined_1h ?? 0} />
          <Metric label="Null user_id (%)" value={`${Number(dq?.null_user_pct ?? 0).toFixed(2)}%`} />
          <Metric label="Late Events (1h)" value={dq?.late_1h ?? 0} />
        </div>
      </div>

      {/* Time Travel */}
      <div style={{ ...cardStyle, marginTop: 12 }}>
        <div style={sectionTitle}>Time Travel</div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <input type="datetime-local" value={asOf} onChange={(e) => setAsOf(e.target.value ? e.target.value + 'Z' : '')} />
          {asOf && <button onClick={() => setAsOf('')} style={{ padding: '8px 10px', border: '1px solid #e5e7eb', borderRadius: 6 }}>Clear</button>}
        </div>
      </div>

      {/* Charts */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, margin: '12px 0' }}>
        <div style={cardStyle}>
          <div style={sectionTitle}>Recent Events (last hour)</div>
          <div style={{ height: 260 }}>
            <ResponsiveContainer>
              <BarChart data={eventBarData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="count" name="Count" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div style={cardStyle}>
          <div style={sectionTitle}>Conversion Rate Trend</div>
          <div style={{ height: 260 }}>
            {conversionSeries.length === 0 ? (
              <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>No data</div>
            ) : (
              <ResponsiveContainer>
                <LineChart data={conversionSeries}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey={queryData?.chart_config?.x || 'window_start'} tickFormatter={(v: any) => String(v).slice(11, 19)} />
                  <YAxis domain={[0, 'auto']} />
                  <Tooltip />
                  <Line type="monotone" dataKey={queryData?.chart_config?.y || 'conversion_rate'} dot={false} name="Conversion" />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      </div>

      <div style={{ ...cardStyle, marginBottom: 12 }}>
        <div style={sectionTitle}>Revenue Trend</div>
        <div style={{ height: 260 }}>
          {revenueSeries.length === 0 ? (
            <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>No data</div>
          ) : (
            <ResponsiveContainer>
              <LineChart data={revenueSeries}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="window_start" tickFormatter={(v: any) => String(v).slice(11, 19)} />
                <YAxis domain={[0, 'auto']} />
                <Tooltip />
                <Line type="monotone" dataKey="gmv" dot={false} name="GMV" />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Lineage */}
      <div style={cardStyle}>
        <div style={sectionTitle}>Lineage Impact</div>
        <LineageList />
      </div>

      {/* Query Box */}
      <div style={{ ...cardStyle, marginTop: 12 }}>
        <div style={{ ...sectionTitle, marginBottom: 12 }}>Ask a question</div>
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <input value={question} onChange={(e) => setQuestion(e.target.value)} placeholder="e.g., Show conversion rate over time" style={{ flex: 1, padding: '10px 12px', border: '1px solid #e5e7eb', borderRadius: 6 }} />
          <button onClick={runQuery} disabled={running} style={{ padding: '10px 14px', background: '#111827', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>{running ? 'Running…' : 'Run'}</button>
        </div>
        {error && <div style={{ color: '#b91c1c', marginBottom: 8 }}>Error: {error}</div>}
        {queryData && (
          <div>
            <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 8 }}>SQL: {queryData.sql}</div>
            <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
              <button onClick={() => queryData?.sql && navigator.clipboard.writeText(queryData.sql)} style={{ padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6 }}>Copy SQL</button>
              <details style={{ flex: 1 }}>
                <summary>Explain Plan</summary>
                <pre style={{ whiteSpace: 'pre-wrap' }}>{queryData.plan || 'No plan'}</pre>
              </details>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr>
                    {queryData.results.length > 0 && Object.keys(queryData.results[0]).map((k) => (
                      <th key={k} style={{ textAlign: 'left', borderBottom: '1px solid #e5e7eb', padding: 8, fontSize: 12, color: '#374151' }}>{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {queryData.results.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((v, j) => (
                        <td key={j} style={{ padding: 8, borderBottom: '1px solid #f3f4f6', fontSize: 13 }}>{String(v)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default Dashboard;
