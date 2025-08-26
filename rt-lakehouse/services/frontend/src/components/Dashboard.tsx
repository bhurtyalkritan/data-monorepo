import React, { useEffect, useMemo, useState } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  Legend,
} from 'recharts';

// Types
interface MetricsResponse {
  latest_kpis: Record<string, any>;
  recent_events: Record<string, number>;
}

interface QueryResponse {
  sql: string;
  results: Array<Record<string, any>>;
  explanation: string;
  chart_config?: {
    type: 'line' | 'bar' | 'scatter';
    x: string;
    y: string;
    title?: string;
  } | null;
}

const API_URL = (process.env.REACT_APP_API_URL as string) || 'http://localhost:8000';

const cardStyle: React.CSSProperties = {
  border: '1px solid #e5e7eb',
  borderRadius: 8,
  padding: 16,
  background: '#fff',
  boxShadow: '0 1px 2px rgba(0,0,0,0.05)'
};

const sectionTitle: React.CSSProperties = {
  fontSize: 16,
  fontWeight: 600,
  marginBottom: 8,
};

const Dashboard: React.FC = () => {
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const [loadingMetrics, setLoadingMetrics] = useState(false);

  const [question, setQuestion] = useState('Show conversion rate over time');
  const [queryData, setQueryData] = useState<QueryResponse | null>(null);
  const [loadingQuery, setLoadingQuery] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // New: trend states
  const [conversionTrend, setConversionTrend] = useState<Array<Record<string, any>>>([]);
  const [revenueTrend, setRevenueTrend] = useState<Array<Record<string, any>>>([]);

  // Poll metrics every 5s
  useEffect(() => {
    let mounted = true;

    const fetchMetrics = async () => {
      try {
        setLoadingMetrics(true);
        const res = await fetch(`${API_URL}/metrics`);
        if (!res.ok) throw new Error(await res.text());
        const data: MetricsResponse = await res.json();
        if (mounted) setMetrics(data);
      } catch (e: any) {
        if (mounted) setError(e?.message ?? 'Failed to load metrics');
      } finally {
        if (mounted) setLoadingMetrics(false);
      }
    };

    fetchMetrics();
    const id = setInterval(fetchMetrics, 5000);
    return () => {
      mounted = false;
      clearInterval(id);
    };
  }, []);

  const runQuery = async () => {
    try {
      setLoadingQuery(true);
      setError(null);
      const res = await fetch(`${API_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, include_chart: true }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data: QueryResponse = await res.json();
      setQueryData(data);
    } catch (e: any) {
      setError(e?.message ?? 'Query failed');
    } finally {
      setLoadingQuery(false);
    }
  };

  // Prepare recent events bar data
  const eventBarData = useMemo(() => {
    if (!metrics?.recent_events) return [] as Array<{ name: string; count: number }>;
    return Object.entries(metrics.recent_events).map(([name, count]) => ({ name, count }));
  }, [metrics]);

  // New: fetch non-AI conversion & revenue trends every 10s
  useEffect(() => {
    let mounted = true;
    const fetchTrends = async () => {
      try {
        const [cRes, rRes] = await Promise.all([
          fetch(`${API_URL}/trend/conversion?limit=60`),
          fetch(`${API_URL}/trend/revenue?limit=60`),
        ]);
        if (cRes.ok) {
          const cData = await cRes.json();
          if (mounted) setConversionTrend(Array.isArray(cData?.results) ? cData.results : []);
        }
        if (rRes.ok) {
          const rData = await rRes.json();
          if (mounted) setRevenueTrend(Array.isArray(rData?.results) ? rData.results : []);
        }
      } catch (_) {
        // ignore
      }
    };
    fetchTrends();
    const id = setInterval(fetchTrends, 10000);
    return () => { mounted = false; clearInterval(id); };
  }, []);

  // Prepare conversion trend data for chart: prefer query results, fallback to non-AI trend
  const conversionSeries = useMemo(() => {
    const arr = (queryData?.results?.length ? queryData.results : conversionTrend) || [];
    return [...arr].reverse(); // chronological
  }, [queryData, conversionTrend]);

  // Prepare revenue trend
  const revenueSeries = useMemo(() => {
    const arr = revenueTrend || [];
    return [...arr].reverse();
  }, [revenueTrend]);

  return (
    <div style={{ padding: 20, fontFamily: 'Inter, system-ui, Arial, sans-serif', background: '#f6f7f9', minHeight: '100vh' }}>
      <header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>RT-Lakehouse Dashboard</h1>
        <div style={{ color: '#6b7280' }}>Backend: {API_URL}</div>
      </header>

      {/* KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12, marginBottom: 16 }}>
        <div style={cardStyle}>
          <div style={sectionTitle}>Orders (1m)</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>
            {metrics?.latest_kpis?.orders ?? (loadingMetrics ? '…' : 0)}
          </div>
        </div>
        <div style={cardStyle}>
          <div style={sectionTitle}>GMV (1m)</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>
            {metrics?.latest_kpis?.gmv?.toFixed ? `$${metrics.latest_kpis.gmv.toFixed(2)}` : (loadingMetrics ? '…' : '$0.00')}
          </div>
        </div>
        <div style={cardStyle}>
          <div style={sectionTitle}>Conversion Rate</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>
            {metrics?.latest_kpis?.conversion_rate != null ? `${(metrics.latest_kpis.conversion_rate * 100).toFixed(2)}%` : (loadingMetrics ? '…' : '0.00%')}
          </div>
        </div>
        <div style={cardStyle}>
          <div style={sectionTitle}>Active Users (1m)</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>
            {metrics?.latest_kpis?.active_users ?? (loadingMetrics ? '…' : 0)}
          </div>
        </div>
      </div>

      {/* Charts Row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
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
                <Bar dataKey="count" fill="#6366f1" name="Count" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div style={cardStyle}>
          <div style={sectionTitle}>Conversion Rate Trend</div>
          <div style={{ height: 260 }}>
            {conversionSeries.length === 0 ? (
              <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>
                No conversion rate data available
              </div>
            ) : (
              <ResponsiveContainer>
                <LineChart data={conversionSeries}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey={(queryData?.chart_config?.x) || 'window_start'} tickFormatter={(v) => String(v).slice(11, 19)} />
                  <YAxis domain={[0, 'auto']} />
                  <Tooltip />
                  <Line type="monotone" dataKey={(queryData?.chart_config?.y) || 'conversion_rate'} stroke="#10b981" dot={false} name="Conversion" />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      </div>

      {/* Revenue Trend Row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 12, marginBottom: 16 }}>
        <div style={cardStyle}>
          <div style={sectionTitle}>Revenue Trend</div>
          <div style={{ height: 260 }}>
            {revenueSeries.length === 0 ? (
              <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>
                No revenue data available
              </div>
            ) : (
              <ResponsiveContainer>
                <LineChart data={revenueSeries}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="window_start" tickFormatter={(v) => String(v).slice(11, 19)} />
                  <YAxis domain={[0, 'auto']} />
                  <Tooltip />
                  <Line type="monotone" dataKey="gmv" stroke="#6366f1" dot={false} name="GMV" />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      </div>

      {/* Query Box */}
      <div style={cardStyle}>
        <div style={{ ...sectionTitle, marginBottom: 12 }}>Ask a question</div>
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <input
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="e.g., Show conversion rate over time"
            style={{ flex: 1, padding: '10px 12px', border: '1px solid #e5e7eb', borderRadius: 6 }}
          />
          <button onClick={runQuery} disabled={loadingQuery} style={{ padding: '10px 14px', background: '#111827', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
            {loadingQuery ? 'Running…' : 'Run'}
          </button>
        </div>

        {error && (
          <div style={{ color: '#b91c1c', marginBottom: 8 }}>Error: {error}</div>
        )}

        {queryData && (
          <div>
            <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 8 }}>SQL: {queryData.sql}</div>
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