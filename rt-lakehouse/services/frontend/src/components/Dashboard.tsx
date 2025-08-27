import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  BarChart, Bar, Legend, ScatterChart, Scatter, ZAxis
} from 'recharts';
import {
  Box, Card, CardContent, Typography, Button, TextField, Select, MenuItem,
  FormControl, InputLabel, Paper, Chip, Alert, CircularProgress,
  AppBar, Toolbar, Switch, FormControlLabel, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Accordion, AccordionSummary, AccordionDetails,
  List, ListItem, ListItemText, Link, Divider, Container, Stack,
  ThemeProvider, createTheme, CssBaseline
} from '@mui/material';
import Grid from '@mui/material/Grid';
import {
  Dashboard as DashboardIcon, TrendingUp, Security, Storage, Speed,
  ExpandMore, ContentCopy, PlayArrow, Refresh, Timeline, Assessment,
  Warning, Analytics, DataUsage, Schedule
} from '@mui/icons-material';

// -----------------------------
// Types (API contracts)
// -----------------------------
interface MetricsResponse { latest_kpis: Record<string, any>; recent_events: Record<string, number>; }
interface QueryResponse { sql: string; results: Array<Record<string, any>>; explanation: string; plan?: string;
  chart_config?: { type: 'line'|'bar'|'scatter', x: string, y: string, title?: string } | null; }

type DQStatus = { null_user_pct: number; quarantined_1h: number; late_1h: number };
type Healthz = { duckdb: boolean; silver_parquet_parts: number; gold_parquet_parts: number;
  uptime_sec: number; cache_stats: {hits:number;misses:number}; rate_limit: any;
  tenant_enforce: boolean; tenant_column: string; };

type ForecastResp = { history: number[]; history_smoothed: number[]; forecast: number[]; horizon: number };
type SkewResp = { key: string; top: { key: string; freq: number }[] };
type LatencyResp = { silver_latest: string|null; gold_latest: string|null; lag_sec: number|null };
type AnalyzeResp = { sql: string; explain: string[]; count_estimate_plan: string[] };
type OptimizeAdvice = { gold_parts: number; advice: string[] };
type LineageGraph = { nodes: {id:string;label:string}[]; edges: {from:string;to:string}[] };
type WhatIfResp = { node: string; impacted: string[]; recommendation: string };
type AlertsState = { breach: boolean; threshold: number; window_minutes: number; samples: {window_start:string; conversion_rate:number}[] };
type AnomalyResp = { mean:number; std:number; z:number; anomalies: {window_start:string; rate:number; z:number}[] };
type MLFeaturesResp = { features: { user_id: any; purchase_count: number; total_spend: number; views: number }[] };
type WhoAmIResp = { tenant: string|null; tenant_enforce: boolean; jwt_enabled: boolean; tenant_column: string };
type RunbookLinks = { [k:string]: string };

const API_URL = (process.env.REACT_APP_API_URL as string) || 'http://localhost:8000';

// Custom theme
const createCustomTheme = (darkMode: boolean) =>
  createTheme({
    palette: {
      mode: darkMode ? 'dark' : 'light',
      primary: {
        main: '#1976d2',
        light: '#42a5f5',
        dark: '#1565c0',
      },
      secondary: {
        main: '#9c27b0',
        light: '#ba68c8',
        dark: '#7b1fa2',
      },
      success: { main: '#2e7d32' },
      warning: { main: '#ed6c02' },
      error: { main: '#d32f2f' },
      background: {
        default: darkMode ? '#0a1929' : '#f5f5f5',
        paper: darkMode ? '#1e293b' : '#ffffff',
      },
    },
    typography: {
      fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
      h4: { fontWeight: 600 },
      h6: { fontWeight: 600 },
    },
    components: {
      MuiCard: {
        styleOverrides: {
          root: {
            borderRadius: 12,
            boxShadow: darkMode
              ? '0 4px 6px -1px rgba(0, 0, 0, 0.3), 0 2px 4px -1px rgba(0, 0, 0, 0.2)'
              : '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
          },
        },
      },
      MuiButton: {
        styleOverrides: {
          root: {
            borderRadius: 8,
            textTransform: 'none',
            fontWeight: 500,
          },
        },
      },
    },
  });

// Professional metric card component
const MetricCard: React.FC<{
  title: string;
  value: React.ReactNode;
  icon?: React.ReactNode;
  color?: 'primary' | 'secondary' | 'success' | 'warning' | 'error';
  subtitle?: string;
}> = ({ title, value, icon, color = 'primary', subtitle }) => (
  <Card elevation={2}>
    <CardContent>
      <Stack direction="row" alignItems="center" justifyContent="space-between" mb={1}>
        <Typography variant="body2" color="text.secondary" fontWeight={500}>
          {title}
        </Typography>
        {icon && (
          <Box sx={{ color: `${color}.main`, opacity: 0.7 }}>
            {icon}
          </Box>
        )}
      </Stack>
      <Typography variant="h5" fontWeight={700} color={`${color}.main`}>
        {value}
      </Typography>
      {subtitle && (
        <Typography variant="caption" color="text.secondary">
          {subtitle}
        </Typography>
      )}
    </CardContent>
  </Card>
);

// Chart card wrapper
const ChartCard: React.FC<{ title: string; children: React.ReactNode; action?: React.ReactNode }> = ({
  title, children, action
}) => (
  <Card elevation={2} sx={{ height: '100%' }}>
    <CardContent>
      <Stack direction="row" alignItems="center" justifyContent="space-between" mb={2}>
        <Typography variant="h6" fontWeight={600}>
          {title}
        </Typography>
        {action}
      </Stack>
      <Box sx={{ height: 300 }}>
        {children}
      </Box>
    </CardContent>
  </Card>
);

const Dashboard: React.FC = () => {
  // All state variables remain unchanged
  const [envDark, setEnvDark] = useState(false);
  const [health, setHealth] = useState<Healthz | null>(null);
  const [whoami, setWhoami] = useState<WhoAmIResp | null>(null);
  const [runbook, setRunbook] = useState<RunbookLinks | null>(null);
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const sseRef = useRef<EventSource | null>(null);
  const [asOf, setAsOf] = useState<string>('');
  const [conversionTrend, setConversionTrend] = useState<any[]>([]);
  const [revenueTrend, setRevenueTrend] = useState<any[]>([]);
  const [dq, setDq] = useState<DQStatus | null>(null);
  const [alerts, setAlerts] = useState<AlertsState | null>(null);
  const [lineage, setLineage] = useState<LineageGraph>({nodes:[],edges:[]});
  const [whatIfNode, setWhatIfNode] = useState<string>('gold_kpis');
  const [whatIf, setWhatIf] = useState<WhatIfResp | null>(null);
  const [forecast, setForecast] = useState<ForecastResp | null>(null);
  const [skew, setSkew] = useState<SkewResp | null>(null);
  const [latency, setLatency] = useState<LatencyResp | null>(null);
  const [optimize, setOptimize] = useState<OptimizeAdvice | null>(null);
  const [anomaly, setAnomaly] = useState<AnomalyResp | null>(null);
  const [ml, setML] = useState<MLFeaturesResp | null>(null);
  const [question, setQuestion] = useState('Show conversion rate over time');
  const [queryData, setQueryData] = useState<QueryResponse | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sqlToAnalyze, setSqlToAnalyze] = useState<string>("SELECT window_start, gmv FROM read_parquet('/delta/gold_latest.parquet/*.parquet') ORDER BY window_start DESC LIMIT 100");
  const [analysis, setAnalysis] = useState<AnalyzeResp | null>(null);
  const [backfillMsg, setBackfillMsg] = useState<string>('');

  const theme = createCustomTheme(envDark);

  // All useEffect hooks remain unchanged
  useEffect(() => {
    const boot = async () => {
      try {
        const [h, l, w, rb] = await Promise.all([
          fetch(`${API_URL}/healthz`).then(r => r.json()),
          fetch(`${API_URL}/lineage/graph`).then(r => r.json()),
          fetch(`${API_URL}/security/whoami`).then(r => r.json()),
          fetch(`${API_URL}/runbook/links`).then(r => r.json()),
        ]);
        setHealth(h); setLineage(l); setWhoami(w); setRunbook(rb);
      } catch {}
    };
    boot();
    const id = setInterval(boot, 15000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const poll = async () => {
      try {
        const [d,a,o,lat,sk,mlf,an] = await Promise.all([
          fetch(`${API_URL}/dq/status`).then(r => r.json()),
          fetch(`${API_URL}/alerts/state`).then(r => r.json()),
          fetch(`${API_URL}/optimize/advice`).then(r => r.json()),
          fetch(`${API_URL}/latency`).then(r => r.json()),
          fetch(`${API_URL}/skew/check?key=user_id&topk=12`).then(r => r.json()),
          fetch(`${API_URL}/ml/features?limit=20`).then(r => r.json()),
          fetch(`${API_URL}/anomaly/conversion?limit=120&z=2.0`).then(r => r.json())
        ]);
        setDq(d); setAlerts(a); setOptimize(o); setLatency(lat); setSkew(sk); setML(mlf); setAnomaly(an);
      } catch {}
    };
    poll();
    const id = setInterval(poll, 10000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    let pollId: any = null;
    const startPoll = () => {
      if (pollId) return;
      const poll = async () => {
        try {
          const d = await fetch(`${API_URL}/metrics`, { cache: 'no-store' }).then(r => r.json());
          setMetrics(d);
        } catch {}
      };
      poll();
      pollId = setInterval(poll, 5000);
    };

    try {
      const es = new EventSource(`${API_URL}/sse/metrics`);
      sseRef.current = es;
      es.onmessage = (ev) => { try { setMetrics(JSON.parse(ev.data)); } catch {} };
      es.onerror = () => {
        es.close();
        sseRef.current = null;
        startPoll();
      };
      return () => {
        es.close();
        if (pollId) { clearInterval(pollId); }
      };
    } catch {
      startPoll();
      return () => { if (pollId) { clearInterval(pollId); } };
    }
  }, []);

  useEffect(() => {
    const fetchTimeSeries = async () => {
      const q = asOf ? `?limit=60&as_of=${encodeURIComponent(asOf)}` : `?limit=60`;
      try {
        const [c, r, f] = await Promise.all([
          fetch(`${API_URL}/trend/conversion${q}`).then(r => r.json()),
          fetch(`${API_URL}/trend/revenue${q}`).then(r => r.json()),
          fetch(`${API_URL}/forecast/conversion?limit=120&horizon=12&ma_window=7`).then(r => r.json()),
        ]);
        setConversionTrend(Array.isArray(c?.results) ? [...c.results].reverse() : []);
        setRevenueTrend(Array.isArray(r?.results) ? [...r.results].reverse() : []);
        setForecast(f);
      } catch {}
    };
    fetchTimeSeries();
  }, [asOf]);

  useEffect(() => {
    const run = async () => {
      try {
        const res = await fetch(`${API_URL}/lineage/whatif?node=${encodeURIComponent(whatIfNode)}`).then(r => r.json());
        setWhatIf(res);
      } catch {}
    };
    run();
  }, [whatIfNode]);

  // All functions remain unchanged
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

  const runAnalyze = async () => {
    try {
      const res = await fetch(`${API_URL}/analyze/sql`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: sqlToAnalyze })
      });
      if (!res.ok) throw new Error(await res.text());
      setAnalysis(await res.json());
    } catch (e:any) {
      setAnalysis(null);
      setError(e?.message ?? 'Analyze failed');
    }
  };

  const runBackfill = async () => {
    try {
      const res = await fetch(`${API_URL}/backfill/gold`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(data));
      setBackfillMsg(`Materialized ${data.materialized_rows} rows into ${data.table}`);
    } catch(e:any) {
      setBackfillMsg(`Backfill failed: ${e?.message ?? e}`);
    }
  };

  // All useMemo calculations remain unchanged
  const eventBarData = useMemo(() => {
    if (!metrics?.recent_events) return [] as Array<{ name: string; count: number }>;
    return Object.entries(metrics.recent_events).map(([name, count]) => ({ name, count }));
  }, [metrics]);

  const conversionSeries = useMemo(() => (queryData?.results?.length ? queryData.results : conversionTrend) || [], [queryData, conversionTrend]);
  const revenueSeries = useMemo(() => revenueTrend || [], [revenueTrend]);

  const forecastSeries = useMemo(() => {
    if (!forecast) return [];
    const h = forecast.history || [];
    const s = forecast.history_smoothed || [];
    const f = forecast.forecast || [];
    const merged: { idx:number; history?:number; smoothed?:number; forecast?:number }[] = [];
    let idx = 0;
    for (let i=0;i<h.length;i++) merged.push({ idx: idx++, history: h[i], smoothed: s[i] });
    for (let j=0;j<f.length;j++) merged.push({ idx: idx++, forecast: f[j] });
    return merged;
  }, [forecast]);

  const skewData = skew?.top?.map(item => ({ key: String(item.key), freq: item.freq })) ?? [];
  const mlSpendTop = ml?.features?.map(f => ({ user_id: String(f.user_id), total_spend: f.total_spend })) ?? [];

  const lagText = latency?.lag_sec == null ? '—' : `${latency.lag_sec.toFixed(1)}s`;
  const lagColor: 'success' | 'warning' | 'error' | 'primary' =
    latency?.lag_sec == null ? 'primary' :
    latency.lag_sec < 10 ? 'success' :
    latency.lag_sec < 60 ? 'warning' : 'error';

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
        {/* App Bar */}
        <AppBar position="static" elevation={1}>
          <Toolbar>
            <DashboardIcon sx={{ mr: 2 }} />
            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
              RT-Lakehouse Dashboard
            </Typography>
            <Typography variant="body2" sx={{ mr: 2, opacity: 0.8 }}>
              Backend: {API_URL}
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={envDark}
                  onChange={(e) => setEnvDark(e.target.checked)}
                  color="secondary"
                />
              }
              label="Dark Mode"
            />
          </Toolbar>
        </AppBar>

        <Container maxWidth="xl" sx={{ py: 3 }}>
          {/* Global Alert */}
          {alerts?.breach && (
            <Alert severity="error" icon={<Warning />} sx={{ mb: 3 }}>
              <Typography fontWeight={600}>
                Conversion Alert: Below {(alerts.threshold*100).toFixed(1)}% for {alerts.window_minutes} minutes
              </Typography>
            </Alert>
          )}

          {/* System Health Metrics */}
          <Typography variant="h5" gutterBottom sx={{ mb: 2, fontWeight: 600 }}>
            System Health
          </Typography>
          <Grid container spacing={2} sx={{ mb: 4 }}>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="DuckDB Status"
                value={health?.duckdb ? 'Online' : 'Offline'}
                icon={<Storage />}
                color={health?.duckdb ? 'success' : 'error'}
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Silver Parts"
                value={health?.silver_parquet_parts ?? '—'}
                icon={<DataUsage />}
                color="primary"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Gold Parts"
                value={health?.gold_parquet_parts ?? '—'}
                icon={<DataUsage />}
                color="secondary"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Cache Hit Rate"
                value={`${((health?.cache_stats?.hits || 0) / Math.max(1, (health?.cache_stats?.hits || 0) + (health?.cache_stats?.misses || 0)) * 100).toFixed(1)}%`}
                icon={<Speed />}
                color="primary"
                subtitle={`${health?.cache_stats?.hits ?? 0} hits / ${health?.cache_stats?.misses ?? 0} misses`}
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Pipeline Latency"
                value={lagText}
                icon={<Schedule />}
                color={lagColor}
                subtitle="Silver → Gold"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Tenant"
                value={whoami?.tenant ?? 'None'}
                icon={<Security />}
                color="primary"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Tenant Enforcement"
                value={whoami?.tenant_enforce ? 'Enabled' : 'Disabled'}
                icon={<Security />}
                color={whoami?.tenant_enforce ? 'success' : 'warning'}
              />
            </Grid>
          </Grid>

          {/* Business KPIs */}
          <Typography variant="h5" gutterBottom sx={{ mb: 2, fontWeight: 600 }}>
            Business Metrics (1min window)
          </Typography>
          <Grid container spacing={2} sx={{ mb: 4 }}>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Orders"
                value={metrics?.latest_kpis?.orders ?? '—'}
                icon={<TrendingUp />}
                color="primary"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="GMV"
                value={metrics?.latest_kpis?.gmv?.toFixed ? `${metrics.latest_kpis.gmv.toFixed(2)}` : '—'}
                icon={<Assessment />}
                color="success"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Conversion Rate"
                value={metrics?.latest_kpis?.conversion_rate != null ? `${(metrics.latest_kpis.conversion_rate * 100).toFixed(2)}%` : '—'}
                icon={<Timeline />}
                color="secondary"
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }}>
              <MetricCard
                title="Active Users"
                value={metrics?.latest_kpis?.active_users ?? '—'}
                icon={<Analytics />}
                color="primary"
              />
            </Grid>
          </Grid>

          {/* Data Quality */}
          <Card elevation={2} sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom fontWeight={600}>
                Data Quality (Last Hour)
              </Typography>
              <Grid container spacing={2}>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <MetricCard
                    title="Quarantined Rows"
                    value={dq?.quarantined_1h ?? 0}
                    color={dq?.quarantined_1h ? 'warning' : 'success'}
                  />
                </Grid>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <MetricCard
                    title="Null user_id (%)"
                    value={`${Number(dq?.null_user_pct ?? 0).toFixed(2)}%`}
                    color={Number(dq?.null_user_pct ?? 0) > 5 ? 'warning' : 'success'}
                  />
                </Grid>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <MetricCard
                    title="Late Events"
                    value={dq?.late_1h ?? 0}
                    color={dq?.late_1h ? 'warning' : 'success'}
                  />
                </Grid>
              </Grid>
            </CardContent>
          </Card>

          {/* Time Travel */}
          <Card elevation={2} sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom fontWeight={600}>
                Time Travel
              </Typography>
              <Stack direction="row" spacing={2} alignItems="center">
                <TextField
                  type="datetime-local"
                  value={asOf ? asOf.slice(0, -1) : ''}
                  onChange={(e) => setAsOf(e.target.value ? e.target.value + 'Z' : '')}
                  size="small"
                  InputLabelProps={{ shrink: true }}
                />
                {asOf && (
                  <Button
                    variant="outlined"
                    onClick={() => setAsOf('')}
                    startIcon={<Refresh />}
                    size="small"
                  >
                    Clear
                  </Button>
                )}
              </Stack>
            </CardContent>
          </Card>

          {/* Charts Section */}
          <Typography variant="h5" gutterBottom sx={{ mb: 2, fontWeight: 600 }}>
            Analytics & Trends
          </Typography>

          <Grid container spacing={3} sx={{ mb: 4 }}>
            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title="Recent Events (Last Hour)">
                <ResponsiveContainer>
                  <BarChart data={eventBarData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="count" fill={theme.palette.primary.main} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </Grid>
            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title="Conversion Rate Trend">
                {conversionSeries.length === 0 ? (
                  <Box display="flex" alignItems="center" justifyContent="center" height="100%">
                    <Typography color="text.secondary">No data available</Typography>
                  </Box>
                ) : (
                  <ResponsiveContainer>
                    <LineChart data={conversionSeries}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey={queryData?.chart_config?.x || 'window_start'} tickFormatter={(v: any) => String(v).slice(11, 19)} />
                      <YAxis domain={[0, 'auto']} />
                      <Tooltip />
                      <Line type="monotone" dataKey={queryData?.chart_config?.y || 'conversion_rate'} dot={false} stroke={theme.palette.secondary.main} strokeWidth={2} />
                    </LineChart>
                  </ResponsiveContainer>
                )}
              </ChartCard>
            </Grid>

            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title="Revenue Trend">
                <ResponsiveContainer>
                  <LineChart data={revenueSeries}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="window_start" tickFormatter={(v: any) => String(v).slice(11, 19)} />
                    <YAxis domain={[0, 'auto']} />
                    <Tooltip />
                    <Line type="monotone" dataKey="gmv" dot={false} stroke={theme.palette.success.main} strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              </ChartCard>
            </Grid>

            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title="Conversion Forecast">
                <ResponsiveContainer>
                  <LineChart data={forecastSeries}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="idx" />
                    <YAxis domain={[0, 'auto']} />
                    <Tooltip />
                    <Line type="monotone" dataKey="history" dot={false} stroke={theme.palette.primary.main} strokeWidth={2} />
                    <Line type="monotone" dataKey="smoothed" dot={false} stroke={theme.palette.secondary.main} strokeDasharray="4 2" strokeWidth={2} />
                    <Line type="monotone" dataKey="forecast" dot={false} stroke={theme.palette.warning.main} strokeDasharray="2 4" strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              </ChartCard>
            </Grid>

            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title="Key Skew Analysis">
                <ResponsiveContainer>
                  <BarChart data={skewData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="key" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="freq" fill={theme.palette.warning.main} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </Grid>

            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title="ML Features - Top Spenders">
                <ResponsiveContainer>
                  <BarChart data={mlSpendTop}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="user_id" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="total_spend" fill={theme.palette.success.main} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </Grid>
          </Grid>

          <Grid container spacing={3} sx={{ mb: 4 }}>
            <Grid size={{ xs: 12, lg: 6 }}>
              <ChartCard title={`Anomaly Detection (z-score > ${anomaly?.z ?? 2})`}>
                <ResponsiveContainer>
                  <ScatterChart>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="x" name="t" />
                    <YAxis dataKey="y" name="rate" />
                    <ZAxis range={[60, 60]} />
                    <Tooltip />
                    <Scatter
                      name="Normal"
                      data={(anomaly?.anomalies?.length ? [] : [{x:0,y:anomaly?.mean??0}])}
                      fill={theme.palette.primary.main}
                    />
                    <Scatter
                      name="Anomaly"
                      data={(anomaly?.anomalies ?? []).map((a, i) => ({ x: i, y: a.rate }))}
                      fill={theme.palette.error.main}
                    />
                  </ScatterChart>
                </ResponsiveContainer>
                <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                  μ={anomaly?.mean?.toFixed?.(4)} σ={anomaly?.std?.toFixed?.(4)} — {anomaly?.anomalies?.length ?? 0} anomalies
                </Typography>
              </ChartCard>
            </Grid>

            <Grid size={{ xs: 12, lg: 6 }}>
              <Card elevation={2} sx={{ height: '100%' }}>
                <CardContent>
                  <Typography variant="h6" fontWeight={600} gutterBottom>
                    Optimizer Advice
                  </Typography>
                  <MetricCard
                    title="Gold Parts"
                    value={optimize?.gold_parts ?? '—'}
                    color="primary"
                  />
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                      Recommendations:
                    </Typography>
                    <List dense>
                      {(optimize?.advice ?? []).map((advice, i) => (
                        <ListItem key={i} sx={{ py: 0.5 }}>
                          <ListItemText primary={advice} />
                        </ListItem>
                      ))}
                    </List>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          </Grid>

          {/* Lineage & What-If Analysis */}
          <Card elevation={2} sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" fontWeight={600} gutterBottom>
                Data Lineage & Impact Analysis
              </Typography>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                  Lineage Flow:
                </Typography>
                <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                  {(lineage.edges || []).map((e, i) => (
                    <Chip
                      key={i}
                      label={`${e.from} → ${e.to}`}
                      variant="outlined"
                      size="small"
                    />
                  ))}
                </Stack>
              </Box>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                  What-If Analysis:
                </Typography>
                <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap">
                  <FormControl size="small" sx={{ minWidth: 200 }}>
                    <InputLabel>Select Node</InputLabel>
                    <Select
                      value={whatIfNode}
                      onChange={(e) => setWhatIfNode(e.target.value)}
                      label="Select Node"
                    >
                      {(lineage.nodes || []).map(n => (
                        <MenuItem key={n.id} value={n.id}>{n.id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>

                  <Box>
                    <Typography variant="body2" color="text.secondary" gutterBottom>
                      Impacted Nodes:
                    </Typography>
                    <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                      {whatIf?.impacted?.length ?
                        whatIf.impacted.map((n, i) => (
                          <Chip key={i} label={n} color="warning" size="small" />
                        )) :
                        <Typography variant="body2" color="text.secondary">No selection</Typography>
                      }
                    </Stack>
                  </Box>
                </Stack>

                {whatIf?.recommendation && (
                  <Alert severity="info" sx={{ mt: 2 }}>
                    {whatIf.recommendation}
                  </Alert>
                )}
              </Box>
            </CardContent>
          </Card>

          {/* Natural Language Query Interface */}
          <Card elevation={2} sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" fontWeight={600} gutterBottom>
                Natural Language Query Interface
              </Typography>

              <Stack direction="row" spacing={2} sx={{ mb: 2 }}>
                <TextField
                  fullWidth
                  value={question}
                  onChange={(e) => setQuestion(e.target.value)}
                  placeholder="e.g., Show conversion rate over time"
                  variant="outlined"
                  size="small"
                />
                <Button
                  variant="contained"
                  onClick={runQuery}
                  disabled={running}
                  startIcon={running ? <CircularProgress size={16} /> : <PlayArrow />}
                  sx={{ minWidth: 120 }}
                >
                  {running ? 'Running...' : 'Execute'}
                </Button>
              </Stack>

              {error && (
                <Alert severity="error" sx={{ mb: 2 }}>
                  {error}
                </Alert>
              )}

              {queryData && (
                <Box>
                  <Box sx={{ mb: 2 }}>
                    <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                      Generated SQL:
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50' }}>
                      <Typography variant="body2" fontFamily="monospace">
                        {queryData.sql}
                      </Typography>
                    </Paper>
                  </Box>

                  <Stack direction="row" spacing={2} sx={{ mb: 2 }}>
                    <Button
                      variant="outlined"
                      startIcon={<ContentCopy />}
                      onClick={() => queryData?.sql && navigator.clipboard.writeText(queryData.sql)}
                      size="small"
                    >
                      Copy SQL
                    </Button>

                    <Accordion sx={{ flex: 1 }}>
                      <AccordionSummary expandIcon={<ExpandMore />}>
                        <Typography variant="body2">Execution Plan</Typography>
                      </AccordionSummary>
                      <AccordionDetails>
                        <Paper variant="outlined" sx={{ p: 2 }}>
                          <Typography variant="body2" fontFamily="monospace" whiteSpace="pre-wrap">
                            {queryData.plan || 'No plan available'}
                          </Typography>
                        </Paper>
                      </AccordionDetails>
                    </Accordion>
                  </Stack>

                  <TableContainer component={Paper} variant="outlined">
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          {queryData.results.length > 0 && Object.keys(queryData.results[0]).map((k) => (
                            <TableCell key={k} sx={{ fontWeight: 600 }}>
                              {k}
                            </TableCell>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {queryData.results.map((row, i) => (
                          <TableRow key={i} hover>
                            {Object.values(row).map((v, j) => (
                              <TableCell key={j}>
                                {String(v)}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </Box>
              )}
            </CardContent>
          </Card>

          {/* Operations Panel */}
          <Grid container spacing={3}>
            <Grid size={{ xs: 12, lg: 8 }}>
              <Card elevation={2}>
                <CardContent>
                  <Typography variant="h6" fontWeight={600} gutterBottom>
                    SQL Analysis & Performance
                  </Typography>

                  <Stack spacing={2}>
                    <TextField
                      multiline
                      rows={4}
                      fullWidth
                      value={sqlToAnalyze}
                      onChange={(e) => setSqlToAnalyze(e.target.value)}
                      variant="outlined"
                      placeholder="Enter SQL to analyze..."
                    />

                    <Button
                      variant="contained"
                      onClick={runAnalyze}
                      startIcon={<Assessment />}
                    >
                      Analyze SQL
                    </Button>

                    {analysis && (
                      <Grid container spacing={2}>
                        <Grid size={{ xs: 12, md: 6 }}>
                          <Paper variant="outlined" sx={{ p: 2 }}>
                            <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                              Query Plan
                            </Typography>
                            <Typography variant="body2" fontFamily="monospace" whiteSpace="pre-wrap">
                              {analysis.explain.join('\n')}
                            </Typography>
                          </Paper>
                        </Grid>
                        <Grid size={{ xs: 12, md: 6 }}>
                          <Paper variant="outlined" sx={{ p: 2 }}>
                            <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                              Cost Estimation
                            </Typography>
                            <Typography variant="body2" fontFamily="monospace" whiteSpace="pre-wrap">
                              {analysis.count_estimate_plan.join('\n')}
                            </Typography>
                          </Paper>
                        </Grid>
                      </Grid>
                    )}
                  </Stack>
                </CardContent>
              </Card>
            </Grid>

            <Grid size={{ xs: 12, lg: 4 }}>
              <Card elevation={2}>
                <CardContent>
                  <Typography variant="h6" fontWeight={600} gutterBottom>
                    Operations
                  </Typography>

                  <Stack spacing={2}>
                    <Button
                      variant="outlined"
                      onClick={runBackfill}
                      startIcon={<Refresh />}
                      fullWidth
                    >
                      Backfill Gold Table
                    </Button>

                    {backfillMsg && (
                      <Alert severity={backfillMsg.includes('failed') ? 'error' : 'success'}>
                        {backfillMsg}
                      </Alert>
                    )}

                    <Divider />

                    <Typography variant="subtitle2" fontWeight={600}>
                      Runbook Links
                    </Typography>

                    <List dense>
                      {runbook && Object.entries(runbook).map(([k, v]) => (
                        <ListItem key={k} sx={{ px: 0 }}>
                          <Link href={v} target="_blank" rel="noopener noreferrer">
                            {k}
                          </Link>
                        </ListItem>
                      ))}
                    </List>
                  </Stack>
                </CardContent>
              </Card>
            </Grid>
          </Grid>
        </Container>
      </Box>
    </ThemeProvider>
  );
};

export default Dashboard;
