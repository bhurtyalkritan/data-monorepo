# Frontend Dashboard Documentation

## Overview

The React frontend (`services/frontend/`) provides a modern, responsive dashboard for real-time business analytics visualization. Built with React 18, TypeScript, and Material-UI v7, it offers live KPI monitoring and trend analysis.

## Technology Stack

### Core Framework
```json
{
  "react": "^18.2.0",
  "typescript": "^5.3.3", 
  "@types/react": "^18.2.45",
  "@types/react-dom": "^18.2.18"
}
```

### UI Library (Material-UI v7)
```json
{
  "@mui/material": "^6.1.6",
  "@mui/icons-material": "^6.1.6",
  "@emotion/react": "^11.11.4",
  "@emotion/styled": "^11.11.5"
}
```

### Data Visualization
```json
{
  "recharts": "^2.8.0"
}
```

### Styling & Utilities  
```json
{
  "tailwindcss": "^3.3.6",
  "@tailwindcss/typography": "^0.5.10",
  "autoprefixer": "^10.4.16"
}
```

## Component Architecture

### Main Dashboard (`src/components/Dashboard.tsx`)

#### Layout Structure
```tsx
import { Grid, Card, CardContent, Typography, Box } from '@mui/material';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const Dashboard: React.FC = () => {
  return (
    <Box sx={{ flexGrow: 1, p: 3 }}>
      {/* Header */}
      <Typography variant="h4" component="h1" gutterBottom>
        Real-Time Analytics Dashboard
      </Typography>
      
      {/* KPI Cards Grid */}
      <Grid container spacing={3}>
        {/* Orders Card */}
        <Grid size={{ xs: 12, sm: 6, md: 3 }}>
          <MetricCard
            title="Orders"
            value={metrics?.latest_kpis?.orders || 0}
            icon={<ShoppingCartIcon />}
            color="primary"
          />
        </Grid>
        
        {/* Revenue Card */}
        <Grid size={{ xs: 12, sm: 6, md: 3 }}>
          <MetricCard
            title="Revenue (GMV)"
            value={`$${(metrics?.latest_kpis?.gmv || 0).toLocaleString()}`}
            icon={<AttachMoneyIcon />}
            color="success"
          />
        </Grid>
        
        {/* Conversion Rate Card */}
        <Grid size={{ xs: 12, sm: 6, md: 3 }}>
          <MetricCard
            title="Conversion Rate"
            value={`${((metrics?.latest_kpis?.conversion_rate || 0) * 100).toFixed(2)}%`}
            icon={<TrendingUpIcon />}
            color="info"
          />
        </Grid>
        
        {/* Active Users Card */}
        <Grid size={{ xs: 12, sm: 6, md: 3 }}>
          <MetricCard
            title="Active Users"
            value={metrics?.latest_kpis?.active_users || 0}
            icon={<PeopleIcon />}
            color="warning"
          />
        </Grid>
      </Grid>
      
      {/* Charts Section */}
      <Grid container spacing={3} sx={{ mt: 2 }}>
        {/* Conversion Trend Chart */}
        <Grid size={{ xs: 12, md: 6 }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Conversion Rate Trend
              </Typography>
              <ConversionTrendChart data={conversionTrend} />
            </CardContent>
          </Card>
        </Grid>
        
        {/* Revenue Trend Chart */}
        <Grid size={{ xs: 12, md: 6 }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Revenue Trend (GMV)
              </Typography>
              <RevenueTrendChart data={revenueTrend} />
            </CardContent>
          </Card>
        </Grid>
      </Grid>
      
      {/* Recent Events Table */}
      <Grid container spacing={3} sx={{ mt: 2 }}>
        <Grid size={{ xs: 12 }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Recent Event Activity
              </Typography>
              <RecentEventsTable events={metrics?.recent_events} />
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};
```

### Metric Card Component
```tsx
interface MetricCardProps {
  title: string;
  value: string | number;
  icon: React.ReactNode;
  color: 'primary' | 'secondary' | 'success' | 'error' | 'info' | 'warning';
  trend?: {
    direction: 'up' | 'down' | 'stable';
    percentage: number;
  };
}

const MetricCard: React.FC<MetricCardProps> = ({ title, value, icon, color, trend }) => {
  return (
    <Card sx={{ height: '100%' }}>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 40,
              height: 40,
              borderRadius: 1,
              bgcolor: `${color}.light`,
              color: `${color}.dark`,
              mr: 2,
            }}
          >
            {icon}
          </Box>
          <Typography variant="h6" component="div">
            {title}
          </Typography>
        </Box>
        
        <Typography variant="h4" component="div" sx={{ mb: 1 }}>
          {value}
        </Typography>
        
        {trend && (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            {trend.direction === 'up' && <TrendingUpIcon color="success" />}
            {trend.direction === 'down' && <TrendingDownIcon color="error" />}
            {trend.direction === 'stable' && <TrendingFlatIcon color="info" />}
            <Typography
              variant="body2"
              color={trend.direction === 'up' ? 'success.main' : trend.direction === 'down' ? 'error.main' : 'text.secondary'}
              sx={{ ml: 0.5 }}
            >
              {trend.percentage > 0 ? '+' : ''}{trend.percentage.toFixed(1)}%
            </Typography>
          </Box>
        )}
      </CardContent>
    </Card>
  );
};
```

### Chart Components

#### Conversion Rate Trend Chart
```tsx
interface ConversionTrendChartProps {
  data: Array<{
    window_start: string;
    conversion_rate: number;
    purchase_users: number;
    view_users: number;
  }>;
}

const ConversionTrendChart: React.FC<ConversionTrendChartProps> = ({ data }) => {
  const chartData = data.map(point => ({
    time: new Date(point.window_start).toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit' 
    }),
    conversion_rate: (point.conversion_rate * 100).toFixed(2),
    purchase_users: point.purchase_users,
    view_users: point.view_users
  }));

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="time" 
          tick={{ fontSize: 12 }}
          angle={-45}
          textAnchor="end"
          height={60}
        />
        <YAxis 
          tick={{ fontSize: 12 }}
          label={{ value: 'Conversion Rate (%)', angle: -90, position: 'insideLeft' }}
        />
        <Tooltip 
          formatter={(value, name) => [
            name === 'conversion_rate' ? `${value}%` : value,
            name === 'conversion_rate' ? 'Conversion Rate' : 
            name === 'purchase_users' ? 'Purchase Users' : 'View Users'
          ]}
          labelFormatter={(label) => `Time: ${label}`}
        />
        <Line 
          type="monotone" 
          dataKey="conversion_rate" 
          stroke="#2196f3" 
          strokeWidth={2}
          dot={{ fill: '#2196f3', strokeWidth: 2, r: 4 }}
          activeDot={{ r: 6 }}
        />
      </LineChart>
    </ResponsiveContainer>
  );
};
```

#### Revenue Trend Chart
```tsx
interface RevenueTrendChartProps {
  data: Array<{
    window_start: string;
    gmv: number;
    orders: number;
  }>;
}

const RevenueTrendChart: React.FC<RevenueTrendChartProps> = ({ data }) => {
  const chartData = data.map(point => ({
    time: new Date(point.window_start).toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit' 
    }),
    gmv: point.gmv,
    orders: point.orders,
    avg_order_value: point.orders > 0 ? (point.gmv / point.orders).toFixed(2) : 0
  }));

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="time" 
          tick={{ fontSize: 12 }}
          angle={-45}
          textAnchor="end"
          height={60}
        />
        <YAxis 
          tick={{ fontSize: 12 }}
          label={{ value: 'Revenue ($)', angle: -90, position: 'insideLeft' }}
        />
        <Tooltip 
          formatter={(value, name) => [
            name === 'gmv' ? `$${Number(value).toLocaleString()}` : 
            name === 'avg_order_value' ? `$${value}` : value,
            name === 'gmv' ? 'Revenue (GMV)' : 
            name === 'orders' ? 'Orders' : 'Avg Order Value'
          ]}
          labelFormatter={(label) => `Time: ${label}`}
        />
        <Line 
          type="monotone" 
          dataKey="gmv" 
          stroke="#4caf50" 
          strokeWidth={2}
          dot={{ fill: '#4caf50', strokeWidth: 2, r: 4 }}
          activeDot={{ r: 6 }}
        />
      </LineChart>
    </ResponsiveContainer>
  );
};
```

### Recent Events Table
```tsx
interface RecentEventsTableProps {
  events: {
    page_view: number;
    add_to_cart: number;
    purchase: number;
  };
}

const RecentEventsTable: React.FC<RecentEventsTableProps> = ({ events }) => {
  if (!events) return <Typography>No recent events data</Typography>;

  const eventData = [
    { type: 'Page Views', count: events.page_view, icon: <VisibilityIcon />, color: 'info' },
    { type: 'Add to Cart', count: events.add_to_cart, icon: <AddShoppingCartIcon />, color: 'warning' },
    { type: 'Purchases', count: events.purchase, icon: <ShoppingCartIcon />, color: 'success' }
  ];

  return (
    <Box sx={{ mt: 2 }}>
      <Grid container spacing={2}>
        {eventData.map((event, index) => (
          <Grid size={{ xs: 12, sm: 4 }} key={index}>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                p: 2,
                border: 1,
                borderColor: 'divider',
                borderRadius: 1,
                bgcolor: 'background.paper'
              }}
            >
              <Box sx={{ mr: 2, color: `${event.color}.main` }}>
                {event.icon}
              </Box>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  {event.type}
                </Typography>
                <Typography variant="h6">
                  {event.count.toLocaleString()}
                </Typography>
              </Box>
            </Box>
          </Grid>
        ))}
      </Grid>
    </Box>
  );
};
```

## Data Integration

### API Client
```tsx
// API service for data fetching
class ApiService {
  private baseURL: string;

  constructor() {
    this.baseURL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
  }

  async getMetrics(): Promise<MetricsResponse> {
    const response = await fetch(`${this.baseURL}/metrics`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }

  async getConversionTrend(limit: number = 60): Promise<ConversionTrendResponse> {
    const response = await fetch(`${this.baseURL}/trend/conversion?limit=${limit}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }

  async getRevenueTrend(limit: number = 60): Promise<RevenueTrendResponse> {
    const response = await fetch(`${this.baseURL}/trend/revenue?limit=${limit}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  }
}

const apiService = new ApiService();
```

### React Hooks for Data Management
```tsx
// Custom hook for metrics data
const useMetrics = (refreshInterval: number = 30000) => {
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        setLoading(true);
        const data = await apiService.getMetrics();
        setMetrics(data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, refreshInterval);

    return () => clearInterval(interval);
  }, [refreshInterval]);

  return { metrics, loading, error, refetch: fetchMetrics };
};

// Custom hook for trend data
const useTrendData = (refreshInterval: number = 60000) => {
  const [conversionTrend, setConversionTrend] = useState<ConversionTrendData[]>([]);
  const [revenueTrend, setRevenueTrend] = useState<RevenueTrendData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchTrendData = async () => {
      try {
        setLoading(true);
        const [conversionData, revenueData] = await Promise.all([
          apiService.getConversionTrend(),
          apiService.getRevenueTrend()
        ]);
        
        setConversionTrend(conversionData.data);
        setRevenueTrend(revenueData.data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };

    fetchTrendData();
    const interval = setInterval(fetchTrendData, refreshInterval);

    return () => clearInterval(interval);
  }, [refreshInterval]);

  return { conversionTrend, revenueTrend, loading, error };
};
```

## Responsive Design

### Material-UI Grid System (v7)
```tsx
// Updated Grid API for MUI v7
<Grid container spacing={3}>
  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
    {/* Content for extra-small: 12 cols, small: 6 cols, medium+: 3 cols */}
  </Grid>
  <Grid size={{ xs: 12, md: 6 }}>
    {/* Content for extra-small: 12 cols, medium+: 6 cols */}
  </Grid>
</Grid>
```

### Responsive Breakpoints
```tsx
// Theme configuration for responsive design
const theme = createTheme({
  breakpoints: {
    values: {
      xs: 0,
      sm: 600,
      md: 900,
      lg: 1200,
      xl: 1536,
    },
  },
  // Custom responsive styles
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 12,
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
        },
      },
    },
  },
});
```

### Mobile Optimization
```tsx
// Mobile-first responsive chart container
const ResponsiveChartContainer: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  return (
    <Box sx={{ width: '100%', height: isMobile ? 250 : 300 }}>
      {children}
    </Box>
  );
};
```

## Real-Time Updates

### Polling Strategy
```tsx
// Dashboard with configurable refresh rates
const Dashboard: React.FC = () => {
  const [refreshRate, setRefreshRate] = useState(30000); // 30 seconds default
  
  const { metrics, loading: metricsLoading } = useMetrics(refreshRate);
  const { conversionTrend, revenueTrend, loading: trendsLoading } = useTrendData(60000);

  // Real-time status indicator
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  useEffect(() => {
    if (metrics) {
      setLastUpdate(new Date());
    }
  }, [metrics]);

  return (
    <Box>
      {/* Status bar with last update time */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h4">Real-Time Analytics Dashboard</Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Chip 
            icon={<AccessTimeIcon />}
            label={`Updated: ${lastUpdate.toLocaleTimeString()}`}
            color={metricsLoading ? "warning" : "success"}
            variant="outlined"
          />
          <FormControl size="small">
            <InputLabel>Refresh Rate</InputLabel>
            <Select
              value={refreshRate}
              onChange={(e) => setRefreshRate(Number(e.target.value))}
            >
              <MenuItem value={10000}>10s</MenuItem>
              <MenuItem value={30000}>30s</MenuItem>
              <MenuItem value={60000}>1m</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </Box>
      
      {/* Dashboard content */}
      {/* ... */}
    </Box>
  );
};
```

### Error Boundaries
```tsx
// Error boundary for graceful error handling
class DashboardErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error('Dashboard error:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <Box sx={{ p: 3, textAlign: 'center' }}>
          <Typography variant="h5" color="error" gutterBottom>
            Dashboard Error
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Something went wrong loading the dashboard. Please refresh the page.
          </Typography>
          <Button 
            variant="contained" 
            onClick={() => window.location.reload()}
            sx={{ mt: 2 }}
          >
            Refresh Page
          </Button>
        </Box>
      );
    }

    return this.props.children;
  }
}
```

## Build & Deployment

### Docker Configuration
```dockerfile
# Frontend Dockerfile
FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./
RUN npm ci --only=production

# Copy source code
COPY . .

# Build application
RUN npm run build

# Serve with nginx
FROM nginx:alpine
COPY --from=0 /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 3000
CMD ["nginx", "-g", "daemon off;"]
```

### Environment Configuration
```bash
# Environment variables
REACT_APP_API_URL=http://localhost:8000
REACT_APP_QDRANT_URL=http://localhost:6333
REACT_APP_REFRESH_INTERVAL=30000
REACT_APP_ENABLE_DEBUG=false
```

### Performance Optimization
- **Code Splitting**: Lazy loading for route components
- **Bundle Analysis**: webpack-bundle-analyzer for size optimization  
- **Image Optimization**: WebP format with fallbacks
- **Caching**: Service worker for offline functionality
- **Tree Shaking**: Eliminate unused Material-UI components
