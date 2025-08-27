# RT-Lakehouse Deployment Guide

## Deployment Options

The RT-Lakehouse can be deployed to the web using several approaches:

### 1. 🚀 **Quick Deploy (Recommended for Demo)**
- **Platform**: Railway, Render, or DigitalOcean App Platform
- **Complexity**: Low
- **Cost**: $20-50/month
- **Setup Time**: 15 minutes

### 2. ☁️ **Cloud Platform Deploy**
- **Platform**: AWS, GCP, Azure
- **Complexity**: Medium
- **Cost**: $50-200/month
- **Setup Time**: 1-2 hours

### 3. 🐳 **Container Deploy**
- **Platform**: Docker + VPS (Linode, DigitalOcean)
- **Complexity**: Medium
- **Cost**: $40-100/month
- **Setup Time**: 30 minutes

### 4. ⚡ **Serverless Deploy** 
- **Platform**: Vercel + Supabase + Kafka Cloud
- **Complexity**: High
- **Cost**: $30-80/month
- **Setup Time**: 2-3 hours

---

## 🚀 Quick Deploy (Railway) - FASTEST OPTION

### Step 1: Prepare for Railway
Railway automatically deploys from your GitHub repo with Docker Compose support.

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login to Railway
railway login

# Initialize project
railway init
```

### Step 2: Configure Railway Services
Railway will detect your docker-compose.yml and create services automatically.

### Step 3: Set Environment Variables
```bash
# Required environment variables for Railway
KAFKA_BOOTSTRAP=your-kafka-instance.railway.app:9092
OPENROUTER_API_KEY=your_openrouter_key
OPENAI_API_KEY=your_openai_key
```

### Step 4: Deploy
```bash
railway up
```

**Result**: Your app will be live at `https://your-app.railway.app` in ~5 minutes!

---

## ☁️ AWS Cloud Deploy

### Architecture Overview
```
Internet Gateway
    ↓
Application Load Balancer
    ↓
ECS Fargate Services
    ├── Frontend (React)
    ├── API (FastAPI) 
    ├── Monitoring (Streamlit)
    └── Spark Streaming
    ↓
RDS (PostgreSQL) + MSK (Kafka) + S3 (Delta Lake)
```

### Step 1: Infrastructure as Code
```bash
# Create AWS resources using CloudFormation/Terraform
cd deployment/aws
terraform init
terraform plan
terraform apply
```

### Step 2: Container Registry
```bash
# Build and push images to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789.dkr.ecr.us-east-1.amazonaws.com

# Tag and push each service
docker tag rt-lakehouse-frontend:latest 123456789.dkr.ecr.us-east-1.amazonaws.com/rt-lakehouse-frontend:latest
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/rt-lakehouse-frontend:latest
```

### Step 3: Deploy to ECS
The GitHub Actions workflow will handle ECS deployment automatically.

**Estimated Cost**: $80-150/month for production workload

---

## 🐳 Docker VPS Deploy (DigitalOcean)

### Step 1: Create VPS
```bash
# Create a $40/month droplet with Docker pre-installed
# CPU: 4 cores, RAM: 8GB, SSD: 160GB
```

### Step 2: Setup Domain & SSL
```bash
# Point your domain to the droplet IP
# Install Caddy for automatic HTTPS
sudo apt update
sudo apt install -y caddy
```

### Step 3: Deploy with Docker Compose
```bash
# Clone your repo on the VPS
git clone https://github.com/bhurtyalkritan/data-monorepo.git
cd data-monorepo/rt-lakehouse

# Create production environment
cp .env.example .env.production
# Edit with your production values

# Deploy with production overrides
docker-compose -f docker-compose.yml -f docker-compose.production.yml up -d
```

**Result**: App available at `https://yourdomain.com`

---

## ⚡ Serverless Deploy (Vercel + Cloud Services)

### Component Mapping
- **Frontend**: Vercel (Free tier available)
- **API**: Vercel Functions or Railway
- **Database**: Supabase PostgreSQL (Free tier)
- **Kafka**: Confluent Cloud (Pay-as-you-go)
- **Storage**: Vercel KV or Upstash Redis

### Step 1: Deploy Frontend
```bash
cd services/frontend
npx vercel --prod
```

### Step 2: Deploy API
```bash
# Convert FastAPI to Vercel Functions
cd services
npx vercel --prod
```

### Step 3: Configure External Services
- Confluent Cloud for Kafka
- Supabase for PostgreSQL
- Vercel KV for caching

**Result**: Globally distributed, auto-scaling deployment

---

## 📊 Cost Comparison

| Platform | Monthly Cost | Pros | Cons |
|----------|-------------|------|------|
| Railway | $20-50 | Easiest setup, automatic scaling | Limited customization |
| AWS ECS | $80-150 | Full control, enterprise-grade | Complex setup |
| VPS + Docker | $40-100 | Good balance, predictable costs | Manual scaling |
| Serverless | $30-80 | Pay-per-use, global CDN | Cold starts, complexity |

---

## 🔧 Production Configuration

### Environment Variables for Production
```bash
# API Configuration
ENVIRONMENT=production
API_WORKERS=4
MAX_CONNECTIONS=100

# Database
DATABASE_URL=postgresql://user:pass@host:5432/rtlakehouse
REDIS_URL=redis://user:pass@host:6379

# Kafka
KAFKA_BOOTSTRAP=your-kafka-cluster:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN

# Monitoring
SENTRY_DSN=your-sentry-dsn
LOG_LEVEL=INFO

# Security
JWT_SECRET=your-secret-key
CORS_ORIGINS=https://yourdomain.com
```

### SSL/HTTPS Setup
All deployment options include automatic HTTPS:
- Railway: Automatic SSL
- AWS: ALB with ACM certificates  
- VPS: Caddy automatic HTTPS
- Vercel: Built-in SSL

### Monitoring & Alerts
- **Uptime**: UptimeRobot (free)
- **Errors**: Sentry (free tier)
- **Metrics**: Built-in Streamlit dashboard
- **Logs**: Platform-specific (Railway, CloudWatch, etc.)

---

## 🚀 RECOMMENDED: Railway Quick Deploy

For your Databricks interview demo, I recommend Railway because:

1. **5-minute setup** - Deploy directly from GitHub
2. **Automatic HTTPS** - Professional URLs
3. **Zero DevOps** - Focus on the technical discussion
4. **Free trial** - No upfront costs
5. **Real-time logs** - Easy debugging

### Railway Deploy Commands
```bash
# 1. Install Railway CLI
npm install -g @railway/cli

# 2. Login and create project
railway login
cd rt-lakehouse
railway init

# 3. Deploy all services
railway up

# 4. Set environment variables in Railway dashboard
# OPENROUTER_API_KEY, KAFKA settings, etc.
```

**Your app will be live at a public URL in under 10 minutes!**