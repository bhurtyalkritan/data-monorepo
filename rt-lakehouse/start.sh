#!/bin/bash

# RT-Lakehouse Startup Script
# This script starts all services in the correct order

set -e

echo "🏠 Starting RT-Lakehouse Analytics Platform..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install Docker Compose."
    exit 1
fi

# Navigate to the project directory
cd "$(dirname "$0")"

echo "📋 Checking services configuration..."

# Create necessary directories
mkdir -p data/delta
mkdir -p data/checkpoints/{bronze,silver,gold}
mkdir -p data/kafka
mkdir -p data/zookeeper
mkdir -p data/qdrant

echo "🧹 Cleaning up any existing containers..."
docker-compose down --remove-orphans

echo "🔨 Building Docker images..."
docker-compose build

echo "🚀 Starting core infrastructure services..."

# Start Zookeeper and Kafka first
docker-compose up -d zookeeper kafka

echo "⏱️  Waiting for Kafka to be ready..."

# Wait up to ~5 minutes for Kafka to become available on the internal listener
for i in {1..60}; do
  if docker-compose exec -T kafka bash -lc "kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 || kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1"; then
    echo "✅ Kafka is ready!"
    break
  fi
  echo "   Waiting for Kafka..."
  sleep 5
done

if [ "$i" -eq 60 ]; then
  echo "❌ Kafka failed to become ready in time. Showing recent logs:"
  docker-compose logs --tail=200 kafka || true
  exit 1
fi

# Create Kafka topic
echo "📝 Creating Kafka topic..."
docker-compose exec -T kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --topic ecommerce_events --partitions 3 --replication-factor 1 --if-not-exists || kafka-topics --bootstrap-server kafka:29092 --create --topic ecommerce_events --partitions 3 --replication-factor 1 --if-not-exists"

echo "🗄️  Starting databases..."
docker-compose up -d qdrant

echo "⏱️  Waiting for Qdrant to be ready..."
sleep 10

echo "🔥 Starting Spark streaming pipeline..."
docker-compose up -d spark-streaming

echo "⏱️  Waiting for Spark to initialize..."
sleep 20

echo "🤖 Starting AI Assistant API..."
docker-compose up -d assistant-api

echo "⏱️  Waiting for Assistant API to be ready..."
sleep 10

echo "🎛️  Starting monitoring dashboard..."
docker-compose up -d monitoring

echo "🌐 Starting frontend dashboard..."
docker-compose up -d frontend

echo "📊 Starting event producer..."
docker-compose up -d producer

echo ""
echo "🎉 All services started successfully!"
echo ""
echo "📊 Your RT-Lakehouse is now running:"
echo "   🌐 Frontend Dashboard:    http://localhost:3000"
echo "   📈 Monitoring Dashboard:  http://localhost:8501"
echo "   🤖 Assistant API:        http://localhost:8000"
echo "   🔧 Assistant Docs:       http://localhost:8000/docs"
echo ""
echo "🔍 To check service status:"
echo "   docker-compose ps"
echo ""
echo "📋 To view logs:"
echo "   docker-compose logs -f [service-name]"
echo ""
echo "⏹️  To stop all services:"
echo "   docker-compose down"
echo ""
echo "🔥 Data is being generated and processed in real-time!"
echo "   Check the dashboards to see live analytics."

# Optional: show service status
echo ""
echo "📊 Current service status:"
docker-compose ps
