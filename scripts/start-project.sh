#!/bin/bash

# Auto-fix script permissions if needed
if [ ! -x "scripts/setup-metabase.sh" ]; then
    echo "Setting up script permissions..."
    chmod +x scripts/*.sh
fi

echo "Starting GA4 Realtime Analytics Project..."

# Start services with astro
astro dev start

echo "Waiting for Metabase to be ready..."
sleep 60

# Run setup script using curl from host
echo "Setting up Metabase..."
./scripts/setup-metabase.sh

echo "✅ Project started successfully!"
echo ""
echo "🚀 Services available at:"
echo "   • Airflow UI: http://localhost:8080 (admin/admin)"
echo "   • Metabase: http://localhost:3000 (admin@example.com/AdminPass123!)"
echo ""

# Function to detect OS and open browser (macOS/Linux only)
open_browser() {
    local url=$1
    
    if command -v open > /dev/null; then
        # macOS
        open "$url"
    elif command -v xdg-open > /dev/null; then
        # Linux
        xdg-open "$url"
    else
        echo "Please open $url manually in your browser"
    fi
}

# Wait a bit more to ensure services are fully ready
echo "Opening Metabase in browser..."
sleep 10

# Only open Metabase since Airflow opens automatically
open_browser "http://localhost:3000"

echo ""
echo "🎉 Metabase opened in browser!"
echo ""
echo "Next steps:"
echo "1. In Airflow: Enable the 'ga4_realtime_pipeline' DAG and let it run"
echo "2. In Metabase: Check 'Databases' -> 'GA4 Analytics' to see your tables"
echo "3. Create dashboards using the GA4 data tables"
echo ""
echo "📊 Tables will appear after the DAG runs successfully!"