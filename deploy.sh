#!/bin/bash
# =============================================================================
# TTV Analysis - One-Command Deploy
#
# Usage: ./deploy.sh
#
# What it does:
#   1. Ensures correct gcloud account
#   2. Syncs code to VM
#   3. Sets up symlinks (shared .env for SF credentials)
#   4. Clears Python cache
#   5. Installs Python dependencies
#   6. Restarts dashboard on port 8506
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT="yalo-scanning-agent"
ZONE="us-central1-a"
VM="scanning-agent-vm"
REQUIRED_ACCOUNT="ernestodelae@gmail.com"
PORT=8506

echo "=============================================="
echo "  TTV Analysis - Deploy to Production"
echo "=============================================="
echo ""

# Step 1: Check gcloud account
echo -e "${YELLOW}[1/6] Checking gcloud account...${NC}"
CURRENT_ACCOUNT=$(gcloud config get-value account 2>/dev/null)
if [ "$CURRENT_ACCOUNT" != "$REQUIRED_ACCOUNT" ]; then
    echo "  Switching from $CURRENT_ACCOUNT to $REQUIRED_ACCOUNT"
    gcloud config set account "$REQUIRED_ACCOUNT" 2>/dev/null
fi
echo -e "${GREEN}  ✓ Using account: $REQUIRED_ACCOUNT${NC}"

# Step 2: Create and sync tarball
echo ""
echo -e "${YELLOW}[2/6] Syncing code to VM...${NC}"
TARBALL="/tmp/ttv_analysis_code.tar.gz"

cd "$SCRIPT_DIR"
tar -czf "$TARBALL" \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.git' \
    --exclude='venv' \
    --exclude='*.egg-info' \
    --exclude='.env' \
    --exclude='data/*.db' \
    app.py requirements.txt api_clients config components services data scripts

gcloud compute scp "$TARBALL" \
    "$VM:/tmp/ttv_analysis_code.tar.gz" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --quiet

gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="sudo mkdir -p /opt/ttv_analysis/logs && sudo chown -R \$(whoami):\$(whoami) /opt/ttv_analysis && cd /opt/ttv_analysis && tar -xzf /tmp/ttv_analysis_code.tar.gz && rm /tmp/ttv_analysis_code.tar.gz" \
    --quiet

rm "$TARBALL"
echo -e "${GREEN}  ✓ Code synced to /opt/ttv_analysis/${NC}"

# Step 3: Set up symlinks (shared .env for SF credentials)
echo ""
echo -e "${YELLOW}[3/6] Setting up symlinks...${NC}"
gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="cd /opt/ttv_analysis && ln -sf /opt/scanning_agent/.env .env 2>/dev/null; echo done" \
    --quiet
echo -e "${GREEN}  ✓ Symlinks configured${NC}"

# Step 4: Clear Python cache
echo ""
echo -e "${YELLOW}[4/6] Clearing Python cache...${NC}"
gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="find /opt/ttv_analysis -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; echo done" \
    --quiet
echo -e "${GREEN}  ✓ Cache cleared${NC}"

# Step 5: Install dependencies
echo ""
echo -e "${YELLOW}[5/6] Installing Python dependencies...${NC}"
gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="cd /opt/ttv_analysis && /opt/scanning_agent/venv/bin/pip install -r requirements.txt --quiet 2>&1 | tail -3" \
    --quiet
echo -e "${GREEN}  ✓ Dependencies installed${NC}"

# Step 6: Restart dashboard on port 8506
echo ""
echo -e "${YELLOW}[6/6] Restarting TTV dashboard (port $PORT)...${NC}"

# Write the startup script on the VM
gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="cat > /opt/ttv_analysis/start_dashboard.sh << 'STARTUP'
#!/bin/bash
cd /opt/ttv_analysis

# Source shared .env
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Use Yalo account for BigQuery access (env var is process-scoped, more reliable than gcloud config)
export CLOUDSDK_CORE_ACCOUNT=ernesto.espriella@yalo.com

export PYTHONPATH=/opt/ttv_analysis
exec /opt/scanning_agent/venv/bin/streamlit run app.py \
    --server.port=8506 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.baseUrlPath=ttv \
    --browser.gatherUsageStats=false
STARTUP
chmod +x /opt/ttv_analysis/start_dashboard.sh" \
    --quiet

# Kill existing process on port and start fresh
gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="lsof -ti:$PORT | xargs kill 2>/dev/null; sleep 1; setsid nohup /opt/ttv_analysis/start_dashboard.sh </dev/null > /opt/ttv_analysis/logs/ttv_dashboard.log 2>&1 & disown; echo started" \
    --quiet

# Verify it's running
sleep 3
gcloud compute ssh "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --command="lsof -ti:$PORT || echo ''" \
    --quiet

EXTERNAL_IP=$(gcloud compute instances describe "$VM" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo -e "${GREEN}  ✓ Dashboard running on port $PORT${NC}"
echo ""
echo "=============================================="
echo -e "${GREEN}  Deploy complete!${NC}"
echo ""
echo "  Dashboard:  https://yalo.ernestodelae.com/ttv/"
echo "  Direct IP:  http://$EXTERNAL_IP:$PORT/ttv/"
echo "=============================================="
