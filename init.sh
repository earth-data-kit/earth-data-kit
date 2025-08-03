source .env

# Create workspace requirements.txt if it doesn't exist
WORKSPACE_DIR="${WORKSPACE_DIR:-./workspace}"
REQ_FILE="$WORKSPACE_DIR/requirements.txt"
if [ ! -f "$REQ_FILE" ]; then
  mkdir -p "$WORKSPACE_DIR"
  touch "$REQ_FILE"
fi


docker compose --env-file .env -f docker-compose.yml up --build