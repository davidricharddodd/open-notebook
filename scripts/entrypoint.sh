#!/bin/bash
# Entrypoint script for Railway deployment
# Raises file descriptor limits to prevent "Resource temporarily unavailable" 
# errors when combining podcast audio clips with moviepy/ffmpeg

# Try to raise file descriptor limits (may be restricted by container runtime)
ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || true
ulimit -u 4096 2>/dev/null || true

echo "File descriptor limit: $(ulimit -n)"
echo "Process limit: $(ulimit -u)"

# Start supervisord
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
