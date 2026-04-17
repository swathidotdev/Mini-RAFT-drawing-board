Project: Mini-RAFT Distributed Drawing Board

Team Members:
1. Swathi M – Frontend
2. Srujana T– Gateway
3. Srividhya M – Replica
4. Spoorthi V – Mini-RAFT + Docker

## Description

A real-time collaborative drawing board using the Mini-RAFT consensus algorithm to ensure consistency and fault tolerance across distributed replicas. Users can draw collaboratively, and the system maintains synchronization even in the presence of network partitions or node failures.

## Features

- Real-time collaborative drawing on a shared canvas
- Fault-tolerant architecture using RAFT consensus
- WebSocket-based communication for live updates
- Automatic leader election and log replication
- Docker containerization for easy deployment
- Status indicators for connection, leader, and log state

## Architecture

The system consists of three main components:

- **Frontend**: HTML5 Canvas-based drawing interface with WebSocket client
- **Gateway**: Express.js server that handles client connections and routes requests to replicas
- **Replicas**: Three RAFT nodes that maintain the distributed state and consensus

### Network Topology

- Gateway runs on port 3000
- Replicas run on ports 4001, 4002, 4003
- All services communicate via a Docker network

## Prerequisites

- Docker and Docker Compose
- Node.js (for local development)
- Modern web browser with WebSocket support

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/s-r-i-v-i-d-h-y-a-m/CC-mini-raft-drawing-board.git
   cd CC-mini-raft-drawing-board
   ```

2. Start the services using Docker Compose:
   ```bash
   cd docker
   docker-compose up --build
   ```

## Usage

1. Start the services using Docker Compose:
   ```bash
   cd docker
   docker-compose up --build
   ```

2. In a separate terminal or using VS Code Live Preview, serve the frontend files from the `frontend/` directory on port 3000 (or any port, but update the WebSocket URL in `canvas.js` if different).

3. Open your browser and navigate to the frontend URL (use live preview)

4. Wait for the WebSocket connection to establish (status bar will show "Connected").

5. Start drawing on the canvas - your strokes will be synchronized across all connected clients.

## Technologies Used

- **Frontend**: HTML5 Canvas, JavaScript, CSS
- **Backend**: Node.js, Express.js
- **Communication**: WebSockets, HTTP
- **Consensus**: Custom Mini-RAFT implementation
- **Containerization**: Docker, Docker Compose

## Development

For local development without Docker:

1. Install dependencies for each service:
   ```bash
   cd gateway && npm install
   cd ../replicas && npm install
   ```

2. Start the replicas:
   ```bash
   cd replicas
   npm run dev  # Starts replica1 on port 4001
   # In separate terminals, start replica2 and replica3 with appropriate environment variables
   ```

3. Start the gateway:
   ```bash
   cd gateway
   npm run dev
   ```

4. Serve the frontend (e.g., using a simple HTTP server):
   ```bash
   cd frontend
   python -m http.server 8080  # or use any static file server
   ```

## API Endpoints

### Gateway (Port 3000)
- `POST /leader` - Register a new leader replica
- `POST /broadcast` - Broadcast committed strokes to clients
- `GET /status` - Gateway health and status
- `WS /` - WebSocket endpoint for real-time drawing updates

### Replicas (Ports 4001, 4002, 4003)
- `POST /stroke` - Submit drawing commands
- `GET /status` - Replica status and state
- `GET /log` - Retrieve log entries for history replay
