# Distributed NoSQL Database System for E-commerce Platform

This project is a **Distributed Database Management System** designed for efficient management of customer data across multiple regional databases. It allows the system to handle node availability dynamically, ensuring that data operations such as creating records are logged, replicated, and recovered efficiently using **Kafka** for messaging and **Redis** for storing node statuses.

The system leverages a **FastAPI-based middleware** to facilitate seamless interactions between the frontend and backend, while handling node unavailability gracefully by logging data to Kafka and processing it when the nodes come back online. Additionally, **MongoDB** serves as the primary database system, with cross-region replication for data redundancy and fault tolerance.

---

## Key Features

- **Dynamic Node Status Management**: Node statuses are managed using Redis, allowing real-time updates to node availability.
- **Kafka for Logging Unavailable Nodes**: When a node is unavailable, customer records are logged to Kafka for later processing.
- **Cross-Region Replication**: Ensures data redundancy by replicating data across regional databases.
- **Efficient Middleware**: FastAPI-based backend efficiently routes data and handles recovery operations.
- **Streamlined Frontend**: A user-friendly interface for viewing and creating customer records with node status displayed dynamically.

---

## Why Use Kafka and Redis?

### Kafka:
- **Reliable Logging**: Ensures no data is lost during node downtime by logging to topics for each unavailable node.
- **Asynchronous Processing**: Processes Kafka messages only when nodes become available, ensuring efficient recovery.

### Redis:
- **Real-Time Updates**: Stores and retrieves node statuses instantly for dynamic routing.
- **Persistence**: Retains node statuses across application restarts.

---

## Setup Instructions

### Prerequisites

- **Docker**: Ensure Docker is installed and running.
- **Kafka and Redis**: Ensure Kafka and Redis are available via Docker or local installation.
- **Python 3.9+**

---

### Installation Steps

#### 1. Clone the Repository
```bash
git clone git@github.com:aarti-kalekar/cse-512-project.git
cd /cse-512-project
```

#### 2. Open the Application folder
```bash
cd /cse-512-project/Application
```

#### 2. Set Up the Python Environment
```bash
python -m venv myenv
source myenv/bin/activate  # On Windows: myenv\Scripts\activate
pip install -r requirements.txt
```

#### 3. Start Services Using Docker Compose
Ensure Docker is running and download redis and kafka images in docker desktop, then execute:
```bash
1. docker compose up
2. cd /cse-512-project/Application/kafka-docker
3. docker-compose -f docker-compose.yml up -d
4. cd /cse-512-project/Application/redis-docker
5. docker-compose -f docker-compose.yml up -d
```

#### 5. Start the Backend
```bash
fastapi dev app.py
```

#### 6. Access the Frontend
Open the following URLs in your browser:
- **index.html**: `open using live server in vscode(http://localhost:5500/cse-512-project/Application/index.html)`
![image](https://github.com/user-attachments/assets/0770d0b8-b984-4242-bf26-715efa09fa2a)
![image](https://github.com/user-attachments/assets/cb68fc40-f633-4730-b866-46e4009c74e6)
![image](https://github.com/user-attachments/assets/00391760-ab2e-479e-a3e2-4010cc8a2da2)

---

## Usage Instructions

### Frontend
- **Node Management**: View real-time statuses of nodes using sliding switches.
- **Create Records**: Add customer data dynamically, with unavailability logged to Kafka.
- **View Records**: Query and view customer data across regions, leveraging alternate databases during downtime.

### Backend API Endpoints
- **Node Status**:
  - `POST /node-status`: Update the status of a node (available/unavailable).
- **Customer Records**:
  - `POST /create-customer`: Create a customer record, with fallback for unavailable nodes.
  - `GET /filter-records`: Query customer records with filters.

---

## Contribution Guidelines

Contributions are welcome! If you'd like to contribute:
1. Open an issue to discuss your proposal.
2. Fork the repository and create a feature branch.
3. Submit a pull request with a clear description of your changes.

For any questions, feel free to reach out!
