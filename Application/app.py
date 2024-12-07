from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType, NewTopic
from confluent_kafka.admin import AdminClient
import redis
import json
import time

app = FastAPI()

# Redis client setup
redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

# Global dictionary to simulate node statuses
node_statuses = {}

# Initialize default statuses in Redis (if not already set)
@app.on_event("startup")
async def init_node_statuses():
    for region in region_databases.keys():
        if not redis_client.hexists("node_statuses", region):
            redis_client.hset("node_statuses", region, "available")
            

@app.get("/node-statuses")
async def get_node_statuses():
    """
    Retrieve all node statuses from the persistent store (e.g., Redis or database).
    """
    node_statuses = redis_client.hgetall("node_statuses")  # Example with Redis
    print(node_statuses)
    return node_statuses


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this to specify the origins you want to allow
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods
    allow_headers=["*"],  # Allows all headers
)

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer_1 = KafkaConsumer(
    "unavailable-records",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=False,
    consumer_timeout_ms=4000,
    group_id = "regions_1"
)

consumer_2 = KafkaConsumer(
    "unavailable-records",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=False,
    consumer_timeout_ms=4000,
    group_id = "regions_2"
)

consumer_3 = KafkaConsumer(
    "unavailable-records",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=False,
    consumer_timeout_ms=4000,
    group_id = "regions_3"
)

consumers = {
    'region_1':consumer_1,
    'region_2':consumer_2,
    'region_3':consumer_3,
}

# admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="admin_client"
)


# MongoDB connection details
MONGO_DETAILS = "mongodb+srv://Akshay:FirstDB2024@akshay-cluster.63skn.mongodb.net/"
client = AsyncIOMotorClient(MONGO_DETAILS)

# Connect to each region's database and its customers collection
region_databases = {
    "region_1": client["olist_database_region_1"]["customers"],
    "region_2": client["olist_database_region_2"]["customers"],
    "region_3": client["olist_database_region_3"]["customers"]
}

alt_databases = {
"region_1":client["olist_database_region_3"]["customers_1"],
"region_2":client["olist_database_region_1"]["customers_2"],
"region_3":client["olist_database_region_2"]["customers_3"]
}


region_mapping = {
    "region_1":"region_3",
    "region_2":"region_1",
    "region_3":"region_2"
}

class NodeStatusRequest(BaseModel):
    node_id: str
    status: str

@app.post("/node-status")
async def update_node_status(request: NodeStatusRequest):
    if request.status.lower() not in ["available", "unavailable"]:
        raise HTTPException(status_code=400, detail="Invalid status. Use 'available' or 'unavailable'.")
    
    print(request.node_id, request.status)
    
    # Update node status in Redis
    redis_client.hset("node_statuses", request.node_id, request.status)
    
    
        # If the node becomes available, process Kafka messages
    if request.status.lower() == "available":
        print(f"Processing Kafka messages for {request.node_id}...")
        
        consumer = KafkaConsumer(
            request.node_id,
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            group_id=f"regions_{request.node_id}_{int(time.time())}",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=10000,
            enable_auto_commit=False,
        )
        
    
        # Subscribe to the Kafka topic named after the node_id
        consumer.subscribe([request.node_id])
        try:
            for message in consumer:
                record = message.value

                zip_code_prefix = record.get("customer_zip_code_prefix")
                region = get_region(zip_code_prefix)
                if region is None:
                    raise HTTPException(status_code=400, detail="Region could not be determined for the given zip code prefix.")
                if region == request.node_id:
                    collection = region_databases.get(request.node_id)
                    if collection is None:
                        raise HTTPException(status_code=500, detail=f"Database for {request.node_id} is not configured.")
                    # Write the record to MongoDB
                    await collection.insert_one(record)
                    print(f"Record written to {request.node_id}: {record}")
                    
                else:
                    collection = alt_databases[region]
                    if collection is None:
                        raise HTTPException(status_code=500, detail=f"No database configured for region: {region}")
                    await collection.insert_one(record)
                    print(f"Record written to {request.node_id}: {record}")                   
                # Commit the Kafka offset after processing
                consumer.commit()
            
        except Exception as e:
            print(f"Error while processing Kafka messages for {request.node_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to process Kafka messages: {str(e)}")
        admin_client.delete_topics(topics=[request.node_id])
        # new_topic = NewTopic(
        #     name=request.node_id,
        #     num_partitions=1,
        #     replication_factor=1
        # )
        # admin_client.create_topics([new_topic])
        # Create a ConfigResource for the topic
    return {"message": f"Node {request.node_id} status updated to {request.status} and processed pending records."}
    


def build_query(zipRangeMin: int, zipRangeMax: int, city: Optional[str] = None, state: Optional[str] = None) -> Dict[str, Any]:
    # Create the base query with integer range for zip_code_prefix
    query: Dict[str, Any] = {
        "customer_zip_code_prefix": {"$gte": zipRangeMin, "$lte": zipRangeMax}
    }
    
    # Add city filter if provided
    if city:
        query["customer_city"] = {"$regex": city, "$options": "i"}  # MongoDB regex expects strings here
    
    # Add state filter if provided
    if state:
        query["customer_state"] = {"$regex": state, "$options": "i"}

    return query

@app.post("/create-record")
async def create_record(record: dict):
    """
    Write a record to the appropriate database.
    If the node is unavailable, log the record to Kafka.
    """
    # Determine the region based on zip code prefix
    zip_code = int(record.get("zip_code_prefix", 0))
    region = None
    if 1000 <= zip_code <= 39999:
        region = "region_1"
    elif 40000 <= zip_code <= 69999:
        region = "region_2"
    elif 70000 <= zip_code <= 99999:
        region = "region_3"
    else:
        region = None

    if not region:
        raise HTTPException(status_code=400, detail="Invalid zip code prefix.")

    # Check node status in the global dictionary
    node_status = node_statuses.get(region, "unavailable")
    if node_status == "unavailable":
        # Log the record to Kafka
        producer.send("unavailable-records", value={"node_id": region, **record})
        producer.flush()
        return {"message": f"Node {region} is unavailable. Record logged to Kafka."}

    # Write directly to MongoDB if node is available
    collection = region_databases.get(region)
    if not collection:
        raise HTTPException(status_code=500, detail=f"Database for {region} is not configured.")
    
    await collection.insert_one(record)
    return {"message": f"Record successfully written to {region}"}


# Define the data model for incoming filter requests
class FilterRequest(BaseModel):
    zipRangeMin: int
    zipRangeMax: int
    city: Optional[str] = None
    state: Optional[str] = None

# Endpoint to filter records based on the criteria
@app.post("/filter-records")
async def filter_records(request: FilterRequest):
    # return filtered_records
    # Get node statuses from Redis
    node_statuses = redis_client.hgetall("node_statuses")

    # Identify unavailable regions
    unavailable_regions = [region for region, status in node_statuses.items() if status == "unavailable"]

    # Build region_databases_copy dynamically
    region_databases_copy = {}
    for region, collection in region_databases.items():
        if region not in unavailable_regions:
            region_databases_copy[region] = collection
        elif region_mapping[region] not in unavailable_regions:
            region_databases_copy[region] = alt_databases[region]

    
    # Build MongoDB query
    query = build_query(
            zipRangeMin=request.zipRangeMin,
            zipRangeMax=request.zipRangeMax,
            city=request.city,
            state=request.state
        )

    # Now you can use this `query` in MongoDB's `find` method without type issues
    records = []
    for region_name, collection in region_databases_copy.items():
        async for record in collection.find(query).limit(50):
            records.append({
                "customer_id": record.get("customer_id"),
                "customer_unique_id": record.get("customer_unique_id"),
                "zip_code_prefix": record.get("customer_zip_code_prefix"),
                "city": record.get("customer_city"),
                "state": record.get("customer_state"),
                "region": region_name  # Optional: indicate which region the record is from
            })

    # Return the records as a list
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    
    return records

def get_region(zip_code_prefix: int):
    """
    Determine the region based on the zip code prefix.
    """
    if 1000 <= zip_code_prefix <= 39999:
        return "region_1"
    elif 40000 <= zip_code_prefix <= 69999:
        return "region_2"
    elif 70000 <= zip_code_prefix <= 99999:
        return "region_3"

# Customer model for request validation
class Customer(BaseModel):
    customer_id: str
    customer_unique_id: str
    customer_zip_code_prefix: int
    customer_city: str
    customer_state: str

@app.post("/create-customer")
async def create_customer(customer: Customer):
    """
    Write a customer record to MongoDB.
    If the region is unavailable, log the record to Kafka and write to the alternate region.
    """
    # Determine the region based on zip code prefix
    region = get_region(customer.customer_zip_code_prefix)
    if not region:
        raise HTTPException(status_code=400, detail="Invalid zip code prefix.")

    # Check the node status
    node_status = redis_client.hget('node_statuses',region)
    kafka_message = customer.dict()  # Prepare Kafka message once
    
    print(f"Region: {region}, Node Status: {node_status}")
    
    # Check alternate node status
    alt_region = region_mapping[region]
    alt_node_status = redis_client.hget('node_statuses',alt_region)
    
    print(f"Region: {alt_region}, Node Status: {alt_node_status}")
    
    primary_collection = region_databases[region]
    alt_collection = alt_databases[region]
    
    if node_status == "unavailable":
        # Log the record to Kafka for the unavailable primary node
        producer.send(region, value=kafka_message)
        producer.flush()

        
        if alt_node_status == "unavailable":
            # Log to Kafka for alternate node
            producer.send(alt_region, value=kafka_message)
            producer.flush()
            raise HTTPException(
                status_code=503,
                detail=f"Both primary region {region} and alternate region {alt_region} are unavailable. Record logged to Kafka."
            )
        else:
            # Write to the alternate database
            alt_collection = alt_databases[region]
            try:
                await alt_collection.insert_one(kafka_message)
                return {"message": f"Region {region} is unavailable. Record logged to Kafka and written to alternate database {alt_region}."}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to write to alternate database: {str(e)}")

    # Write to primary region and replicate to alternate region if available
    elif alt_node_status == "unavailable" and node_status == "available":
        # Log to Kafka for alternate node
        producer.send(alt_region, value=kafka_message)
        producer.flush()
        try:
            # Write to primary database
            await primary_collection.insert_one(kafka_message)
            return {"message": f"Customer record successfully written to {region} and replicated to alt."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to write to database: {str(e)}")
        # raise HTTPException(
        #     status_code=503,
        #     detail=f"Primary region {region} available but, alternate region {alt_region} is unavailable. Record logged to Kafka."
        # )
    else:
        try:
            # Write to primary database
            await primary_collection.insert_one(kafka_message)

            # Replicate to alternate database
            await alt_collection.insert_one(kafka_message)
            return {"message": f"Customer record successfully written to {region} and replicated to alt."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to write to database: {str(e)}")

# Root endpoint to check the server status
@app.get("/")
async def root():
    return {"message": "FastAPI server is running!"}
