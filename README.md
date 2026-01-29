# Real-Time Streaming Pipeline

## Setup
1. cp .env.example .env
2. docker-compose up -d
3. python producer.py  # Generate data
4. docker-compose logs spark-app  # Monitor
5. docker exec -it db psql -U user stream_data  # Query tables

## Verification
- Kafka: kafka-console-consumer --bootstrap-server localhost:29092 --topic user_activity
- Parquet: ./spark/data/lake/event_date=...
- Late data auto-dropped by watermark.
"# Data-Pipeline" 
