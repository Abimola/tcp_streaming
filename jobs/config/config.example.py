config = {
    "openai": {
        "api_key": "YOUR_OPENAI_PROJECT_API_KEY_HERE"
    },
    "kafka": {
        "sasl.username": "YOUR_KAFKA_SASL_USERNAME_HERE",
        "sasl.password": "YOUR_KAFKA_SASL_PASSWORD_HERE",
        "bootstrap.servers": "YOUR_KAFKA_CLUSTER_ADDRESS_HERE",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "session.timeout.ms": 50000
    }
}
