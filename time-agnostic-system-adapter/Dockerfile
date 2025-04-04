# Use OpenJDK 8 instead of 11 to match the project configuration
FROM openjdk:8-jre-slim

# Set working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Add the JAR file
ADD target/time-agnostic-system-adapter-*.jar /app/time-agnostic-system-adapter.jar

# Create data directory
RUN mkdir -p /versioning/data

# HOBBIT platform environment variables (will be set by the platform)
ENV HOBBIT_RABBIT_HOST=rabbit \
    HOBBIT_RABBIT_PORT=5672 \
    HOBBIT_RABBIT_USERNAME=guest \
    HOBBIT_RABBIT_PASSWORD=guest

# Add any additional dependencies for your time-agnostic system here
# For example, if your system needs other binaries or configuration files
# COPY your-system-files /app/your-system/

# Optional: Set up your system if it needs to run in the same container
# For example:
# RUN curl -L -o your-system.zip https://example.com/your-system.zip && \
#     unzip your-system.zip -d /app/your-system && \
#     rm your-system.zip

# Command to run the adapter
CMD ["java", "-Xmx2g", "-jar", "/app/time-agnostic-system-adapter.jar"] 