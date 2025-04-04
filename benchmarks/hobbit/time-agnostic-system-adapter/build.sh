#!/bin/bash

# Default options
clean_cache=false
build_deps=false
build_docker=false

# Show help
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "Build options for the Time-Agnostic System Adapter"
    echo ""
    echo "  -h, --help       Show this help message"
    echo "  -c, --clean      Clean Maven cache before building"
    echo "  -d, --deps       Build dependencies before building this project"
    echo "  --docker         Build Docker image after successful build"
    echo ""
    echo "Examples:"
    echo "  $0               Standard build"
    echo "  $0 -c            Clean Maven cache then build"
    echo "  $0 -d            Build dependencies then this project"
    echo "  $0 -cd --docker  Clean, build dependencies, build project, create Docker image"
}

# Parse command-line options
while [ "$1" != "" ]; do
    case $1 in
        -c | --clean )      clean_cache=true
                            ;;
        -d | --deps )       build_deps=true
                            ;;
        --docker )          build_docker=true
                            ;;
        -h | --help )       show_help
                            exit 0
                            ;;
        * )                 echo "Unknown option: $1"
                            show_help
                            exit 1
    esac
    shift
done

# Clean Maven cache if requested
if [ "$clean_cache" = true ]; then
    echo "Cleaning Maven cache..."
    rm -rf ~/.m2/repository/central-mirror
    rm -rf ~/.m2/repository/org/hobbit
    rm -rf ~/.m2/repository/org/eclipse/rdf4j
    rm -rf ~/.m2/repository/org/apache/jena
    rm -rf ~/.m2/repository/commons-io
    rm -rf ~/.m2/repository/org/slf4j
    rm -rf ~/.m2/repository/junit
    rm -rf ~/.m2/repository/org/apache/maven/plugins
    echo "Maven cache cleaned."
fi

# Build dependencies if requested
if [ "$build_deps" = true ]; then
    echo "Building hobbit-core dependency..."
    cd ../benchmarks/hobbit-core/ || { echo "Error: hobbit-core directory not found"; exit 1; }
    mvn clean install -DskipTests
    if [ $? -ne 0 ]; then
        echo "Failed to build hobbit-core dependency!"
        exit 1
    fi
    cd - || exit 1
    echo "Successfully built hobbit-core dependency."
fi

# Build the Java project
echo "Building Java project..."
mvn clean package -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true

# Check if the build was successful
if [ $? -ne 0 ]; then
    echo "Maven build failed!"
    exit 1
fi

# Build Docker image if requested
if [ "$build_docker" = true ]; then
    echo "Building Docker image..."
    docker build -t time-agnostic-system-adapter .
    
    if [ $? -ne 0 ]; then
        echo "Docker build failed!"
        exit 1
    fi
    
    echo "Docker image built successfully."
    echo "You can now run the adapter with: docker run -it time-agnostic-system-adapter"
fi

echo "Build completed successfully!" 