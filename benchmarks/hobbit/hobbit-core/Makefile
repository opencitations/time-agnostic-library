default:

start-rabbit:
	docker pull rabbitmq:3-management
	./start_test_environment.sh &

test:	start-rabbit
	mvn clean test
