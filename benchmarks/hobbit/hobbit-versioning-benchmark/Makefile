default: build dockerize-all

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize-all: dockerize-controller dockerize-datagen dockerize-taskgen dockerize-evalmodule dockerize-virtuoso-baseline
	
dockerize-controller:
	docker build -f docker/versioningbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:$(tag) .
	docker push git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller:$(tag)
		
dockerize-datagen:
	docker build -f docker/versioningdatagenerator.docker -t git.project-hobbit.eu:4567/papv/versioningdatagenerator:$(tag) .
	docker push git.project-hobbit.eu:4567/papv/versioningdatagenerator:$(tag)
	
dockerize-taskgen:
	docker build -f docker/versioningtaskgenerator.docker -t git.project-hobbit.eu:4567/papv/versioningtaskgenerator:$(tag) .
	docker push git.project-hobbit.eu:4567/papv/versioningtaskgenerator:$(tag)

dockerize-evalmodule:
	docker build -f docker/versioningevaluationmodule.docker -t git.project-hobbit.eu:4567/papv/versioningevaluationmodule:$(tag) .
	docker push git.project-hobbit.eu:4567/papv/versioningevaluationmodule:$(tag)

dockerize-virtuoso-baseline:
	docker build -f docker/versioningvirtuososystemadapter.docker -t git.project-hobbit.eu:4567/papv/systems/versioningsystem:$(tag) .
	docker push git.project-hobbit.eu:4567/papv/systems/versioningsystem:$(tag)
