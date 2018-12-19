
clean:
	docker ps -a | grep node_status | awk '{print $$1}' | xargs docker rm
	docker images -a | grep node_status | awk '{print $$3}' | xargs docker image rm
	-docker images -a | grep '<none>' | awk '{print $$3}' | xargs docker image rm
	-docker images -a | grep '<none>' | awk '{print $$3}' | xargs docker image rm
	yes | docker volume prune
	yes | docker network prune

build:
	docker-compose build

dev:
	docker-compose up

prod:
	docker-compose -f docker-compose.prod.yml up -d
