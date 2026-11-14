IMAGE := nsds-kafka

.PHONY: help build run stop

help:
	@echo "make build  build the $(IMAGE) image (Kafka 4.4.0)"
	@echo "make run    build, then start the broker on localhost:9092"
	@echo "make stop   stop and remove the $(IMAGE) container"
	@echo "make help   show this help"

build:
	docker build -t $(IMAGE) .

run: build
	docker run -d --name $(IMAGE) -p 9092:9092 $(IMAGE)

stop:
	docker rm -f $(IMAGE)
