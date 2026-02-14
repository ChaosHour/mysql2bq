.PHONY: build clean docker-build docker-run docker-compose-up docker-compose-down

build:
	mkdir -p ./bin
	go build -o ./bin/mysql2bq .

clean:
	rm -rf ./bin

docker-build:
	docker build -t mysql2bq .

docker-run: docker-build
	docker run -v $(PWD)/config.yaml:/app/config.yaml mysql2bq start --config /app/config.yaml

docker-compose-up:
	docker-compose up -d

docker-compose-down:
	docker-compose down

docker-compose-logs:
	docker-compose logs -f mysql2bq