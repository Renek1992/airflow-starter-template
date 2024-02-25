up:
	docker-compose up -d
down:
	docker-compose down
build:
	docker-compose build
cli:
	docker-compose exec airflow-webserver bash
health-check:
	curl http://localhost:8080/health | jq .