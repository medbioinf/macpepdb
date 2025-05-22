.SILENT:

docker-compose-up:
	docker compose -f docker-compose.yml -f monitoring.docker-compose.yml up

docker-compose-up:
	docker compose -f docker-compose.yml -f monitoring.docker-compose.yml down
