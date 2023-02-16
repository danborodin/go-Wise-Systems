
start:
	docker compose up zookeeper -d
	docker compose up kafka -d
	sleep 10s
	docker compose up producer -d
	docker compose up consumer

stop:
	docker compose down