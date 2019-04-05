test:
	docker-compose run --rm tests

lint:
	docker-compose run --entrypoint pylama --rm tests
	docker-compose run --entrypoint "mypy ." --rm tests
