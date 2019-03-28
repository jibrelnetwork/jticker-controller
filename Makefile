test:
	docker-compose run --rm controller-tests

lint:
	docker-compose run --entrypoint pylama --rm controller-tests
	docker-compose run --entrypoint "mypy ." --rm controller-tests
