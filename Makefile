down:
	docker-compose down

build-ci:
	docker-compose pull motoserver > /dev/null 2>&1 &
	docker-compose build spark-server > /dev/null 2>&1 &
	docker-compose build app-tests

run-tests:
	docker-compose run --rm app-tests pytest

run-tests-bash:
	docker-compose run --rm app-tests /bin/bash
