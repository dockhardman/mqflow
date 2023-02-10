# Developing
install_all:
	poetry install --with dev

format_code:
	black .

sync_packages:
	poetry update
	poetry export --without-hashes -f requirements.txt --output requirements.txt
	poetry export --without-hashes --with dev -f requirements.txt --output requirements-test.txt

test_package:
	python -m pytest
