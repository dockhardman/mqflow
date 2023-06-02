# Developing
install_all:
	poetry install --with dev -E all

format_code:
	black .

update_packages:
	poetry update
	poetry export --without-hashes -f requirements.txt --output requirements.txt
	poetry export --without-hashes -E all --with dev -f requirements.txt --output requirements-test.txt

test_package:
	python -m pytest
