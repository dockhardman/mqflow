# Developing
test_package:
	python -m pytest

format_code:
	black .

sync_packages:
	poetry update && poetry export --without-hashes -f requirements.txt --output requirements.txt
