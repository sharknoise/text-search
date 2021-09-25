.PHONY: server
server:
	FLASK_APP=api/api.py poetry run flask run

requirements.txt: poetry.lock
	poetry export --format requirements.txt --output requirements.txt