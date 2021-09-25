.PHONY: server
server:
	FLASK_APP=api/api.py poetry run flask run