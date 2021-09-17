.PHONY: server
server:
	FLASK_APP=text_search/import_from_csv.py poetry run flask run