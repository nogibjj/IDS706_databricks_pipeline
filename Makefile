install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

format:	
	black . --line-length 100 --verbose

lint:
	ruff check src/ --fix --verbose