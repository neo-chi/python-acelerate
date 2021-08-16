readme:
	asciidoctor-pdf -o ~/Desktop/README.pdf README.adoc
test:
	pytest -vv --cov
run:
	python3 src/acelerate/main.py
