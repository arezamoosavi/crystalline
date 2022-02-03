create-venv:
	virtualenv .venv --python=python3.7

csv-to-utf8:
	python etl/prepare_csv.py ./resources/sb_utf16.csv ./resources/sb_utf8.csv


run-beam:
	python etl/run_process_text.py --runner=DirectRunner --input_file=resources/sb_utf8.csv --output_file=./output/results_sb