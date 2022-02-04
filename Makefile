create-venv:
	virtualenv .venv --python=python3.7
install:
	pip install -r requirement.txt

csv-to-utf8:
	python etl/prepare_csv.py ./resources/sb_utf16.csv ./resources/sb_utf8.csv


run-beam-text:
	python etl/run_process_text.py --runner=DirectRunner --input_file=resources/sb_utf8.csv --output_file=./output/results_sb

run-beam-df:
	python etl/run_process_dataframe.py --runner=DirectRunner --input_file=resources/sb_utf8.csv --output_file=./output/sb_result_utf8
	# python etl/run_process_dataframe.py --runner=DirectRunner --input_file=resources/sb_utf16.csv --output_file=./output/sb_result_utf16

run-beam-pd:
	python etl/run_process_pandas.py --input_file=resources/sb_utf16.csv --output_file=./output/sb_result_utf16