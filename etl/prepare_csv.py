import sys


def main(input_file, output_file):
    with open(input_file, 'rb') as source_file:
        with open(output_file, 'w+b') as dest_file:
            contents = source_file.read()
            dest_file.write(contents.decode('utf-16').encode('utf-8'))


if __name__ == "__main__":

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)
