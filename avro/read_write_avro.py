import json
import avro
import argparse
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter


def read_avro_data(avro_file_path):
    avro_file_reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
    for user in avro_file_reader:
        print(user)


def write_avro_date(data, schema, output_file_name):
    file_pointer = open(output_file_name, "wb")
    avro_file_writer = DataFileWriter(file_pointer, DatumWriter(), schema)
    for element in data:
        avro_file_writer.append(element)
    avro_file_writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema_file_path", help="Path of avro schema", required=True)
    parser.add_argument("--json_file_path", help="Path of JSON File data", required=True)
    parser.add_argument("--output_file_name", help="Path of Avro File to write", required=True)

    args = parser.parse_args()

    schema_file_path = args.schema_file_path
    json_file_path = args.json_file_path
    output_file_path = args.output_file_name

    # Create data from json file
    json_array = json.load(open(json_file_path))
    # Create schema from avro schema file
    writers_schema = avro.schema.parse(open(schema_file_path, "r").read())
    write_avro_date(json_array, writers_schema, output_file_path)

    read_avro_data(output_file_path)
