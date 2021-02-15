import pandas as pd
import os


input_dir = "input"
output_dir = "output"

# Create output directory if not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def converter(files, id=input_dir, od=output_dir):
    '''Convert parquet files to the JSON

    :param files: List of the parquet files
    :param id: Input directory
    :param od: Output directory
    '''

    for file in files:
        # Get the name of the file
        name = file.split(".parquet")[0]

        # Read parquet file
        df = pd.read_parquet(id + "/" + file)

        # Write parquet filr to the json
        df.to_json(od + "/" + name + ".json")

        # Print output message
        print("[%s] Done" % name)


if __name__ == '__main__':
    # Get list of the files
    list_of_files = os.listdir(input_dir)

    # Convert files
    converter(list_of_files)
