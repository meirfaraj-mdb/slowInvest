import os
import sys
from config import Config

def get_file_size(filepath):
    """Returns the file size in bytes."""
    return os.path.getsize(filepath)
def append_until_target_size(input_file_path, output_file_path, target_size_bytes=60*1024**3):
    # Get the sizes of the input and output files
    input_size = get_file_size(input_file_path)
    output_size = get_file_size(output_file_path) if os.path.exists(output_file_path) else 0
    # Calculate the number of times to repeat the input file
    remaining_space = target_size_bytes - output_size
    num_iterations = remaining_space // input_size
    if num_iterations == 0:
        print("Target size is already met or cannot accommodate another iteration of the input file.")
        return
    # Append the input file to the output file the calculated number of times
    with open(input_file_path, 'rb') as input_file:
        input_data = input_file.read()
    with open(output_file_path, 'ab') as output_file:
        for _ in range(int(num_iterations)):
            output_file.write(input_data)
    print(f"Successfully appended {num_iterations} iterations of {input_file_path} to {output_file_path}.")

if __name__ == "__main__":
    first_option = sys.argv[1] if len(sys.argv) > 1 else None
    # Use the first_option in your Config class or elsewhere
    config = Config(first_option)
    if config.RETRIEVAL_MODE == "files":
        for file in config.LOGS_FILENAME:
            source_file = f"{config.INPUT_PATH}/{file}"
            destination_file = f"{config.INPUT_PATH}/60g_{file}"
            append_until_target_size(source_file, destination_file)
            print(f"Data successfully concatenated into {destination_file}")
