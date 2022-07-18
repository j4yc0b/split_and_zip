import os
import pandas as pd
import argparse
from pandas.errors import ParserError


def _get_size_MB(file_path):
    """
    :param file_path: Path of the file
    :return: Size of the file in MB
    """
    return os.path.getsize(file_path) / (1024.0 * 1024.0)


class Splitter:
    """ Class for handling the zipping and splitting of the file
    Takes in the max_size argument from the command line
    :param file_name: The file path of the csv-file.
    :param max_file_size: The maximum size of the desired output file(s).
    :param csv_delimiter: Delimiter of the csv file.
    """

    def __init__(self, file_name, max_file_size, csv_delimiter):
        self.file_name = file_name
        self.max_file_size = max_file_size
        self.csv_delimiter = csv_delimiter

    def main(self):
        """
        Reading in the data as a Dataframe, writing it in to a .zip file
        and checking the size of the zip
        :return: The provided csv file in as many zip files as determined by the set maximum file size. 
        """
        file_name = self.file_name
        if os.path.exists(file_name):
            try:
                df = pd.read_csv(file_name, sep=self.csv_delimiter)
            except ParserError:
                raise Exception("Failed to read in the csv data. Please provide the correct delimiter.")
        else:
            raise Exception(F"Given argument {args.file} is not a valid path.")

        compression_options = dict(method="zip",
                                   archive_name='test.csv')
        df.to_csv(f"{os.path.splitext(file_name)[0]}.zip",
                  index=False,
                  sep=self.csv_delimiter,
                  decimal=".",
                  compression=compression_options)

        file_size = _get_size_MB(f"{os.path.splitext(file_name)[0]}.zip")
        if file_size > self.max_file_size:
            print(f"\nZipped size of file is {round(file_size)} MB, which is greater than the "
                  f"max file size of {self.max_file_size}MB. Continuing with splitting the file.")
            self._split_file(file_name, file_size, df)
        else:
            return print(f"\nZipped size of file is {round(file_size)} MB, which is smaller than the "
                  f"max file size of {self.max_file_size}MB. No need to split.")

    def split_file(self, full_file_path, file_size, df):
        """

        :param full_file_path: The file path.
        :param file_size: The size of the file.
        :param df: Dataframe of the csv file.
        :return: The provided csv file in as many zip files as determined by the set maximum file size.
        """
        nbr_of_files = int(file_size) // self.max_file_size + 1
        nbr_of_records = df.shape[0]
        records_per_file = nbr_of_records // nbr_of_files + 1

        for n in range(1, nbr_of_files + 1):
            df_from = (n - 1) * records_per_file
            df_to = n * records_per_file

            df.iloc[df_from:df_to].to_csv(
                f"{os.path.splitext(full_file_path)[0]}_{str(n)}.csv",
                index=False,
                sep=self.csv_delimiter,
                decimal="."
            )

            compression_options = dict(method="zip",
                                       archive_name=os.path.split(full_file_path)[1])

            df.iloc[df_from:df_to].to_csv(
                f"{os.path.splitext(full_file_path)[0]}_{str(n)}.zip",
                index=False,
                sep=self.csv_delimiter,
                decimal=".",
                compression=compression_options
            )

            return print(f"The file has been splitted into {nbr_of_files} parts.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_name", help="Provide the filename.")
    parser.add_argument("max_file_size", type=float,
                        help="Provide the maximum filesize in MB. Files larger than this will be splitted into multiple parts.")

    parser.add_argument("--csv_delimiter", help="Provide the delimiter of the csv file (defaults to ';').",
                        type=str, default=";")
    args = parser.parse_args()

    fw = Splitter(args.file_name, args.max_file_size, args.csv_delimiter)
    fw.main()


