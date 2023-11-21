import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging


class FileProcessor:
    def __init__(self, dir_in, dir_out, dir_save):
        '''

        :param dir_in: source directory
        :param dir_out: final directory
        :param dir_save: intermediate directory
        '''
        # Constructor to initialize directory paths and logger
        self.dir_in = dir_in
        self.dir_out = dir_out
        self.dir_save = dir_save
        self.logger = self.setup_logger()

    def setup_logger(self):
        """

        :return: logger
        """
        # Initialize logger
        logger = logging.getLogger('FileProcessor')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def get_file_names(self):
        """

        :return: list of file names
        """
        # Get a list of file names in the specified directory
        command = f'hdfs dfs -ls {self.dir_in}'
        try:
            output = subprocess.check_output(command, shell=True)
            files = output.decode().split('\n')
            files_name = []
            for path in files:
                if path.endswith('.csv') or path.endswith('.csv.gz'):
                    files_name.append(path.split('/')[-1])
            return files_name
        except subprocess.CalledProcessError as e:
            self.logger.error(f'Ошибка выполнения команды {e}', exc_info=True)
            return []

    def change_file(self, csv_file, column_value, spark):
        '''
        Add a column and fill it with the specified value
        :param self:
        :param csv_file: file to process
        :param column_value: value to add to the column
        :param spark: SparkSession object for reading the file
        :return:
        '''

        df_csv = spark.read.csv(csv_file, header=True, sep=',', inferSchema=True)
        df_csv = df_csv.withColumn('dispatcher_id', lit(column_value))
        df_csv.coalesce(1) \
            .write \
            .option('sep', ',') \
            .mode('overwrite') \
            .option('header', True) \
            .option('compression', 'gzip') \
            .csv(self.dir_save)


