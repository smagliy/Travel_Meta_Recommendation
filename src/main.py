import pandas as pd

from src.config.settings import (
    FILE_BIDS_NAME, HEADERS_FOR_BIDS,
    FILE_EXCHANGE_RATE_NAME, HEADERS_FOR_EXCHANGE_RATE,
    FILE_MOTELS_NAME, HEADERS_FOR_MOTELS,
    COUNTRIES_CODE, FOLDER_OUTPUT_NAME
)
from src.services.log.logger import LoggerConfig
from src.tasks.biddata_processor import BidDataProcessor
from src.tasks.currency_conversion import CurrencyProcessor
from src.tasks.load_motels import LoadMotels
from src.tasks.transform_bids import BidTransformer
from src.tasks.max_motels_processor import MaxMotelsProcessor
from src.services.read.reader import ReaderTxtFile
from src.services.write.writer import WriterParquet
from src.utils.file_helper import get_folder_path, get_absolute_root_path


logging = LoggerConfig.set_up_logger()


def get_task_dataframe(task_number: int, folder_path: str) -> pd.DataFrame:
    bids_path = f"{folder_path}/{FILE_BIDS_NAME}"
    exchange_path = f"{folder_path}/{FILE_EXCHANGE_RATE_NAME}"
    motels_path = f"{folder_path}/{FILE_MOTELS_NAME}"

    reader = ReaderTxtFile

    match task_number:
        case 1:
            logging.info("Running Task 1: Basic Bid Data Processing")
            df_bids = reader(bids_path, HEADERS_FOR_BIDS).read()
            return BidDataProcessor(df_bids).run()

        case 2:
            logging.info("Running Task 2: Currency Conversion for Selected Countries")
            df_bids = reader(bids_path, HEADERS_FOR_BIDS).read()
            df_exchange = reader(exchange_path, HEADERS_FOR_EXCHANGE_RATE).read()
            selected_countries = ['US', 'CA', 'MX']
            return BidTransformer(df_bids, df_exchange,
                                  selected_countries).run()

        case 3:
            logging.info("Running Task 3: Full Currency Conversion")
            df_bids = reader(bids_path, HEADERS_FOR_BIDS).read()
            df_exchange = reader(exchange_path, HEADERS_FOR_EXCHANGE_RATE).read()
            return CurrencyProcessor(df_bids, df_exchange, COUNTRIES_CODE).run()

        case 4:
            logging.info("Running Task 4: Join Motels with Bids")
            df_bids = reader(bids_path, HEADERS_FOR_BIDS).read()
            df_motels = reader(motels_path, HEADERS_FOR_MOTELS).read()
            field_name = 'MotelID'
            return LoadMotels(df_motels, df_bids, field_name=field_name).run()

        case 5:
            logging.info("Running Task 5: Max Bid Price per Motel")
            df_bids = reader(bids_path, HEADERS_FOR_BIDS).read()
            df_motels = reader(motels_path, HEADERS_FOR_MOTELS).read()
            return MaxMotelsProcessor(df_motels, df_bids, 'MotelID', COUNTRIES_CODE).run()

        case _:
            raise ValueError("Invalid task number. Please choose a number from 1 to 5.")


def run_task_pipeline():
    try:
        folder_path = get_folder_path()
        task_input = input("Enter task number (1-5): ").strip()

        if not task_input.isdigit() or not 1 <= int(task_input) <= 5:
            raise ValueError("Task number must be an integer between 1 and 5.")

        task_number = int(task_input)
        df_result = get_task_dataframe(task_number, folder_path)

        output_dir = f"{get_absolute_root_path()}/{FOLDER_OUTPUT_NAME}"
        output_file = f"task_{task_number}.parquet"
        WriterParquet(output_dir, output_file, df_result).write()

        logging.info(f"Task {task_number} completed successfully. Output saved to: {output_dir}/{output_file}")

    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    run_task_pipeline()

