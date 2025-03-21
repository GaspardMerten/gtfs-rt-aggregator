import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..utils.log_helper import setup_logger


class ParquetSerializer:
    """Class for serializing and deserializing Parquet data."""

    logger = setup_logger(f"{__name__}.ParquetSerializer")

    @staticmethod
    def dataframe_to_parquet_bytes(
        df: pd.DataFrame, compression: str = "snappy"
    ) -> bytes:
        """
        Convert a DataFrame to Parquet bytes.

        @param df: DataFrame to convert
        @param compression: Compression to use (default: snappy)
        @return Parquet bytes
        """
        logger = ParquetSerializer.logger

        if df.empty:
            logger.debug("Empty DataFrame provided, returning empty bytes")
            return b""

        try:
            logger.debug(
                f"Converting DataFrame with {len(df)} rows to Parquet bytes with {compression} compression"
            )

            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df=df, columns=df.columns)
            # Write to bytes buffer
            buffer = io.BytesIO()

            pq.write_table(table, buffer, compression=compression, use_dictionary=True)

            # Get bytes
            buffer.seek(0)
            result = buffer.getvalue()
            logger.debug(f"Successfully converted DataFrame to {len(result)} bytes")
            return result
        except Exception as e:
            logger.error(
                f"Error converting DataFrame to Parquet: {str(e)}", exc_info=True
            )
            raise

    @staticmethod
    def pyarrow_table_to_bytes(table: pa.Table, compression: str = "gzip") -> bytes:
        """
        Convert a PyArrow Table to bytes.

        @param table: PyArrow Table to convert
        @param compression: Compression to use (default: gzip)
        @return Bytes
        """
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=compression, use_dictionary=True)

        # Get bytes
        buffer.seek(0)

        return buffer.getvalue()
