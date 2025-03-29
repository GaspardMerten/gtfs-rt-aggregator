import io

import pyarrow as pa
import pyarrow.parquet as pq

from ..utils.log_helper import setup_logger


class ParquetSerializer:
    """Class for serializing and deserializing Parquet data."""

    logger = setup_logger(f"{__name__}.ParquetSerializer")

    @staticmethod
    def pyarrow_table_to_bytes(table: pa.Table, compression: str = "brotli") -> bytes:
        """
        Convert a PyArrow Table to bytes.

        @param table: PyArrow Table to convert
        @param compression: Compression to use (default: gzip)
        @return Bytes
        """
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=compression,
            use_dictionary=True,
            dictionary_pagesize_limit=5000,
        )

        # Get bytes
        buffer.seek(0)

        return buffer.getvalue()
