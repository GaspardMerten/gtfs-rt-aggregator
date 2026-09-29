import io

import pyarrow as pa
import pyarrow.parquet as pq


class ParquetSerializer:
    """Class for serializing and deserializing Parquet data."""

    @staticmethod
    def pyarrow_table_to_bytes(table: pa.Table, compression: str = "brotli") -> bytes:
        """
        Convert a PyArrow Table to Parquet bytes.

        @param table: PyArrow Table to convert (its schema metadata is kept)
        @param compression: Compression to use (default: brotli)
        @return Bytes
        """
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=compression)
        return buffer.getvalue()
