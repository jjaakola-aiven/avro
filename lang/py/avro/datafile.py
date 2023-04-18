#!/usr/bin/env python3

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Read/Write Avro File Object Containers.

https://avro.apache.org/docs/current/spec.html#Object+Container+Files
"""
import io
import json
import warnings
from types import TracebackType
from typing import IO, Any, AnyStr, BinaryIO, Dict, MutableMapping, Optional, Type, cast

import avro.checksums
import avro.codecs
import avro.errors
import avro.io
import avro.schema
from avro.utils import TypedDict, randbytes

VERSION = 1
MAGIC = bytes(b"Obj" + bytearray([VERSION]))
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
SYNC_INTERVAL = 4000 * SYNC_SIZE  # TODO(hammer): make configurable
META_SCHEMA = cast(
    avro.schema.RecordSchema,
    avro.schema.parse(
        json.dumps(
            {
                "type": "record",
                "name": "org.apache.avro.file.Header",
                "fields": [
                    {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": MAGIC_SIZE}},
                    {"name": "meta", "type": {"type": "map", "values": "bytes"}},
                    {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": SYNC_SIZE}},
                ],
            }
        )
    ),
)
CHECKSUM_DOC = (
    "The checksum is represented as bytes. This is required to accomodate e.g. unsigned long. CRC32 and xxhash are in the big endian byte order."
)
BLOCK_HEADER_SCHEMA = avro.schema.parse(
    json.dumps(
        {
            "type": "record",
            "name": "org.apache.avro.file.block.Header",
            "fields": [
                {"name": "blocks", "type": "long"},
                {"name": "uncompressed_length", "type": "long"},
                {"name": "checksum_algorithm", "type": ["string", "null"], "default": None},
                {"name": "checksum", "type": ["bytes", "null"], "default": None, "doc": CHECKSUM_DOC},
            ],
        }
    )
)
NULL_CODEC = "null"
VALID_CODECS = avro.codecs.KNOWN_CODECS.keys()
VALID_ENCODINGS = ["binary"]  # not used yet
VALID_CHECKSUM_ALGORITHMS = avro.checksums.KNOWN_CHECKSUMS.keys()

CODEC_KEY = "avro.codec"
SCHEMA_KEY = "avro.schema"
CHECKSUM_KEY = "avro.checksum"

# Block header marker. The marker is written where the block count was written in previous
# version of the format. The block count must be greater than 0 in the previous format.
# Therefore the marker value is selected as negative value, -1.
BLOCK_HEADER_MAGIC_MARKER = -1


class HeaderType(TypedDict):
    magic: bytes
    meta: MutableMapping[str, bytes]
    sync: bytes


class _DataFileMetadata:
    """
    Mixin for meta properties.

    Files may include arbitrary user-specified metadata.
    File metadata is written as if defined by the following map schema:

    `{"type": "map", "values": "bytes"}`

    All metadata properties that start with "avro." are reserved.
    The following file metadata properties are currently used:

    - `avro.schema` contains the schema of objects stored in the file, as JSON data (required).
    - `avro.codec`, the name of the compression codec used to compress blocks, as a string.
      Implementations are required to support the following codecs: "null" and "deflate".
      If codec is absent, it is assumed to be "null". See avro.codecs for implementation details.
    """

    __slots__ = ("_meta",)

    _meta: MutableMapping[str, bytes]

    def get_meta(self, key: str) -> Optional[bytes]:
        """Get the metadata property at `key`."""
        return self.meta.get(key)

    def set_meta(self, key: str, val: bytes) -> None:
        """Set the metadata property at `key`."""
        self.meta[key] = val

    def del_meta(self, key: str) -> None:
        """Unset the metadata property at `key`."""
        del self.meta[key]

    @property
    def meta(self) -> MutableMapping[str, bytes]:
        """Get the dictionary of metadata for this datafile."""
        if not hasattr(self, "_meta"):
            self._meta = {}
        return self._meta

    @property
    def schema(self) -> str:
        """Get the schema of objects stored in the file from the file's metadata."""
        schema_str = self.get_meta(SCHEMA_KEY)
        if schema_str:
            return schema_str.decode()
        raise avro.errors.DataFileException("Missing required schema metadata.")

    @schema.setter
    def schema(self, value: str) -> None:
        """Set the schema of objects stored in the file's metadata."""
        self.set_meta(SCHEMA_KEY, value.encode())

    @property
    def codec(self) -> str:
        """Get the file's compression codec algorithm from the file's metadata."""
        codec = self.get_meta(CODEC_KEY)
        return "null" if codec is None else codec.decode()

    @codec.setter
    def codec(self, value: str) -> None:
        """Set the file's compression codec algorithm in the file's metadata."""
        if value not in VALID_CODECS:
            raise avro.errors.DataFileException(f"Unknown codec: {value!r}")
        self.set_meta(CODEC_KEY, value.encode())

    @codec.deleter
    def codec(self) -> None:
        """Unset the file's compression codec algorithm from the file's metadata."""
        self.del_meta(CODEC_KEY)


class DataFileWriter(_DataFileMetadata):
    __slots__ = (
        "_buffer_encoder",
        "_buffer_writer",
        "_datum_writer",
        "_encoder",
        "_header_written",
        "_writer",
        "block_count",
        "sync_marker",
        "checksum_algorithm",
    )

    _buffer_encoder: avro.io.BinaryEncoder
    _buffer_writer: io.BytesIO  # BinaryIO would have better compatibility, but we use getvalue right now.
    _datum_writer: avro.io.DatumWriter
    _encoder: avro.io.BinaryEncoder
    _header_written: bool
    _writer: IO[bytes]
    block_count: int
    sync_marker: bytes
    checksum_algorithm: Optional[str]

    def __init__(
        self,
        writer: IO[AnyStr],
        datum_writer: avro.io.DatumWriter,
        writers_schema: Optional[avro.schema.Schema] = None,
        codec: str = NULL_CODEC,
        checksum_algorithm: Optional[str] = None,
    ) -> None:
        """If the schema is not present, presume we're appending."""
        if hasattr(writer, "mode") and "b" not in writer.mode:
            warnings.warn(avro.errors.AvroWarning(f"Writing binary data to a writer {writer!r} that's opened for text"))
        bytes_writer = cast(IO[bytes], getattr(writer, "buffer", writer))
        self._writer = bytes_writer
        self._encoder = avro.io.BinaryEncoder(bytes_writer)
        self._datum_writer = datum_writer
        self._buffer_writer = io.BytesIO()
        self._buffer_encoder = avro.io.BinaryEncoder(self._buffer_writer)
        self.block_count = 0
        self._header_written = False

        if writers_schema is None:
            # open writer for reading to collect metadata
            dfr = DataFileReader(writer, avro.io.DatumReader())

            # TODO(hammer): collect arbitrary metadata
            # collect metadata
            self.sync_marker = dfr.sync_marker
            self.codec = dfr.codec
            self.checksum_algorithm = dfr.checksum_algorithm

            # get schema used to write existing file
            self.schema = schema_from_file = dfr.schema
            self.datum_writer.writers_schema = avro.schema.parse(schema_from_file)

            # seek to the end of the file and prepare for writing
            writer.seek(0, 2)
            self._header_written = True
            return
        self.sync_marker = randbytes(16)
        self.codec = codec
        self.checksum_algorithm = checksum_algorithm
        self.schema = str(writers_schema)
        self.datum_writer.writers_schema = writers_schema

    @property
    def writer(self) -> IO[bytes]:
        return self._writer

    @property
    def encoder(self) -> avro.io.BinaryEncoder:
        return self._encoder

    @property
    def datum_writer(self) -> avro.io.DatumWriter:
        return self._datum_writer

    @property
    def buffer_writer(self) -> io.BytesIO:
        return self._buffer_writer

    @property
    def buffer_encoder(self) -> avro.io.BinaryEncoder:
        return self._buffer_encoder

    def _write_header(self) -> None:
        header = {"magic": MAGIC, "meta": self.meta, "sync": self.sync_marker}
        self.datum_writer.write_data(META_SCHEMA, header, self.encoder)
        self._header_written = True

    def _write_block_header(self, uncompressed_data: bytes):
        # Write number of items in block.
        # Write length of block.
        block_header: Dict[str, Any] = {
            "blocks": self.block_count,
            "uncompressed_length": len(uncompressed_data),
            "checksum_algorithm": self.checksum_algorithm,
        }

        # If checksum enabled, write checksum.
        if self.checksum_algorithm is not None:
            checksum = avro.checksums.get_checksum(self.checksum_algorithm)
            block_checksum_value = checksum.calculate(uncompressed_data)
            block_header.update(
                {
                    "checksum": block_checksum_value,
                }
            )
        self.datum_writer.write_data(BLOCK_HEADER_SCHEMA, block_header, self.encoder)

    # TODO(hammer): make a schema for blocks and use datum_writer
    def _write_block(self) -> None:
        if not self._header_written:
            self._write_header()

        if self.block_count > 0:
            # Compress block contents.
            uncompressed_data = self.buffer_writer.getvalue()
            codec = avro.codecs.get_codec(self.codec)
            compressed_data, compressed_data_length = codec.compress(uncompressed_data)

            # This is the spot where previous implementation wrote the block count.
            # On the reader this can server as a marker to read the block header or to
            # read the block as it was earlier. As the blocks must be positive for the
            # old implementation the marker shall be -1.
            self.encoder.write_long(BLOCK_HEADER_MAGIC_MARKER)

            # Write block header.
            self._write_block_header(uncompressed_data)

            # Write compressed data length.
            self.encoder.write_long(compressed_data_length)

            # Write block.
            self.writer.write(compressed_data)

            # Write sync marker.
            self.writer.write(self.sync_marker)

            # Reset buffer.
            self.buffer_writer.truncate(0)
            self.buffer_writer.seek(0)
            self.block_count = 0

    def append(self, datum: object) -> None:
        """Append a datum to the file."""
        self.datum_writer.write(datum, self.buffer_encoder)
        self.block_count += 1

        # if the data to write is larger than the sync interval, write the block
        if self.buffer_writer.tell() >= SYNC_INTERVAL:
            self._write_block()

    def sync(self) -> int:
        """
        Return the current position as a value that may be passed to
        DataFileReader.seek(long). Forces the end of the current block,
        emitting a synchronization marker.
        """
        self._write_block()
        return self.writer.tell()

    def flush(self) -> None:
        """Flush the current state of the file, including metadata."""
        self._write_block()
        self.writer.flush()

    def close(self) -> None:
        """Close the file."""
        self.flush()
        self.writer.close()

    def __enter__(self) -> "DataFileWriter":
        return self

    def __exit__(self, type_: Optional[Type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        """Perform a close if there's no exception."""
        if type_ is None:
            self.close()


class DataFileReader(_DataFileMetadata):
    """Read files written by DataFileWriter."""

    __slots__ = (
        "_datum_decoder",
        "_datum_reader",
        "_file_length",
        "_raw_decoder",
        "_reader",
        "block_count",
        "sync_marker",
        "checksum_algorithm",
    )
    _datum_decoder: Optional[avro.io.BinaryDecoder]
    _datum_reader: avro.io.DatumReader
    _file_length: int
    _raw_decoder: avro.io.BinaryDecoder
    _reader: IO[bytes]
    block_count: int
    sync_marker: bytes
    checksum_algorithm: Optional[str]

    # TODO(hammer): allow user to specify expected schema?
    # TODO(hammer): allow user to specify the encoder

    def __init__(self, reader: IO[AnyStr], datum_reader: avro.io.DatumReader) -> None:
        if hasattr(reader, "mode") and "b" not in reader.mode:
            warnings.warn(avro.errors.AvroWarning(f"Reader binary data from a reader {reader!r} that's opened for text"))
        bytes_reader = cast(IO[bytes], getattr(reader, "buffer", reader))
        self._reader = bytes_reader
        self._raw_decoder = avro.io.BinaryDecoder(bytes_reader)
        self._datum_decoder = None  # Maybe reset at every block.
        self._datum_reader = datum_reader
        self.checksum_algorithm = None

        # read the header: magic, meta, sync
        self._read_header()

        # get file length
        self._file_length = self.determine_file_length()

        # get ready to read
        self.block_count = 0
        self.datum_reader.writers_schema = avro.schema.parse(self.schema)

    def __iter__(self) -> "DataFileReader":
        return self

    @property
    def reader(self) -> IO[bytes]:
        return self._reader

    @property
    def raw_decoder(self) -> avro.io.BinaryDecoder:
        return self._raw_decoder

    @property
    def datum_decoder(self) -> Optional[avro.io.BinaryDecoder]:
        return self._datum_decoder

    @property
    def datum_reader(self) -> avro.io.DatumReader:
        return self._datum_reader

    @property
    def file_length(self) -> int:
        return self._file_length

    def determine_file_length(self) -> int:
        """
        Get file length and leave file cursor where we found it.
        """
        remember_pos = self.reader.tell()
        self.reader.seek(0, 2)
        file_length = self.reader.tell()
        self.reader.seek(remember_pos)
        return file_length

    def is_EOF(self) -> bool:
        return self.reader.tell() == self.file_length

    def _read_header(self) -> None:
        # seek to the beginning of the file to get magic block
        self.reader.seek(0, 0)

        # read header into a dict
        header = cast(HeaderType, self.datum_reader.read_data(META_SCHEMA, META_SCHEMA, self.raw_decoder))
        if header.get("magic") != MAGIC:
            raise avro.errors.AvroException(f"Not an Avro data file: {header.get('magic')!r} doesn't match {MAGIC!r}.")
        self._meta = header["meta"]
        self.sync_marker = header["sync"]

    def _read_block_header(self) -> None:
        header_marker_or_block_count = self.raw_decoder.read_long()
        if header_marker_or_block_count > BLOCK_HEADER_MAGIC_MARKER:
            # Previous format, marker contains the number of blocks.
            self.block_count = header_marker_or_block_count
            codec = avro.codecs.get_codec(self.codec)
            self._datum_decoder = codec.decompress(self.raw_decoder)
        else:
            # Current format with block header.
            block_header: Dict[str, Any] = cast(
                Dict[str, Any], self.datum_reader.read_data(BLOCK_HEADER_SCHEMA, BLOCK_HEADER_SCHEMA, self.raw_decoder)
            )
            self.block_count = block_header["blocks"]
            codec = avro.codecs.get_codec(self.codec)
            self._datum_decoder = codec.decompress(self.raw_decoder)

            if "checksum_algorithm" in block_header and block_header.get("checksum") is not None:
                pos = self._datum_decoder.reader.tell()
                data = self._datum_decoder.reader.read(cast(int, block_header.get("uncompressed_length")))
                checksum = avro.checksums.get_checksum(block_header["checksum_algorithm"])
                calculated_checksum = checksum.calculate(data)
                self._datum_decoder.reader.seek(pos)
                if block_header["checksum"] != calculated_checksum:
                    raise avro.errors.DataFileException(
                        f"DataFile block corrupted, checksum mismatch (recorded: {block_header['checksum']!r} != calculated {calculated_checksum!r})"
                    )

    def _skip_sync(self) -> bool:
        """
        Read the length of the sync marker; if it matches the sync marker,
        return True. Otherwise, seek back to where we started and return False.
        """
        proposed_sync_marker = self.reader.read(SYNC_SIZE)
        if proposed_sync_marker == self.sync_marker:
            return True
        self.reader.seek(-SYNC_SIZE, 1)
        return False

    def __next__(self) -> object:
        """Return the next datum in the file."""
        while self.block_count == 0:
            if self.is_EOF() or (self._skip_sync() and self.is_EOF()):
                raise StopIteration
            self._read_block_header()

        if self.datum_decoder is None:
            raise avro.errors.DataFileException("DataFile is not ready to read because it has no decoder")

        datum = self.datum_reader.read(self.datum_decoder)
        self.block_count -= 1
        return datum

    def close(self) -> None:
        """Close this reader."""
        self.reader.close()

    def __enter__(self) -> "DataFileReader":
        return self

    def __exit__(self, type_: Optional[Type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        """Perform a close if there's no exception."""
        if type_ is None:
            self.close()
