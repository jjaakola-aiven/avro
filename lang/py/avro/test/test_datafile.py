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

import contextlib
import itertools
import os
import tempfile
import unittest

import avro.checksums
import avro.codecs
import avro.datafile
import avro.io
import avro.schema

CODECS_TO_VALIDATE = avro.codecs.KNOWN_CODECS.keys()
CHECKSUMS_TO_VALIDATE = avro.checksums.KNOWN_CHECKSUMS.keys()
TEST_PAIRS = tuple(
    (avro.schema.parse(schema), datum)
    for schema, datum in (
        ('"null"', None),
        ('"boolean"', True),
        ('"string"', "adsfasdf09809dsf-=adsf"),
        ('"bytes"', b"12345abcd"),
        ('"int"', 1234),
        ('"long"', 1234),
        ('"float"', 1234.0),
        ('"double"', 1234.0),
        ('{"type": "fixed", "name": "Test", "size": 1}', b"B"),
        ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', "B"),
        ('{"type": "array", "items": "long"}', [1, 3, 2]),
        ('{"type": "map", "values": "long"}', {"a": 1, "b": 3, "c": 2}),
        ('["string", "null", "long"]', None),
        (
            """\
   {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
   """,
            {"f": 5},
        ),
        (
            """\
   {"type": "record",
    "name": "Lisp",
    "fields": [{"name": "value",
                "type": ["null", "string",
                         {"type": "record",
                          "name": "Cons",
                          "fields": [{"name": "car", "type": "Lisp"},
                                     {"name": "cdr", "type": "Lisp"}]}]}]}
   """,
            {"value": {"car": {"value": "head"}, "cdr": {"value": None}}},
        ),
    )
)


@contextlib.contextmanager
def writer(path, schema=None, codec=avro.datafile.NULL_CODEC, checksum_algorithm=None, mode="wb"):
    with avro.datafile.DataFileWriter(open(path, mode), avro.io.DatumWriter(), schema, codec, checksum_algorithm) as dfw:
        yield dfw


@contextlib.contextmanager
def reader(path, mode="rb"):
    with avro.datafile.DataFileReader(open(path, mode), avro.io.DatumReader()) as dfr:
        yield dfr


class TestDataFile(unittest.TestCase):
    files = None

    def setUp(self):
        """Initialize tempfiles collection."""
        self.files = []

    def tempfile(self):
        """Generate a tempfile and register it for cleanup."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".avro") as f:
            pass
        self.files.append(f.name)
        return f.name

    def tearDown(self):
        """Clean up temporary files."""
        for f in self.files:
            os.unlink(f)

    def test_append(self):
        """A datafile can be written to, appended to, and read from."""
        for codec in CODECS_TO_VALIDATE:
            for schema, datum in TEST_PAIRS:
                # write data in binary to file once
                path = self.tempfile()
                with writer(path, schema, codec) as dfw:
                    dfw.append(datum)

                # open file, write, and close nine times
                for _ in range(9):
                    with writer(path, mode="ab+") as dfw:
                        dfw.append(datum)

                # read data in binary from file
                with reader(path) as dfr:
                    data = list(itertools.islice(dfr, 10))
                    self.assertRaises(StopIteration, next, dfr)
                self.assertEqual(len(data), 10)
                self.assertEqual(data, [datum] * 10)

    def test_round_trip(self):
        """A datafile can be written to and read from."""
        for codec in CODECS_TO_VALIDATE:
            for schema, datum in TEST_PAIRS:
                # write data in binary to file 10 times
                path = self.tempfile()
                with writer(path, schema, codec) as dfw:
                    for _ in range(10):
                        dfw.append(datum)

                # read data in binary from file
                with reader(path) as dfr:
                    data = list(itertools.islice(dfr, 10))
                    self.assertRaises(StopIteration, next, dfr)
                self.assertEqual(len(data), 10)
                self.assertEqual(data, [datum] * 10)

    def test_round_trip_with_checksum(self):
        """A datafile can be written to and read from."""
        for codec in CODECS_TO_VALIDATE:
            for checksum_algorithm in CHECKSUMS_TO_VALIDATE:
                for schema, datum in TEST_PAIRS:
                    # write data in binary to file 10 times
                    path = self.tempfile()
                    with writer(path, schema, codec, checksum_algorithm) as dfw:
                        for _ in range(10):
                            dfw.append(datum)

                    # read data in binary from file
                    with reader(path) as dfr:
                        data = list(itertools.islice(dfr, 10))
                        self.assertRaises(StopIteration, next, dfr)
                    self.assertEqual(len(data), 10)
                    self.assertEqual(data, [datum] * 10)

    def test_context_manager(self):
        """A datafile closes its buffer object when it exits a with block."""
        path = self.tempfile()
        for schema, _ in TEST_PAIRS:
            with writer(path, schema) as dfw:
                self.assertFalse(dfw.writer.closed)
            self.assertTrue(dfw.writer.closed)

            with reader(path) as dfr:
                self.assertFalse(dfr.reader.closed)
            self.assertTrue(dfr.reader.closed)

    def test_metadata(self):
        """Metadata can be written to a datafile, and read from it later."""
        path = self.tempfile()
        for schema, _ in TEST_PAIRS:
            with writer(path, schema) as dfw:
                dfw.set_meta("test.string", b"foo")
                dfw.set_meta("test.number", b"1")
            with reader(path) as dfr:
                self.assertEqual(b"foo", dfr.get_meta("test.string"))
                self.assertEqual(b"1", dfr.get_meta("test.number"))

    def test_empty_datafile_without_block_header(self):
        """A reader should not fail to read a file consisting of a single empty block."""
        path = self.tempfile()
        for schema, _ in TEST_PAIRS:
            with writer(path, schema) as dfw:
                dfw.flush()
                # Write an empty block
                dfw.encoder.write_long(0)
                dfw.encoder.write_long(0)
                dfw.writer.write(dfw.sync_marker)

    def test_empty_datafile_with_block_header(self):
        """A reader should not fail to read a file consisting of a single empty block."""
        path = self.tempfile()
        for schema, _ in TEST_PAIRS:
            with writer(path, schema) as dfw:
                dfw.flush()
                # Write the header marker and an empty block header
                dfw.encoder.write_long(avro.datafile.BLOCK_HEADER_MAGIC_MARKER)
                block_header = {"blocks": 0, "uncompressed_length": 0, "checksum_algorithm": None}
                dfw.datum_writer.write_data(avro.datafile.BLOCK_HEADER_SCHEMA, block_header, dfw.encoder)
                # Write data length
                dfw.encoder.write_long(0)
                dfw.writer.write(dfw.sync_marker)

            with reader(path) as dfr:
                self.assertRaises(StopIteration, next, dfr)

    def test_mixed_blocks_with_header_and_headerless(self):
        """The format has had for blocks only the long for number of blocks and data length.
        Newer format has header for block. As the Container File format is append format the
        file can have both kind of blocks.
        """

        def _write_headerless_block(datum):
            """This function duplicates the previous write logic."""
            # Write a block without header.
            # Write the block count.
            dfw.encoder.write_long(1)

            # Write the data to buffer, catch the length.
            dfw.datum_writer.write(datum, dfw.buffer_encoder)
            uncompressed_data = dfw.buffer_writer.getvalue()
            codec = avro.codecs.get_codec(dfw.codec)
            compressed_data, compressed_data_length = codec.compress(uncompressed_data)

            # Reset the buffer writer.
            dfw.buffer_writer.truncate(0)
            dfw.buffer_writer.seek(0)

            # Write data length.
            dfw.encoder.write_long(len(uncompressed_data))

            # Write the block of data.
            dfw.writer.write(uncompressed_data)
            # Write sync marker.
            dfw.writer.write(dfw.sync_marker)

        for schema, datum in TEST_PAIRS:
            # write data in binary to file 10 times
            path = self.tempfile()
            with writer(path, schema) as dfw:
                # Write file header.
                dfw._write_header()

                # Write the headerless block.
                _write_headerless_block(datum)

                # Write a block with header.
                dfw.append(datum)

            # Read data in binary from file.
            with reader(path) as dfr:
                data = list(itertools.islice(dfr, 2))
                self.assertRaises(StopIteration, next, dfr)
            self.assertEqual(len(data), 2)
            self.assertEqual(data, [datum] * 2)
