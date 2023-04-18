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

import abc
import zlib
from importlib.util import find_spec
from typing import Dict, Type

import avro.errors

"""
Contains Checksum functions for Python Avro.
"""


has_xxhash = False
if find_spec("xxhash"):
    import xxhash

    has_xxhash = True


class Checksum(abc.ABC):
    """Abstract base class for all Avro codec classes."""

    @staticmethod
    @abc.abstractmethod
    def calculate(data: bytes) -> bytes:
        """Calculate the checksum for the passed data.

        :param data: a byte string to calculate the checksum
        :type data: bytes

        :rype: int
        :return: checksum as integer
        """


class CRC32Checksum(Checksum):
    @staticmethod
    def calculate(data: bytes) -> bytes:
        return zlib.crc32(data).to_bytes(4, byteorder="big")


if has_xxhash:

    class XXHash64Checksum(Checksum):
        @staticmethod
        def calculate(data: bytes) -> bytes:
            return xxhash.xxh64(data).intdigest().to_bytes(8, byteorder="big")  # type: ignore[no-any-return]


KNOWN_CHECKSUMS: Dict[str, Type[Checksum]] = {
    name[: -len("Checksum")].lower(): class_
    for name, class_ in globals().items()
    if class_ != Checksum and name.endswith("Checksum") and isinstance(class_, type) and issubclass(class_, Checksum)
}


def get_checksum(checksum_algorithm: str) -> Type[Checksum]:
    try:
        return KNOWN_CHECKSUMS[checksum_algorithm]
    except KeyError:
        raise avro.errors.UnsupportedChecksum(f"Unsupported checksum: {checksum_algorithm}. (Is it installed?)")
