# Copyright (c) 2022 NVIDIA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import dataclasses
import io
import json
import os.path
import unittest
from unittest import mock

from swift.common.ring.io import IndexEntry, RingReader, RingWriter

from test.unit import with_tempdir


class TestRoundTrip(unittest.TestCase):
    def assertRepeats(self, data, pattern, n):
        l = len(pattern)
        self.assertEqual(len(data), n * l)
        actual = collections.Counter(
            data[x * l:(x + 1) * l]
            for x in range(n))
        self.assertEqual(actual, {pattern: n})

    @with_tempdir
    def test_write_failure(self, tempd):
        tempf = os.path.join(tempd, 'not-persisted')
        try:
            with RingWriter.open(tempf):
                self.assertEqual(1, len(os.listdir(tempd)))
                raise RuntimeError
        except RuntimeError:
            pass
        self.assertEqual(0, len(os.listdir(tempd)))

    def test_sections(self):
        buf = io.BytesIO()
        with RingWriter(buf) as writer:
            writer.write_magic(2)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef' * 10240)

            with writer.section('bar'):
                # Sometimes you might not want to get the whole section into
                # memory as a byte-string all at once (eg, when writing ring
                # assignments)
                writer.write_size(40960)
                for _ in range(10):
                    writer.write(b'\xda\x7a\xda\x7a' * 1024)

            with writer.section('baz'):
                writer.write_blob(b'more' * 10240)

                # Can't nest sections
                with self.assertRaises(ValueError):
                    with writer.section('inner'):
                        pass
                self.assertNotIn('inner', writer.index)

            writer.write(b'can add arbitrary bytes')
            # ...though accessing them on read may be difficult; see below.
            # This *is not* a recommended pattern -- write proper length-value
            # blobs instead (even if you don't include them as sections in the
            # index).

            with writer.section('quux'):
                writer.write_blob(b'data' * 10240)

            # Gotta do this at the start
            with self.assertRaises(IOError):
                writer.write_magic(2)

            # Can't write duplicate sections
            with self.assertRaises(ValueError):
                with writer.section('foo'):
                    pass

            # We're reserving globs, so we can later support something like
            # reader.load_sections('swift/ring/*')
            with self.assertRaises(ValueError):
                with writer.section('foo*'):
                    pass

        buf.seek(0)
        reader = RingReader(buf)
        self.assertEqual(reader.version, 2)
        # Order matters!
        self.assertEqual(list(reader.index), [
            'foo', 'bar', 'baz', 'quux'])
        self.assertEqual({
            k: (v.uncompressed_start, v.uncompressed_end, v.checksum_method)
            for k, v in reader.index.items()
        }, {
            'foo': (6, 40974, 'sha256'),
            'bar': (40974, 81942, 'sha256'),
            'baz': (81942, 122910, 'sha256'),
            # note the gap between baz and quux for the raw bytes
            'quux': (122933, 163901, 'sha256'),
        })

        self.assertIn('foo', reader)
        self.assertNotIn('inner', reader)

        self.assertRepeats(reader.read_section('foo'),
                           b'\xde\xad\xbe\xef', 10240)
        with reader.open_section('bar') as s:
            for _ in range(10):
                self.assertEqual(s.read(4), b'\xda\x7a\xda\x7a')
            self.assertRepeats(s.read(), b'\xda\x7a\xda\x7a', 10230)
        # If you know that one section follows another, you don't *have*
        # to "open" the next one
        self.assertRepeats(reader.read_blob(), b'more', 10240)
        self.assertRepeats(reader.read_section('quux'),
                           b'data', 10240)
        # Index is just a final (length-prefixed) JSON blob
        index_dict = json.loads(reader.read_blob())
        self.assertEqual(reader.index, {
            section: IndexEntry(*entry)
            for section, entry in index_dict.items()})

        # Missing section
        with self.assertRaises(KeyError) as caught:
            with reader.open_section('foobar'):
                pass
        self.assertEqual("'foobar'", str(caught.exception))

        # seek to the end of baz
        reader.compressed_seek(reader.index['baz'].compressed_end)
        # so we can read the raw bytes we stuffed in
        gap_length = (reader.index['quux'].uncompressed_start -
                      reader.index['baz'].uncompressed_end)
        self.assertGreater(gap_length, 0)
        self.assertEqual(b'can add arbitrary bytes',
                         reader.read(gap_length))

    def test_sections_with_corruption(self):
        buf = io.BytesIO()
        with RingWriter(buf) as writer:
            writer.write_magic(2)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef' * 10240)

        buf.seek(0)
        reader = RingReader(buf)
        # if you open a section, you better read it all!
        read_bytes = b''
        with self.assertRaises(ValueError) as caught:
            with reader.open_section('foo') as s:
                read_bytes = s.read(4)
        self.assertEqual(
            'Incomplete read; expected 40956 more bytes to be read',
            str(caught.exception))
        self.assertEqual(b'\xde\xad\xbe\xef', read_bytes)

        # if there's a digest mismatch, you can read data, but it'll
        # throw an error on close
        self.assertEqual('sha256', reader.index['foo'].checksum_method)
        self.assertEqual(
            'c51d6703d54cd7cf57b4d4b7ecfcca60'
            '56dbd41ebf1c1e83c0e8e48baeff629a',
            reader.index['foo'].checksum_value)
        reader.index['foo'] = dataclasses.replace(
            writer.index['foo'],
            checksum_value='not-the-sha',
        )
        read_bytes = b''
        with self.assertRaises(ValueError) as caught:
            with reader.open_section('foo') as s:
                read_bytes = s.read()
        self.assertIn('Hash mismatch in block: ', str(caught.exception))
        self.assertRepeats(read_bytes, b'\xde\xad\xbe\xef', 10240)

    @mock.patch('logging.getLogger')
    def test_sections_with_unsupported_checksum(self, mock_logging):
        buf = io.BytesIO()
        with RingWriter(buf) as writer:
            writer.write_magic(2)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef')
            writer.index['foo'] = dataclasses.replace(
                writer.index['foo'],
                checksum_method='not_a_digest',
                checksum_value='do not care',
            )

        buf.seek(0)
        reader = RingReader(buf)
        with self.assertRaises(ValueError):
            with reader.open_section('foo'):
                pass

    def test_recompressed(self):
        buf = io.BytesIO()
        with RingWriter(buf) as writer:
            writer.write_magic(2)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef' * 10240)

        buf.seek(0)
        reader = RingReader(buf)
        with self.assertRaises(IOError):
            reader.read(-1)  # don't be greedy
        uncompressed_bytes = reader.read(2 ** 20)

        buf = io.BytesIO()
        with RingWriter(buf) as writer:
            writer.write(uncompressed_bytes)

        buf.seek(0)
        with self.assertRaises(IOError):
            # ...but we can't read it
            RingReader(buf)

    def test_version_too_high(self):
        buf = io.BytesIO()
        with RingWriter(buf) as writer:
            # you can write it...
            writer.write_magic(3)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef' * 10240)

        buf.seek(0)
        with self.assertRaises(ValueError):
            # ...but we can't read it
            RingReader(buf)
