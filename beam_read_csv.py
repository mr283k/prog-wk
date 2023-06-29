import codecs
from _csv import QUOTE_ALL
from typing import Iterable, Dict

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.io import fileio
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems as beam_fs


class ReadCsvFiles(beam.PTransform):

    def __init__(self,
                 file_pattern: str,
                 compression_type: CompressionTypes,
                 delimiter=',',
                 quotechar='"',
                 doublequote=True,
                 skipinitialspace=False,
                 lineterminator='\n',
                 quoting=QUOTE_ALL):
        super().__init__()
        self._file_pattern = file_pattern
        self._compression_type = compression_type
        self._delimiter = delimiter
        self._quotechar = quotechar
        self._doublequote = doublequote
        self._skipinitialspace = skipinitialspace
        self._lineterminator = lineterminator
        self._quoting = quoting

    def expand(self, pbegin: beam.pvalue.PBegin) -> PCollection[Dict[str, str]]:
        return (
                pbegin
                | 'Match files' >> fileio.MatchFiles(self._file_pattern)
                | 'Read CSV lines' >> beam.FlatMap(self._read_csv_lines_as_dicts)
        )

    def _get_csv_reader(self, result_file_as_iterator):
        import csv
        return csv.DictReader(
            result_file_as_iterator,
            delimiter=self._delimiter,
            quotechar=self._quotechar,
            doublequote=self._doublequote,
            skipinitialspace=self._skipinitialspace,
            lineterminator=self._lineterminator,
            quoting=self._quoting)

    def _read_csv_lines_as_dicts(self, readable_file_metadata) -> Iterable[Dict[str, str]]:
        
        with beam_fs.open(readable_file_metadata.path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            import gzip
            if self._compression_type == CompressionTypes.UNCOMPRESSED:
                for row in self._get_csv_reader(codecs.iterdecode(f, 'utf-8')):
                    yield dict(row)
            else:
                with gzip.open(f, "rt") as gzip_text_io_wrapper:
                    for row in self._get_csv_reader(gzip_text_io_wrapper):
                        yield dict(row)
