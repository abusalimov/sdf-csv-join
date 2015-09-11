#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
import os.path
import sys
from collections import defaultdict
from collections import namedtuple


__author__ = "Eldar Abusalimov"


def outerr(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    print(*args, **kwargs)


def parse_sdf(filename, id_prop):
    result = defaultdict(dict)

    with open(filename, 'rb') as sdfile:
        it = iter(sdfile)

        for line in it:
            if line[0] != '>':
                continue

            header = line.split()
            prop = header[1][1:-1]
            try:
                mol = header[2][1:-1]
            except IndexError:
                mol = None

            value = next(it).rstrip()

            props = result[mol]
            if prop in props:
                raise ValueError('Conflicting property name: {}'.format(prop))
            props[prop] = value

            next(it)  # skip newline

    outerr('checking...')
    for mol, props in result.iteritems():
        if props.get(id_prop) != mol:
            raise ValueError('ID mismatch: {} != {}'
                             .format(props.get(id_prop), mol))

    return dict(result)


def read_csv(filename, prop_names):
    result = {}

    id_prop = prop_names[0]
    with open(filename, 'rb') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        reader = csv.DictReader(csvfile, dialect=dialect)
        if reader.fieldnames and id_prop not in reader.fieldnames:
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, prop_names, dialect=dialect)
        for row in reader:
            mol = row[id_prop]
            result[mol] = row

    return result


def join_results(haystack, needle, row_type, include_none=False):
    result = dict()

    for mol in needle:
        try:
            props = haystack[mol]
        except KeyError:
            if include_none:
                result[mol] = None
        else:
            result[mol] = row_type(**dict((prop, props.get(prop, ''))
                                          for prop in row_type._prop_names))

    return result


def write_csv(filename, table, row_type=None):
    with open(filename, 'wb') as csvfile:
        writer = csv.writer(csvfile)

        if row_type is not None:
            writer.writerow(row_type._prop_names)
        writer.writerows(table.itervalues())


def print_table(table, row_type):
    header = row_type._prop_names
    rows = list(table.itervalues())

    col_width = [max(len(x) for x in col) for col in zip(header, *rows)]

    print('  '.join('{:>{}}'.format(r, w) for r, w in zip(header, col_width)))
    for row in rows:
        assert isinstance(row, row_type)
        print('  '.join('{:>{}}'.format(r, w) for r, w in zip(row, col_width)))


def create_row_type(prop_names, id_prop=None):
    if isinstance(prop_names, basestring):
        prop_names = prop_names.replace(',', ' ').split()
    prop_names = map(str, prop_names)

    if id_prop is not None:
        prop_names = [id_prop] + [n for n in prop_names if n != id_prop]

    row_type = namedtuple('Row', prop_names, rename=True)
    row_type._prop_names = row_type._make(prop_names)

    return row_type


def read_input_file(filename, prop_names):
    ext = os.path.splitext(filename)[1].lower()

    outerr('{}: reading...'.format(filename))

    if ext == '.csv':
        ret = read_csv(filename, prop_names)
    else:
        if ext != '.sdf':
            outerr('{}: will be treated as SDF'.format(filename),
                  file=sys.stderr)
        ret = parse_sdf(filename, id_prop=prop_names[0])

    outerr('done: {} molecules'.format(len(ret)))

    return ret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output',
                        help='CSV output file')
    parser.add_argument('-i', '--id', type=str, default='ID',
                        help='ID property name')
    parser.add_argument('-p', '--props', type=str, default='',
                        help='Properties to extract from the haystack')
    parser.add_argument('haystack_file',
                        help='SDF or CSV file to search in')
    parser.add_argument('needle_files', nargs='+',
                        help='SDF or CSV file with data to search for')
    args = parser.parse_args()

    row_type = create_row_type(args.props, id_prop=args.id)

    haystack = read_input_file(args.haystack_file, row_type._prop_names)

    needle = set()
    for needle_file in args.needle_files:
        needle |= set(read_input_file(needle_file, row_type._prop_names))

    result = join_results(haystack, needle, row_type)

    print_table(result, row_type)
    if args.output is not None:
        write_csv(args.output, result, row_type)

if __name__ == '__main__':
    main()
