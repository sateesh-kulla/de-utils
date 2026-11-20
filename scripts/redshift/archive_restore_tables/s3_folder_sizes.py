#!/usr/bin/env python3
"""List total current-object bytes for each immediate subfolder of an S3 prefix."""
import argparse
from collections import defaultdict
import csv
import sys
from urllib.parse import urlsplit


def folder_sizes(s3, bucket, prefix):
    totals = defaultdict(lambda: [0, 0])
    root_bytes = root_objects = 0
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            relative = obj['Key'][len(prefix):]
            if '/' not in relative:
                root_bytes += obj['Size']
                root_objects += 1
                continue
            folder = relative.split('/', 1)[0]
            totals[folder][0] += obj['Size']
            totals[folder][1] += 1
    return totals, root_bytes, root_objects


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('s3_uri', help='Parent path, e.g. s3://bucket/archive/dev/schema/')
    parser.add_argument('--aws-profile', help='Omit when using sdm aws run')
    parser.add_argument('--aws-region')
    parser.add_argument('--csv', dest='csv_file', help='Optionally save exact bytes and object counts to a local CSV')
    args = parser.parse_args()
    parts = urlsplit(args.s3_uri)
    if parts.scheme != 's3' or not parts.netloc or parts.query or parts.fragment:
        parser.error('Provide s3://bucket/prefix/ without query or fragment')
    prefix = parts.path.lstrip('/').rstrip('/')
    if prefix:
        prefix += '/'
    try:
        import boto3
        s3 = boto3.Session(profile_name=args.aws_profile, region_name=args.aws_region).client('s3')
        totals, root_bytes, root_objects = folder_sizes(s3, parts.netloc, prefix)
        rows = [(name, size, count) for name, (size, count) in
                sorted(totals.items(), key=lambda item: item[1][0], reverse=True)]
        print(f"{'FOLDER':<60} {'OBJECTS':>10} {'MiB':>14} {'GiB':>12}")
        for name, size, count in rows:
            print(f'{name:<60} {count:>10,} {size / 1024**2:>14,.2f} {size / 1024**3:>12,.3f}')
        if root_objects:
            print(f'[files directly in parent]: {root_objects:,} objects, {root_bytes:,} bytes')
        total_bytes = root_bytes + sum(row[1] for row in rows)
        total_objects = root_objects + sum(row[2] for row in rows)
        print(f'\nTotal: {total_bytes:,} bytes ({total_bytes / 1024**3:,.3f} GiB), {total_objects:,} objects')
        if args.csv_file:
            with open(args.csv_file, 'w', newline='') as handle:
                writer = csv.writer(handle)
                writer.writerow(['folder', 'bytes', 'objects'])
                writer.writerows(rows)
                if root_objects:
                    writer.writerow(['[files directly in parent]', root_bytes, root_objects])
            print(f'CSV saved: {args.csv_file}')
        return 0
    except Exception as exc:
        print(f'Listing failed ({type(exc).__name__}). Check AWS access, bucket/prefix, and boto3 installation.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
