import argparse

from task2 import BloomFilter

def read_device_ids(paths):
    for path in paths:
        with open(path) as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) == 2 and parts[0] != 'device_id':
                    yield parts[0]

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('inputs', nargs='+')
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('--capacity', type=int)
    parser.add_argument('--error-rate', type=float, default=0.01)
    args = parser.parse_args(argv)

    n = args.capacity or sum(1 for _ in read_device_ids(args.inputs))
    bloom = BloomFilter.create(max(n, 1), args.error_rate)
    for device_id in read_device_ids(args.inputs):
        bloom.add(device_id)
    bloom.save(args.output)
    print(f'{args.output}: m = {bloom.m} бит, k = {bloom.k}')

if __name__ == '__main__':
    main()