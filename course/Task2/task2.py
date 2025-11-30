
import hashlib
import os

directory = r"C:\Users\SanzharSabyr\Desktop\FP\python\course\Task2(files)"


def product_of_digits_plus_one(hex_str):
    product = 1
    for ch in hex_str:
        digit = int(ch, 16)
        product *= (digit + 1)
    return product

file_hashes = []
for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as f:
            file_data = f.read()
            sha3_hash = hashlib.sha3_256(file_data).hexdigest()
            file_hashes.append(sha3_hash)

file_hashes.sort(key=product_of_digits_plus_one)

joined_hashes = ''.join(file_hashes)

final_hash = hashlib.sha3_256(joined_hashes.encode('utf-8')).hexdigest()

print(final_hash)
