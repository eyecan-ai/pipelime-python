import pipelime.sequences as pls
import pipelime.items as pli


# Create a sequence from an underfolder
seq = pls.SamplesSequence.from_underfolder("datasets/mini_mnist")

# Get the length of the sequence
print(len(seq))
# >>> 20

# Slice a sequence to get a new subsequence
subseq = seq[4:10:2]
print(len(subseq))
# >>> 3

# Access a specific sample
sample_7 = seq[7]
print(len(sample_7))
# >>> 8

print(list(sample_7.keys()))
# >>> ['common', 'numbers', 'label', 'maskinv', 'metadata', 'points', 'mask', 'image']

# Access a specific item
image_item = sample_7["image"]
print(image_item)
