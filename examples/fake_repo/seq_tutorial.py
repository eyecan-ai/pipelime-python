from pathlib import Path

import numpy as np

import pipelime.items as pli
import pipelime.sequences as pls

# Let's modify the "mini mnist" dataset by:
# 1. Keeping only the samples with even index.
# 2. Inverting the color of the images.
# 3. Adding a new item called "color" with the average image color.
# 4. Deleting the "maskinv" item.

# Create a sequence from an underfolder
this_folder = Path(__file__).parent
seq = pls.SamplesSequence.from_underfolder(this_folder / "datasets/mini_mnist")

# Keep only the even samples (POINT 1)
even_samples = seq[::2]

# Initialize an empty list of samples
new_samples = []

# Iterate on the sequence
for sample in even_samples:

    # Get the image item
    image: np.ndarray = sample["image"]()  # type: ignore

    # Invert the colors
    invimage = 255 - image

    # Replace the value of "image" with the inverted image (POINT 2)
    sample = sample.set_value("image", invimage)

    # Get the average image color
    avg_color = np.mean(invimage, axis=(0, 1))

    # Create a numpy item with the average color and add it to the sample (POINT 3)
    avg_color_item = pli.NpyNumpyItem(avg_color)
    sample = sample.set_item("avg_color", avg_color_item)

    # Delete the maskinv item (POINT 4)
    sample = sample.remove_keys("maskinv")

    # After the sample has been modified, add it to the sequence
    new_samples.append(sample)

# Create a new sequence from the list of samples
new_seq = pls.SamplesSequence.from_list(new_samples)

# Save the new sequence to an underfolder
new_seq = new_seq.to_underfolder(this_folder / "datasets/mini_mnist_inv")
new_seq.run()
