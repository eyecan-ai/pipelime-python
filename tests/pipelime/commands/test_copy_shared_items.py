import numpy as np

import pipelime.stages as plst

from .test_general_base import TestGeneralCommandsBase


class TestSetCopySharedItemsCommand(TestGeneralCommandsBase):
    def test_set_meta(self, minimnist_dataset, tmp_path):
        from pipelime.commands import CopySharedItemsCommand
        from pipelime.sequences import SamplesSequence

        # Paths
        source_path = minimnist_dataset["path"]
        dest_path = tmp_path / "minimnist_copy_shared_items" / "dest"
        output_path = tmp_path / "minimnist_copy_shared_items" / "output"

        # Remove a samples and items from the dataset to make it a suitable dest seq
        dest_seq = SamplesSequence.from_underfolder(source_path)
        dest_seq = dest_seq.slice(start=0, stop=10, step=2)
        dest_seq = dest_seq.map(
            plst.StageKeysFilter(key_list=["mask", "metadata", "numbers"], negate=True)
        )
        dest_seq.to_underfolder(dest_path).run()

        # Transfer numbers.txt from source to dest
        CopySharedItemsCommand(
            source=source_path,
            dest=dest_path,
            output=output_path,
            k=["numbers"],
        ).run()

        # Check that the numbers.txt file was copied
        source_seq = SamplesSequence.from_underfolder(source_path)
        out_seq = SamplesSequence.from_underfolder(output_path)

        # Must have the same number of samples
        assert len(out_seq) == len(dest_seq)

        # Every sample should be the same, but with the numbers item
        for out_sample, dest_sample in zip(out_seq, dest_seq):
            # Numbers must always be present and equal to the first source numbers
            assert (out_sample["numbers"]() == source_seq[0]["numbers"]()).all()

            # Numbers must be shared
            assert out_sample["numbers"].is_shared

            # All other items must be the same as the dest sample
            for k in dest_sample.keys():
                assert k in out_sample.keys()
                assert out_sample[k].is_shared == dest_sample[k].is_shared
                eq = out_sample[k]() == dest_sample[k]()
                if isinstance(eq, np.ndarray):
                    eq = eq.all()
                assert eq
