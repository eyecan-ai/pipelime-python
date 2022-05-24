from pipelime.stages.base import SampleStage, StageIdentity, StageLambda, StageCompose
from pipelime.stages.augmentations import StageAlbumentations
from pipelime.stages.key_transformations import (
    StageDuplicateKey,
    StageKeyFormat,
    StageKeysFilter,
    StageRemap,
)
from pipelime.stages.item_sources import StageUploadToRemote, StageForgetSource
from pipelime.stages.item_replacement import StageReplaceItem
