from pipelime.stages.base import SampleStage, StageCompose, StageIdentity, StageLambda
from pipelime.stages.augmentations import StageAlbumentations
from pipelime.stages.item_replacement import StageReplaceItem
from pipelime.stages.item_sources import StageForgetSource, StageUploadToRemote
from pipelime.stages.key_transformations import (
    StageDuplicateKey,
    StageKeyFormat,
    StageKeysFilter,
    StageRemap,
)
