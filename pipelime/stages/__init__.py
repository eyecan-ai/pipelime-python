from pipelime.stages.base import SampleStage, StageIdentity, StageLambda, StageCompose
from pipelime.stages.augmentations import StageAlbumentations
from pipelime.stages.key_transformations import StageKeysFilter, StageRemap
from pipelime.stages.item_sources import StageUploadToRemote
from pipelime.stages.item_replacement import StageReplaceItem
