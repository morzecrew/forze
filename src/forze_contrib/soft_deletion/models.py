from forze.domain.models import BaseDTO, Document

from .mixins import SoftDeletionMixin

# ----------------------- #


class DocWithSoftDeletion(Document, SoftDeletionMixin): ...


class UpdateCmdWithSoftDeletion(BaseDTO, SoftDeletionMixin): ...
