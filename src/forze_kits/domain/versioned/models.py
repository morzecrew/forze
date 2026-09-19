from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .mixins import CreateCmdWithVersioning, SupersedeCmdMixin, VersionedMixin

# ----------------------- #


class DocWithVersioning(Document, VersionedMixin): ...


class CreateCmdWithVersioningFields(CreateDocumentCmd, CreateCmdWithVersioning): ...


class UpdateCmdWithVersioning(BaseDTO, SupersedeCmdMixin): ...
