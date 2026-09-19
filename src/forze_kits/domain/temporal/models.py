from forze.domain.models import CreateDocumentCmd, Document

from .mixins import CreateCmdWithTemporal, TemporalMixin

# ----------------------- #


class DocWithTemporal(Document, TemporalMixin): ...


class CreateCmdWithTemporalFields(CreateDocumentCmd, CreateCmdWithTemporal): ...
