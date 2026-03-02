"""Application usecases for document and storage operations.

Usecases encapsulate business workflows and delegate to ports (document, storage,
etc.). Each usecase extends :class:`forze.application.execution.Usecase` and
implements :meth:`main`. Document usecases live in :mod:`document`; storage
operation identifiers in :mod:`storage`.
"""
