from typing import Any

from forze.application.composition.storage.facades import StorageDTOs
from forze.application.execution import UsecaseRegistry
from forze.application.mapping import DTOMapper, MappingStep
from forze.application.usecases.storage import (
    DeleteObject,
    DeleteObjectArgs,
    DownloadObject,
    DownloadObjectArgs,
    ListObjects,
    ListObjectsArgs,
    UploadObject,
    UploadObjectArgs,
)

from .operations import StorageOperation

# ----------------------- #


def build_storage_upload_mapper(
    dtos: StorageDTOs[Any, Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for upload requests."""

    mapper = DTOMapper(in_=dtos.upload or UploadObjectArgs, out=UploadObjectArgs)
    return mapper.with_steps(*steps)


# ....................... #


def build_storage_list_mapper(
    dtos: StorageDTOs[Any, Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for list requests."""

    mapper = DTOMapper(in_=dtos.list or ListObjectsArgs, out=ListObjectsArgs)
    return mapper.with_steps(*steps)


# ....................... #


def build_storage_download_mapper(
    dtos: StorageDTOs[Any, Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for download requests."""

    mapper = DTOMapper(in_=dtos.download or DownloadObjectArgs, out=DownloadObjectArgs)
    return mapper.with_steps(*steps)


# ....................... #


def build_storage_delete_mapper(
    dtos: StorageDTOs[Any, Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for delete requests."""

    mapper = DTOMapper(in_=dtos.delete or DeleteObjectArgs, out=DeleteObjectArgs)
    return mapper.with_steps(*steps)


# ....................... #


def build_storage_registry(
    bucket: str,
    dtos: StorageDTOs[Any, Any, Any, Any],
    *,
    upload_steps: tuple[MappingStep[Any], ...] = (),
    list_steps: tuple[MappingStep[Any], ...] = (),
    download_steps: tuple[MappingStep[Any], ...] = (),
    delete_steps: tuple[MappingStep[Any], ...] = (),
) -> UsecaseRegistry:
    """Build a usecase registry for storage operations in a bucket."""

    upload_mapper = build_storage_upload_mapper(dtos, steps=upload_steps)
    list_mapper = build_storage_list_mapper(dtos, steps=list_steps)
    download_mapper = build_storage_download_mapper(dtos, steps=download_steps)
    delete_mapper = build_storage_delete_mapper(dtos, steps=delete_steps)

    return UsecaseRegistry(
        {
            StorageOperation.UPLOAD: lambda ctx: UploadObject(
                ctx=ctx,
                storage=ctx.storage(bucket),
                mapper=upload_mapper,
            ),
            StorageOperation.LIST: lambda ctx: ListObjects(
                ctx=ctx,
                storage=ctx.storage(bucket),
                mapper=list_mapper,
            ),
            StorageOperation.DOWNLOAD: lambda ctx: DownloadObject(
                ctx=ctx,
                storage=ctx.storage(bucket),
                mapper=download_mapper,
            ),
            StorageOperation.DELETE: lambda ctx: DeleteObject(
                ctx=ctx,
                storage=ctx.storage(bucket),
                mapper=delete_mapper,
            ),
        }
    )
