import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4
from datetime import datetime, UTC

class Document:
    def __init__(self, id, rev, created_at, last_update_at, is_deleted=False, **kwargs):
        self.id = id
        self.rev = rev
        self.created_at = created_at
        self.last_update_at = last_update_at
        self.is_deleted = is_deleted
        for k, v in kwargs.items():
            setattr(self, k, v)

    def update(self, data):
        return self, data

    def touch(self):
        return self, {"last_update_at": datetime.now(tz=UTC)}

    def model_copy(self, update=None, deep=True):
        data = vars(self).copy()
        if update:
            data.update(update)
        return Document(**data)

class MongoWriteGateway:
    def __init__(self, read, client):
        self.read = read
        self.client = client
        self.history = None
        self.rev_bump_strategy = "application"

    def _storage_pk(self, pk): return str(pk)
    def _coerce_query_value(self, val): return val
    def coll(self): return "coll"

    def __bump_rev(self, current, diff):
        diff["rev"] = current.rev + 1
        return diff

    async def _write_history(self, *data): pass

    async def _validate_history(self, *data): pass

    async def _patch_many(self, pks, updates=None, revs=None):
        if not pks: return []
        currents = await self.read.get_many(pks)
        to_patch = []
        if updates is not None:
            if revs is not None:
                await self._validate_history(*[(c, r, u) for c, r, u in zip(currents, revs, updates, strict=True)])
            for i, (current, update) in enumerate(zip(currents, updates, strict=True)):
                _, diff = current.update(update)
                if diff: to_patch.append((i, current, diff))
        else:
            for i, current in enumerate(currents):
                _, diff = current.touch()
                if diff: to_patch.append((i, current, diff))
        if not to_patch: return currents
        async def do_update(idx, current, diff):
            bumped = self.__bump_rev(current, diff)
            matched = await self.client.update_one(self.coll(), {"_id": self._storage_pk(current.id), "rev": current.rev}, {"$set": self._coerce_query_value(bumped)})
            if matched != 1: raise Exception("Failed")
            return idx, current.model_copy(update=bumped, deep=True)
        results = await asyncio.gather(*(do_update(i, c, d) for i, c, d in to_patch))
        updated_map = dict(results)
        out = [updated_map.get(i, currents[i]) for i in range(len(currents))]
        await self._write_history(*(m for _, m in results))
        return out

    async def update_many(self, pks, dtos, revs=None):
        updates = [{"name": d} for d in dtos]
        return await self._patch_many(pks, updates, revs=revs)

async def test_batch_optimization():
    pks = [uuid4() for _ in range(3)]
    docs = [Document(id=pk, rev=1, created_at=datetime.now(tz=UTC), last_update_at=datetime.now(tz=UTC)) for pk in pks]
    read = MagicMock()
    read.get_many = AsyncMock(return_value=docs)
    client = MagicMock()
    client.update_one = AsyncMock(return_value=1)
    gw = MongoWriteGateway(read, client)
    results = await gw.update_many(pks, ["a", "b", "c"])
    assert len(results) == 3
    assert all(r.rev == 2 for r in results)
    assert all(r.name in ["a", "b", "c"] for r in results)
    read.get_many.assert_called_once_with(pks)
    assert client.update_one.call_count == 3
    print("Verification successful: Batch optimization works as expected.")

if __name__ == "__main__":
    asyncio.run(test_batch_optimization())
