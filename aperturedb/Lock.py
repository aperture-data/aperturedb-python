"""
Locking objects in ApertureDB

This module provides a context manager for locking objects in ApertureDB.  This is a mutex-type lock, and only guards against other accesses that also use this locking class.

Example:

```python
from aperturedb.Lock import Lock

with Lock(db, query) as lock:
    # Do something with the locked object
    do_something(lock.entity)
```

This will lock the object with the id 12345 in the class "Foo" and return the object in the `entity` attribute of the lock.  The lock will be released when the context manager exits.

```python
from aperturedb.Lock import Lock
from contextlib import contextmanager

@contextmanager
def lock_foo(db, id):
    query = [
        { "FindEntity": {
            "with_class": "Foo",
            "constraints": { "id": ["==", id] },
            "results": { "all_properties": True },
        }},
    ]

    with Lock(db, query) as lock:
        yield lock.entity
```

This will create a context manager that locks the object with the id `id` in the class "Foo" and returns the object.  The lock will be released when the context manager exits.
"""


from __future__ import annotations
from typing_extensions import override
from typing import Optional
from contextlib import AbstractContextManager
from uuid import uuid4
from datetime import datetime, timedelta

from Connector import Connector
from types import Commands


LOCK_CLASS = "Lock"


class NoObjectFoundException(Exception):
    pass


class LockedException(Exception):
    pass


class CouldNotUnlockException(Exception):
    pass


class LockResults:
    pass


class Lock(AbstractContextManager):
    def __init__(self,
                 db: Connector,
                 query: Commands,
                 client: Optional[str] = None,
                 description: Optional[str] = None,
                 ):
        """Fetches object and locks it

        This is a mutex-type lock, and only guards against other accesses that also use this locking class.

        Arguments:
            db: ApertureDB connector
            query: Query to find the object or objects to lock.  The query should be composed only of Find commands.  The objects locked will be those returned by the last Find command in the query.  If the query returns multiple objects, the lock will be on all of them.
            client: Optional client name to include in the lock.  This is useful for debugging and monitoring.
            description: Optional description of the lock.  This is useful for debugging and monitoring.

        Raises:
            NoObjectFoundException: If no object was found to lock.
            NotUniqueException: If `unique` was `True` but multiple objects matched.
            LockedException: If unique set and already locked
        """
        uuid = uuid4().hex
        now = datetime.now().isoformat()  # Hopefully clocks are synchronized
        lock_properties = {
            "uuid": uuid,
            "timestamp": now,
            "type": "write",  # For now, only write locks are supported
        }
        if client:
            lock_properties["client"] = client
        if description:
            lock_properties["description"] = description

        self._check_query(query)
        max_ref = max(self._get_refs(query))
        objects_ref = max_ref + 1
        query[-1].values()[0]["_ref"] = objects_ref

        lock_ref = objects_ref + 1
        connection_class = self._connection_class(query[-1])

        query.extend([
            {
                "FindEntity": {
                    "with_class": LOCK_CLASS,
                    "is_connected_to": {
                        "any": {
                            "ref": objects_ref,
                            "connection_class": connection_class,
                        },
                    },
                    "_ref": lock_ref,
                }
            },
            {
                "AddEntity": {
                    "class": LOCK_CLASS,
                    "properties": lock_properties,
                    "connect": {
                        "ref": objects_ref,
                        "class": connection_class,
                    },
                    "if_not_found": lock_ref,
                }
            }
        ])

        result, blobs_returned = db.query(query)

        if result[-3]["returned"] == 0:
            raise NoObjectFoundException(
                f"No object found to lock with query {query}")

        if result[-2]["returned"] > 0:
            raise LockedException(
                f"Object already locked with query {query}")

        self.db = db
        self.uuid = uuid
        self.results = LockResults()
        self.results.result = result[:-2]
        self.results.blobs = blobs_returned
        self.locked = True

    @override
    def __enter__(self) -> LockResults:
        """
        Enters the context manager and returns a result object.

        Returns:
            LockResults: A result object containing two fields
                result: The results of the supplied query
                blobs: The blobs returned by the supplied query
        """
        return self.results

    def _unlock(self):
        if not self.locked:
            return

        query = [
            {
                "DeleteEntity": {
                    "with_class": LOCK_CLASS,
                    "constraints": {"uuid": ["==", self.uuid]},
                }
            }
        ]
        result, blobs_returned = self.db.query(query)
        if result[0]["count"] != 1:
            raise CouldNotUnlockException(
                f"Could not unlock object with uuid {self.uuid}")
        self.locked = False  # Don't try to unlock again

    @override
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exits the context manager and unlocks the object. 

        Raises:
            CouldNotUnlockException: If the object could not be unlocked.  This is raised on exit of the context manager or deletion of the object.
        """
        self._unlock()
        return False  # process exceptions normally

    def __del__(self):
        """Unlocks the obect if it hasn't been unlocked yet
        """
        self._unlock()

    @classmethod
    def cleanup(class_, db, age=24*60*60):
        """Cleans up old locks

        Arguments:
            db: ApertureDB connector
            age: Maximum age of locks in seconds.  Default is 24 hours.
        """
        # Hopefully clocks are synchronized
        threshold = (datetime.now() + timedelta(seconds=-age)).isoformat()
        query = [
            {
                "DeleteEntity": {
                    "with_class": LOCK_CLASS,
                    "constraints": {"timestamp": ["<", threshold]},
                }
            }
        ]
        result, blobs_returned = db.query(query)
        return result[0]["count"]  # Return number of locks deleted
