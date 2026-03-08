# MongoDB Integration

This guide explains how to use the MongoDB integration (`forze_mongo`) with Forze.

## Prerequisites

- MongoDB
- `forze[mongo]` extra installed (includes `pymongo`)

## Overview

The `forze_mongo` package provides adapters and primitives for using MongoDB as a persistence store in your Forze applications.

### Transaction Management

The package includes `MongoTxManagerAdapter` which implements `TxManagerPort` for managing MongoDB transactions:

```python
from forze_mongo.adapters.txmanager import MongoTxManagerAdapter
from forze.application.contracts.tx import TxManagerPort

# Used as a dependency for transaction scoping
```

*Note: The MongoDB integration is currently experimental and may not support all features available in the PostgreSQL integration yet.*
