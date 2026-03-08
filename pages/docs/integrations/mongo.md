# MongoDB Integration

This guide explains how to use the MongoDB integration (`forze_mongo`) with Forze.

## Prerequisites

- MongoDB
- `forze[mongo]` extra installed (includes `pymongo`)

## Overview

The `forze_mongo` package provides adapters and primitives for using MongoDB as a persistence store in your Forze applications.

### Document Adapter

`forze_mongo` includes `MongoDocumentAdapter`, a `DocumentPort` implementation
for CRUD, filtering, sorting, pagination, and soft-delete/restore workflows.
It uses the same query DSL contracts as other integrations.

### Transaction Management

The package includes `MongoTxManagerAdapter` which implements `TxManagerPort` for managing MongoDB transactions:

    :::python
    from forze_mongo.adapters.txmanager import MongoTxManagerAdapter
    from forze.application.contracts.tx import TxManagerPort

    # Used as a dependency for transaction scoping
