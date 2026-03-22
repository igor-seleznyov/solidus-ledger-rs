# Solidus Ledger Binary Protocol

## Connections

The client opens two TCP connections:
- **COMMAND** — sending batches, synchronous response (ACCEPTED/FAILED/WITH_REJECTS)
- **RESULT** — listening for asynchronous results after 2PC

Connections are linked by `client_id` (UUIDv7) during handshake.

## Wire format

All messages use length-prefixed framing with a magic word.

### Request (client -> server)

```
+--------------------+------------+----------+--------------+
| magic: [u8; 8]     | msg_type:u8| len: u32 | payload: [u8]|
+--------------------+------------+----------+--------------+
```

magic = `SLDLGRRQ` (0x534C444C47525251) — Solidus Ledger Request.

### Response (server -> client)

```
+--------------------+------------+----------+--------------+
| magic: [u8; 8]     | msg_type:u8| len: u32 | payload: [u8]|
+--------------------+------------+----------+--------------+
```

magic = `SLDLGRRS` (0x534C444C47525253) — Solidus Ledger Response.

`len` — payload length (excludes magic, msg_type, and len).
Byte order: big-endian (network byte order).
Header size: 8 + 1 + 4 = 13 bytes.

### CRC

Not added at the protocol level:
- TCP provides checksum at the transport layer
- TLS (step 9) will add AEAD integrity per record
- CRC on the hot path is unnecessary overhead at millions of TPS
- For persistence (LS files) — CRC is handled separately at the storage layer

## Handshake

First message on each connection.

### HandshakeRequest (client -> server)

```
msg_type = 0x01

+------------------+-------------------+----------------------+
| client_id: [u8;16]| conn_type: u8    | protocol_version: u16|
+------------------+-------------------+----------------------+
```

conn_type:
- 1 = COMMAND
- 2 = RESULT

### HandshakeResponse (server -> client)

```
msg_type = 0x02

+------------+
| status: u8 |
+------------+
```

status:
- 0 = OK
- 1 = UNSUPPORTED_VERSION
- 2 = ALREADY_CONNECTED (this client_id + conn_type is already active)

## Transfer data model

### Transfer (single entry in a batch)

```
Field                  Type        Bytes   Offset
----------------------------------------------------
transfer_id            [u8; 16]    16       0    UUIDv7
idempotency_key        [u8; 16]    16      16    UUIDv7
debit_account_id       [u8; 16]    16      32    UUIDv7
credit_account_id      [u8; 16]    16      48    UUIDv7
amount                 i64          8      64    signed
currency               [u8; 16]    16      72    8 chars + padding
business_operation_id  [u8; 16]    16      88    UUIDv7
transfer_datetime      u64          8     104    epoch nanos
metadata               [u8; M]     M      112    M = from configuration
----------------------------------------------------
Total: 112 + M bytes per transfer
```

Transfer size is aligned to 64 bytes (cache line boundary):
- M=0: 112 → padded to 128 bytes (2 cache lines)
- M=16: 128 bytes (2 cache lines, no padding)
- M=144: 256 bytes (4 cache lines, no padding)
- M=80: 192 bytes (3 cache lines, no padding)

General rule: sizeof(Transfer) = align_up(112 + M, 64).
All structures on the hot path are aligned to cache line (#[repr(C, align(64))]).

## COMMAND connection messages

### BatchRequest (client -> server)

```
msg_type = 0x10

+------------------------+-----------------+-------------------+
| batch_id: [u8; 16]     | count: u16      | transfers: [Transfer; count] |
+------------------------+-----------------+-------------------+
```

batch_id — external, set by the client SDK.

### BatchResponse (server -> client)

```
msg_type = 0x11

+------------------------+------------+------------------+--------------------------------+
| batch_id: [u8; 16]     | status: u8 | reject_count: u16| rejects: [Reject; reject_count]|
+------------------------+------------+------------------+--------------------------------+
```

status (primary validation result):
- 0 = ACCEPTED — entire batch accepted for processing
- 1 = FAILED — entire batch rejected (invalid format, empty batch, ...)
- 2 = WITH_REJECTS — some transfers rejected, the rest accepted

When ACCEPTED: reject_count = 0.
When FAILED: reject_count = 0 (entire batch is invalid, no individual rejects).
When WITH_REJECTS: reject_count > 0.

### Reject (in BatchResponse)

```
+------------------------+------------+
| transfer_id: [u8; 16]  | reason: u8 |
+------------------------+------------+
```

Primary validation rejection reasons:
- 1 = INVALID_TRANSFER_ID (zero or invalid UUID)
- 2 = INVALID_ACCOUNT_ID (debit or credit)
- 3 = INVALID_AMOUNT (zero or overflow)
- 4 = INVALID_CURRENCY
- 5 = INVALID_DATETIME
- 6 = DUPLICATE_IN_BATCH (duplicate transfer_id within the batch)

### SingleTransferRequest (client -> server) — future endpoint

```
msg_type = 0x12

+-------------------+
| transfer: Transfer|
+-------------------+
```

### SingleTransferResponse (server -> client)

```
msg_type = 0x13

+------------------------+------------+
| transfer_id: [u8; 16]  | status: u8 |
+------------------------+------------+
```

status: 0 = ACCEPTED, 1 = FAILED (+ reason in additional field if needed).

## RESULT connection messages

### BatchResult (server -> client)

```
msg_type = 0x20

+------------------------+------------+------------------+-------------------------------+
| batch_id: [u8; 16]     | status: u8 | reject_count: u16| rejects: [ResultReject; count]|
+------------------------+------------+------------------+-------------------------------+
```

status (2PC result):
- 0 = SUCCESS — all transfers in the batch succeeded
- 1 = WITH_REJECTS — some transfers failed 2PC

When SUCCESS: reject_count = 0 (no list sent — all succeeded).
When WITH_REJECTS: reject_count > 0, the list contains only failed transfers.

### ResultReject (in BatchResult)

```
+------------------------+------------+
| transfer_id: [u8; 16]  | reason: u8 |
+------------------------+------------+
```

Post-2PC rejection reasons:
- 10 = INSUFFICIENT_FUNDS
- 11 = DUPLICATE_IDEMPOTENCY_KEY (L1/L2/L3 deduplication)
- 12 = ACCOUNT_NOT_FOUND
- 13 = CURRENCY_MISMATCH
- 14 = RULE_VIOLATION
- 15 = INTERNAL_ERROR

Numbering starts at 10 to avoid overlap with primary validation reason codes.

## msg_type table

```
0x01  HandshakeRequest
0x02  HandshakeResponse
0x10  BatchRequest
0x11  BatchResponse
0x12  SingleTransferRequest (future)
0x13  SingleTransferResponse (future)
0x20  BatchResult
0xFF  Heartbeat (keep-alive, empty payload)
```

## Validation

### Primary (on receipt at COMMAND, before passing to Pipeline)

Fast format checks that do not require data access:
- Batch is not empty (count > 0)
- Payload size is correct: sizeof(batch_header) + count * sizeof(Transfer) == len
- Each transfer_id != zero UUID
- amount != 0
- debit_account_id != credit_account_id
- No duplicate transfer_id within the batch

### Deep (Pipeline -> Actor -> DM, result delivered on RESULT)

- Idempotency (L1 Hopscotch -> L2 Bloom -> L3 .ikey)
- Account existence (PAHT lookup)
- Sufficient funds (balance check)
- Rules (RuleTable)
- Account currency == transfer currency
