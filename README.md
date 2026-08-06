# Design and implement Layer 1 of Blockchain.

Layer 1 represents the core blockchain networks, such as Bitcoin and Ethereum. It ensures security and decentralization through consensus mechanisms like Proof-of-Work (PoW) and Proof-of-Stake (PoS).
- Transaction Processing: Records and verifies transactions.
- Consensus Mechanism: Maintains decentralized agreement on blockchain state.

## System Design

- State: A key-value table (e.g. `alice: "Hi"`)
- Transaction: An account (key) overwrite with a new message (value).
- Consensus: minimal [Tendermint](https://arxiv.org/abs/1807.04938) style.

## Setting Up

Create a virtual environment.

```bash
pip install pynacl
```

## Run test cases

```bash
python main.py --test TEST  # Run test cases (1-8)
```

In test T2
- Duplicated votes made by the same node (validator) is rejected.
- Reordered messages are done by time delay, beginning messages arrive later than those sent at the end. This reverse the order of propose to prevote to precommit, taking the consensus more rounds to converge to new height.

In test T3
- We tamper key pairs of a transaction, and key pairs of some nodes (validators).
- Tampering transaction result in invalid transaction (logging `VAL06_BLOCK`) when a proposed block is validated.
- Tampering nodes (validators) result in invalid block header (logging `VAL03_BLOCK`), and invalid vote (`ON_VOTE_PREVOTE`, `ON_VOTE_PRECOMMIT`).

In test T4
- Transactions made by an account is ordered by `nounce` value, later transactions use higher nonce. This helps applying transactions in sequencially. Before applying, transactions are verified using the signature, and checked for replayed using the `nonnce` value.
- When a transaction is replayed/duplicated, they hold the same `nonce` and only first one is applied.

In test T5
- Drop messages may lead to some nodes failed to move to next step. If correct nodes smaller than quorum, they move to next round and restart proposing and voting. If correct nodes satistfy quorum, all correct nodes will eventually converge to some chain.
- The test start with unreliable network (drop, delay), then stablize the network until correct nodes converge.

## Logging

Log network interaction between nodes with:
- Three types of messages (`PROPOSAL`, `PREVOTE`, `PRECOMMIT`) and two directions (`SENT`, `RECV`).
- Sending messages (`SENT`) in an unreliable network (configured `stabilized=False`) can result in being delayed (`SUCCEEDED` with delay time), duplicated (`DUPLICATED`), dropped (`DROPPED`), or reordered (not yet implemented).

Log events of consensus algorithms:

|Event|Description|Source|
|-|-|-|
|`ROUND_TIMEOUT`|After round timeout|`def _round_timeout()`|
|`VAL<index>_BLOCK`|While valiadating a block|`def validate_block()`|
|`ON_VOTE_<vote_step>`|After receiving a vote|`def on_vote()`|
|`PREVOTE_QUORUM`|Before sending precommit vote|`def _on_prevote_quorum()`|
|`PRECOMMIT_QUORUM`|Before deciding and applying a block's transactions|`def _on_precommit_quorum()`|

## Chạy unit test

```bash
python3 tests/test_crypto.py
python3 tests/test_execution.py
```

(Có thể chuyển sang `pytest` khi nhóm mở rộng thêm test — cấu trúc file đã tương thích.)

## Cấu trúc project

```
src/
├── crypto/
│   ├── encoding.py   # canonical JSON + SHA-256 hashing (deterministic encoding)
│   └── signing.py    # Ed25519 keygen/sign/verify + domain separation (TX/HEADER/VOTE)
├── types/
│   └── messages.py   # Transaction, BlockHeader, Block, Vote dataclasses
├── execution/
│   └── state.py       # deterministic state transition function, replay protection
├── network/
│   └── simulator.py   # asyncio-based network: delay/drop/duplicate + event logging
├── consensus/
│   └── engine.py       # Tendermint-style propose/prevote/precommit + locking rules
└── node.py              # wires crypto+network+consensus+execution into one node
main.py                    # entry point: spins up N nodes, runs T1
tests/                      # unit tests
logs/                        # network + consensus event logs (JSON lines)
config/                      # (for nhóm mở rộng: file-driven topology/scenario config)
```

## Việc còn cần làm (gợi ý phân công theo 4 người, xem chat)

- [ ] `network/simulator.py`: thêm rate-limit outbound + block/unblock peer quá tải
- [ ] `network/simulator.py`: tách header-broadcast-trước-body cho đúng thật (hiện tại giả định mempool đã có sẵn tx)
- [ ] `consensus/engine.py`: xử lý round-change message rõ ràng hơn (hiện dùng timeout đơn giản)
- [ ] `consensus/engine.py`: xử lý validator Byzantine gửi block/vote không hợp lệ có chủ đích (test T7)
- [ ] Viết test end-to-end đầy đủ cho T2, T3, T5, T6, T7, T8 (xem bảng trong đề bài)
- [ ] Script chạy 2 lần cùng seed và so sánh log + state hash byte-identical (T8) — `Network` đã có `set_seed()` sẵn để hỗ trợ việc này
- [ ] Merkle tree cho state nếu muốn thay vì hash toàn bộ dict (hiện tại `state_root()` hash cả map — hợp lệ theo đề nhưng không hỗ trợ Merkle proof)

## Ghi chú thiết kế

- **Deterministic encoding**: mọi object được ký/hash đều qua `canonical_bytes()` (JSON keys sorted, không whitespace) — đảm bảo mọi node tạo ra cùng byte cho cùng nội dung logic.
- **Domain separation**: `TX:<chain_id>`, `HEADER:<chain_id>`, `VOTE:<chain_id>` — chữ ký của loại message này không thể replay sang loại khác.
- **Quorum**: với n = 3f+1 validator, quorum = 2f+1 (>2n/3), đúng theo giả định BFT trong đề.
- **Locking**: `ConsensusState.locked_block` / `locked_round` implement đúng rule 4-5 trong mục 6.1 của đề bài.
