# Lab 01 — Blockchain Layer 1 tối giản với đồng thuận BFT

Mô phỏng một blockchain Layer 1 đạt finality tin cậy trên mạng không đáng tin cậy.
Đồng thuận theo kiểu Tendermint (Prevote/Precommit) với `n = 3f + 1` validator,
thực thi trạng thái deterministic, mạng mô phỏng có delay/drop/duplicate/reorder.

## Yêu cầu môi trường

- Python 3.10 trở lên
- Thư viện `pynacl` (chữ ký Ed25519)

## Cài đặt

```bash
python3 -m pip install pynacl
```

Dùng `python3 -m pip` thay vì `pip` để đảm bảo cài đúng interpreter sẽ chạy chương trình.

Nếu gặp lỗi `externally-managed-environment`, dùng virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install pynacl
```

## Chạy chương trình

### Chạy toàn bộ (điểm vào duy nhất)

```bash
python3 main.py --test all
```

Chạy tất cả unit test và 8 kịch bản T1–T8, in kết quả PASS/FAIL từng mục và
tổng kết ở cuối. Thoát với mã khác 0 nếu có bất kỳ test nào thất bại.

Kết quả mong đợi: `SUMMARY: 33/33 passed`

### Chạy một kịch bản

```bash
python3 main.py --test 1      # thay 1 bằng số từ 1 đến 8
```

### Chỉ chạy unit test

```bash
python3 main.py --test unit
```

## Các kịch bản kiểm thử

| ID | Kịch bản | Kết quả yêu cầu |
|----|----------|-----------------|
| T1 | Chạy bình thường, không lỗi | Mọi node đúng finalize cùng chuỗi và cùng state hash |
| T2 | Message trùng lặp / đảo thứ tự | Không đếm trùng phiếu, không finalize mâu thuẫn |
| T3 | Chữ ký sai / sai domain | Message bị từ chối, ghi log lý do |
| T4 | Giao dịch bị replay / trùng lặp | Chỉ áp dụng đúng một lần |
| T5 | Drop/delay trước khi mạng ổn định | Safety được giữ, không finalize hai lần cùng height |
| T6 | Proposer im lặng / crash | Round timeout kích hoạt, chuỗi tiếp tục |
| T7 | Tối đa f validator Byzantine equivocate | Không finalize mâu thuẫn |
| T8 | Chạy lại cùng seed hai lần | Log giống nhau từng byte và cùng state hash cuối |

## Unit test

Chạy riêng từng nhóm nếu cần:

```bash
python3 tests/test_crypto.py       # xác thực chữ ký, domain separation
python3 tests/test_execution.py    # cập nhật trạng thái, chống replay, quyền sở hữu
python3 tests/test_consensus.py    # đếm phiếu, kiểm tra tính hợp lệ của block
python3 tests/test_network.py      # giới hạn tốc độ gửi, chặn peer quá tải
python3 tests/test_gossip.py       # phát header trước body
```

## Kiểm tra tính deterministic

```bash
bash scripts/check_determinism.sh 1     # thay 1 bằng số kịch bản 1-8
```

Script chạy cùng một kịch bản trong hai tiến trình riêng biệt rồi so sánh file
log trên đĩa và kết quả in ra. Script tự chọn interpreter nào import được
`pynacl`; có thể chỉ định thủ công:

```bash
PYTHON=/duong/dan/toi/python bash scripts/check_determinism.sh 1
```

## Cấu hình

Tham số các kịch bản nằm trong `config/scenarios.json`. Khối `default` áp dụng
cho mọi kịch bản, khối riêng của từng kịch bản ghi đè lên.

| Tham số | Ý nghĩa |
|---------|---------|
| `num_nodes` | Số validator (`n = 3f + 1`), mặc định 8 |
| `bounded_delay` | Độ trễ giao message sau khi mạng ổn định |
| `min_delay` / `max_delay` | Khoảng độ trễ trước khi mạng ổn định |
| `drop_rate` | Xác suất mất message trước khi ổn định |
| `duplicate_rate` | Xác suất message bị nhân đôi |
| `rate_limit` | Số message tối đa mỗi node gửi trong `rate_window` (0 = tắt) |
| `duration` | Thời gian chạy (giây ảo) |
| `num_byzantine` | Số validator Byzantine (chỉ dùng cho T7) |

Sửa file này để đổi cấu hình mà không cần sửa mã nguồn. Nếu file thiếu hoặc sai
định dạng, chương trình dùng giá trị mặc định có sẵn thay vì dừng lại.

## Định dạng log

Log ghi ra `logs/tN.jsonl`, mỗi dòng là một sự kiện JSON. Mọi bản ghi đều có:

| Trường | Ý nghĩa |
|--------|---------|
| `ts` | Thời điểm (giây ảo từ bộ lập lịch) |
| `h` | Height |
| `r` | Round |
| `node` | ID node sinh ra sự kiện |
| `type` | Loại sự kiện |

Sự kiện mạng có thêm `body` chứa loại message (`PROPOSAL`, `PREVOTE`,
`PRECOMMIT`, `BODY_REQUEST`, `BODY_RESPONSE`), chiều (`SENT`/`RECV`) và trạng
thái gửi (`SUCCEEDED`, `DUPLICATED`, `DROPPED`, `BLOCKED`, `UNBLOCKED`).

Sự kiện đồng thuận có thêm `event` và `msg`:

| Sự kiện | Thời điểm ghi |
|---------|---------------|
| `ROUND_TIMEOUT` | Hết thời gian chờ của một round |
| `VAL<n>_BLOCK` | Block không hợp lệ khi kiểm tra |
| `ON_VOTE_<phase>` | Sau khi nhận một phiếu |
| `PREVOTE_QUORUM` | Trước khi gửi phiếu Precommit |
| `PRECOMMIT_QUORUM` | Trước khi finalize block |
| `HEADER_REJECTED` | Từ chối header nhận được |
| `HEADER_ACCEPTED_BODY_REQUESTED` | Chấp nhận header, yêu cầu body |
| `BODY_SENT` / `BODY_RECEIVED` | Gửi / nhận body giao dịch |
| `BYZANTINE_EQUIVOCATE_*` | Node Byzantine gửi thông tin mâu thuẫn |

## Cấu trúc thư mục

```
src/
├── crypto/
│   ├── encoding.py     # mã hóa canonical + băm SHA-256
│   └── signing.py      # Ed25519, domain separation TX/HEADER/VOTE
├── types/
│   └── messages.py     # Transaction, BlockHeader, Block, Vote
├── execution/
│   └── state.py        # hàm chuyển trạng thái deterministic
├── network/
│   └── simulator.py    # bộ lập lịch sự kiện, mô phỏng mạng, ghi log
├── consensus/
│   └── engine.py       # Propose/Prevote/Precommit, quy tắc khóa
├── byzantine.py        # validator Byzantine dùng cho T7
└── node.py             # ghép các tầng thành một node hoàn chỉnh

tests/                  # unit test
config/scenarios.json   # cấu hình các kịch bản
logs/                   # log sự kiện (JSON Lines)
scripts/                # script kiểm tra determinism
main.py                 # điểm vào chương trình
```

## Tài liệu tham khảo

- Buchman, Kwon, Milosevic. *The latest gossip on BFT consensus* (Tendermint):
  https://arxiv.org/abs/1807.04938
- PyNaCl: https://pynacl.readthedocs.io/
