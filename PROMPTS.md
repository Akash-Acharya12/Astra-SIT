# Astra-SIT: AI Interaction Prompts & Collaborative Workflow Log

This document records the actual prompts, engineering instructions, and verification workflows used with AI pair programming during the hackathon development of Astra-SIT.

---

## 1. Core Recovery Verification & Invariant Hardening Prompts

### Prompt: Pre-Truncation Guard, Monotonicity, and Invariant Verification
> "The recovery integration is implemented. Before committing, run and verify these three additional tests against the actual integration code:
> 1. Pre-truncation guard: Calling `RecoveryManager.get_max_tx_id(storage)` before truncating an unsafe log tail must raise `RecoveryError`.
> 2. TX_ABORT monotonicity: Write a transaction that is aborted, close and reopen the database, then verify the next transaction ID is greater than the aborted transaction ID. Ensure aborted operations are not applied to the index.
> 3. Empty database startup: Open a new empty database, verify it starts successfully, and confirm the first batch uses transaction ID 1.
> Also verify:
> - Recovery truncates only unsafe trailing data.
> - Existing persistence, delete recovery and batch recovery tests still pass.
> - Python syntax checks pass.
> - `git diff --check` passes.
> Do not modify storage.py, index.py, batch.py, compaction.py, cli.py or torture.py.
> Do not commit, push, merge or reset anything yet."

---

## 2. Durability & Protocol Verification Prompts

### Prompt: Physical fsync Verification Before ACK
> "The recovery integration tests have passed 16/16. Now prioritize durability verification and CLI integration.
> 1. Inspect `storage.py`, `main.py`, and `batch.py` to verify that:
>    - `put()` calls storage sync/fsync before returning True.
>    - `delete()` calls storage sync/fsync before returning True.
>    - `batch()` writes the commit marker and calls sync/fsync before returning True.
>    - No successful ACK can be sent before the durable write completes.
> 2. Report the exact methods and line numbers responsible for durability.
> 3. Verify that `main.py` correctly integrates `recovery.py` and that the 16 passing tests still pass.
> Report any durability gaps immediately."

---

## 3. Cross-Branch Inspection & Teammate Integration Prompts

### Prompt: Comparative Analysis of Recovery Implementations
> "URGENT: We have less than 3.5 hours remaining. Dakshin has pushed another recovery implementation on branch `storage-engine` (commit `3c273fd4`).
> Our current integration branch already contains recovery commit `b1c09da`.
> Do the following:
> 1. Fetch origin.
> 2. Compare `origin/integration:recovery.py` against `origin/storage-engine:recovery.py`.
> 3. Identify functional differences, API compatibility, transaction ID restoration, crash recovery, CRC validation, truncation behavior and error handling.
> 4. Check whether the storage-engine implementation is compatible with our current main.py, storage.py, index.py and batch.py.
> 5. Report which implementation is safer to retain and identify any critical missing functionality."

### Prompt: Selective CLI & Test Suite Integration
> "URGENT: Integrate Sandesh's CLI implementation into our integration branch.
> Current branch: integration
> Latest core/recovery commit: `b1c09da`
> Sandesh's CLI commit: `41a8a8b` on `cli-testing`.
> Follow these steps carefully:
> 1. Run `git fetch origin`.
> 2. Inspect commit `41a8a8b` using `git show --stat 41a8a8b`.
> 3. Verify that the commit changes only cli.py and tests/.
> 4. Bring ONLY cli.py and specified tests from commit 41a8a8b into the current integration branch.
> 5. Do NOT replace main.py, storage.py, recovery.py, index.py, batch.py, or compaction.py.
> 6. Preserve recovery_before_dakshin.py.
> 7. Run syntax checks: `python -m py_compile cli.py main.py recovery.py storage.py index.py batch.py compaction.py`.
> 8. Run the CLI tests against the actual current integration code.
> 9. Run the 16 recovery tests.
> 10. Test a real CLI subprocess with PUT, GET, DELETE, BATCH, restart and persistence."

---

## 4. Final Audit, Verification & Submission Prompts

### Prompt: Branch Inconsistency Triage & Read-Only Verification
> "STOP documentation generation temporarily.
> I noticed a critical inconsistency in the latest screenshot. The terminal shows: Current branch: main...
> Run these read-only commands in the exact project terminal:
> - `git rev-parse --show-toplevel`
> - `git branch --show-current`
> - `git status --short`
> - `git log -1 --oneline --decorate`
> - `git branch -avv`
> - `git rev-parse HEAD`
> - `git rev-parse origin/integration`
> Explain why the screenshot shows main while your audit says integration.
> Confirm whether the actual implementation and the 1adf842 commit are present in this repository.
> Also correct the requirements audit:
> - Do not mark two concurrent writers VERIFIED if the exclusive process lock prevents a second process from opening the same database.
> - Do not report estimated recovery speed as a measured benchmark.
> - Distinguish generic OSError testing from actual disk-full testing.
> - Distinguish standard-library/offline compatibility from an actual disconnected-network test."

### Prompt: Final 25-Minute Submission Mode
> "URGENT: ASTRA-SIT FINAL 25-MINUTE SUBMISSION MODE.
> ARCHITECTURE.md has been created and reviewed. We have only 25 minutes remaining.
> - Phase 1: Complete missing essential documentation (README.md, BREAKING.md, PROMPTS.md).
> - Phase 2: Run all tests from repository root (py_compile, unittest discover, 16 recovery tests, real CLI subprocess test, Zaid's torture test).
> - Phase 3: Requirements check against verified evidence.
> - Phase 4: Final freeze, git diff check, status confirmation."
