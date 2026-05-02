# Lightning Goats Security Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the audit findings around outbound URL safety, wallet ownership, physical-control fail-closed behavior, payment failure state, configuration state, and secret logging.

**Architecture:** Add small validation helpers near the affected extension code, then apply them at API boundaries and service fetch points. Keep payment handling idempotent, but distinguish successful feeder/distribution processing from failed attempts. Tests cover the security policy and state transitions without requiring a running LNbits server.

**Tech Stack:** Python, FastAPI route helpers, LNbits wallet CRUD, httpx, pytest.

---

### Task 1: URL Policy

**Files:**
- Create: `services/url_validation.py`
- Test: `tests/test_security_fixes.py`

**Steps:**
1. Write tests proving public HTTP(S) and `10.8.0.0/24` are allowed.
2. Write tests proving loopback, link-local, and other private ranges are rejected.
3. Implement `validate_outbound_url()` and `ensure_outbound_url_allowed()`.
4. Apply validation to settings updates and weather fetches.

### Task 2: Wallet Ownership And Configuration

**Files:**
- Modify: `views_api.py`
- Test: `tests/test_security_fixes.py`

**Steps:**
1. Write tests for rejecting a non-owned herd wallet.
2. Write tests for `configured` being false without an OpenHAB URL.
3. Add ownership helper and use it in settings, balance, and status routes.
4. Add a helper that returns operational configuration state.

### Task 3: OpenHAB And Payment Failure State

**Files:**
- Modify: `services/openhab.py`
- Modify: `tasks.py`
- Test: `tests/test_security_fixes.py`

**Steps:**
1. Write tests for override state failure returning unknown.
2. Write tests for feeder trigger failures marking payments failed.
3. Make override checks fail closed unless manually bypassed.
4. Raise on feeder/distribution failure inside payment handling before marking processed.

### Task 4: Logging And Lint

**Files:**
- Modify: `views_api.py`
- Modify: `tasks.py`
- Modify: `services/messaging.py`
- Modify: `services/bitcoin_price.py`

**Steps:**
1. Remove secret-bearing payload logs.
2. Remove unused imports/globals and unnecessary f-strings.
3. Run `pytest tests/test_security_fixes.py`.
4. Run `pyflakes .`.
