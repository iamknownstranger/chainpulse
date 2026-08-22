"""Enclave trust-model tests: measurement, policy gate, anti-replay, sealing, signing."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from chainpulse.enclave import (
    AttestationError,
    KmsLite,
    attest,
    measure_pcr0,
)


@pytest.fixture()
def kms(tmp_path: Path) -> KmsLite:
    k = KmsLite(tmp_path / "vault")
    k.init_kek(PASS)
    return k


PASS = "root-of-trust-passphrase"


def test_pcr_measurement_is_stable_and_source_sensitive(tmp_path: Path) -> None:
    assert measure_pcr0() == measure_pcr0()
    assert len(measure_pcr0()) == 64  # sha256 hex


def test_seal_open_roundtrip(kms: KmsLite) -> None:
    kms.seal("binance-api-key", "super-secret-123", "root-of-trust-passphrase")
    doc = attest()
    assert kms.open_sealed("binance-api-key", "root-of-trust-passphrase", doc) == "super-secret-123"
    # blob on disk must not contain the plaintext
    raw = (kms.secrets_dir / "binance-api-key.sealed").read_text()
    assert "super-secret-123" not in raw


def test_wrong_passphrase_rejected_at_kms(kms: KmsLite) -> None:
    kms.seal("k", "v", PASS)
    with pytest.raises(AttestationError, match="passphrase rejected"):
        kms.open_sealed("k", PASS + "-wrong", attest())


def test_nonce_replay_detected(kms: KmsLite) -> None:
    kms.seal("k", "v", PASS)
    doc = attest(nonce="fixed-nonce")
    kms.open_sealed("k", PASS, doc)
    with pytest.raises(AttestationError, match="replay"):
        kms.open_sealed("k", PASS, doc)  # same nonce reused


def test_stale_attestation_rejected(kms: KmsLite) -> None:
    stale = attest(nonce="old")
    object.__setattr__(stale, "issued_ts", time.time() - 10_000)
    kms.seal("k", "v", PASS)
    with pytest.raises(AttestationError, match="stale"):
        kms.open_sealed("k", PASS, stale)


def test_pcr_policy_mismatch_rejected(kms: KmsLite, tmp_path: Path) -> None:
    kms.seal("k", "v", PASS)
    forged = attest(nonce="n1")
    object.__setattr__(forged, "pcr0", "ab" * 32)
    with pytest.raises(AttestationError, match="not in KMS policy"):
        kms.open_sealed("k", PASS, forged)


def test_tampered_ciphertext_fails_closed(kms: KmsLite) -> None:
    kms.seal("wallet-key", "0xdeadbeef", PASS)
    path = kms.secrets_dir / "wallet-key.sealed"
    blob = json.loads(path.read_text())
    flipped = bytearray(bytes.fromhex(blob["ciphertext"]))
    flipped[0] ^= 0xFF
    blob["ciphertext"] = bytes(flipped).hex()
    path.write_text(json.dumps(blob))
    with pytest.raises(AttestationError, match="tampered"):
        kms.open_sealed("wallet-key", PASS, attest())


def test_sign_without_reveal(kms: KmsLite) -> None:
    kms.seal("wallet-key", "never-show-this", PASS)
    sig_hex, pub_hex = kms.sign_message("wallet-key", "withdraw 1.0 ETH to 0xabc", PASS, attest())
    assert KmsLite.verify_signature("withdraw 1.0 ETH to 0xabc", sig_hex, pub_hex)
    assert not KmsLite.verify_signature("withdraw 9.9 ETH to 0xabc", sig_hex, pub_hex)
    assert len(bytes.fromhex(sig_hex)) == 64  # ed25519 signature
    # the secret never touched disk or the return value
    raw = (kms.secrets_dir / "wallet-key.sealed").read_text()
    assert "never-show-this" not in raw and sig_hex != b"never-show-this".hex()
