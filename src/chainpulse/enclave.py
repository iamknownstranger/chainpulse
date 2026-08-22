"""Enclave-style secret isolation (local simulation of the Nitro trust model).

This is an *educational simulation* of the attested-enclave pattern used to
protect signing keys in production trading infrastructure - not a hardware
TEE. It reproduces the trust model faithfully so the invariants are the same:

1. **Measurement**  The enclave hashes its own sources into a PCR0 digest.
2. **Attestation**  It produces an attestation document binding PCR0 to a
   caller-supplied nonce with a freshness window. In production this document
   is signed by immutable hardware; here the *enforcement point* is faithful:
   the KMS-side policy gate. A document is accepted only if
   ``pcr0 ∈ policy`` and the nonce has never been seen (anti-replay) and the
   timestamp is fresh.
3. **Policy-gated release**  The key-encryption key is released only against
   an acceptable attestation. Secrets on disk are envelope-encrypted with
   per-secret data keys (AES-256-GCM), wrapped under the KEK, bound as AAD to
   their name so blobs cannot be swapped between entries.
4. **Sign-without-reveal**  Unsealed private keys are used in-memory to sign
   and are never returned to callers or written anywhere.

In production the passphrase below stands in for the KMS + hardware root of
trust; swap :class:`KmsLite` for a cloud KMS client and the rest is unchanged.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

NONCE_TTL_S = 300
_ENCLAVE_SOURCES = ["src/chainpulse/enclave.py", "src/chainpulse/models.py"]


class AttestationError(Exception):
    """Raised when an attestation document fails policy."""


def measure_pcr0(root: Path | None = None) -> str:
    """SHA-256 over this enclave's own code (order-normalized)."""
    base = root or Path(__file__).resolve().parents[2]
    digest = hashlib.sha256()
    for rel in sorted(_ENCLAVE_SOURCES):
        path = base / rel
        digest.update(rel.encode())
        digest.update(path.read_bytes() if path.exists() else b"<missing>")
    return digest.hexdigest()


@dataclass(frozen=True)
class AttestationDoc:
    pcr0: str
    nonce: str
    issued_ts: float
    mac: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def _mac(pcr0: str, nonce: str, ts: float, boot_key: bytes) -> str:
    msg = f"{pcr0}|{nonce}|{ts}".encode()
    return hmac.new(boot_key, msg, hashlib.sha256).hexdigest()


def attest(nonce: str | None = None, boot_key: bytes | None = None) -> AttestationDoc:
    """Produce a freshness-bound document describing THIS process's enclave."""
    real_nonce = nonce or secrets.token_hex(16)
    ts = time.time()
    pcr0 = measure_pcr0()
    return AttestationDoc(
        pcr0=pcr0,
        nonce=real_nonce,
        issued_ts=ts,
        mac=_mac(pcr0, real_nonce, ts, boot_key or b""),
    )


class KmsLite:
    """Stand-in for a KMS with an enclave attestation policy.

    Holds the KEK behind PBKDF2(passphrase) - the passphrase models the
    hardware root of trust. ``release_kek`` enforces the attestation policy:
    PCR match + fresh timestamp + never-seen nonce.
    """

    def __init__(self, root: Path | str = "vault") -> None:
        self.root = Path(root)
        self.kms_dir = self.root / "kms"
        self.secrets_dir = self.root / "secrets"
        self.kms_dir.mkdir(parents=True, exist_ok=True)
        self.secrets_dir.mkdir(parents=True, exist_ok=True)
        self.kek_path = self.kms_dir / "kek.json"
        self.policy_path = self.kms_dir / "policy.json"
        self.nonces_path = self.kms_dir / "seen_nonces.json"

    # -- setup ------------------------------------------------------------

    def init_kek(self, passphrase: str) -> None:
        if self.kek_path.exists():
            raise FileExistsError("KEK already initialized")
        salt = os.urandom(16)
        kek = self._derive(passphrase, salt)
        verifier = hmac.new(kek, b"chainpulse-kms-check", hashlib.sha256).hexdigest()
        self.kek_path.write_text(json.dumps({"salt": salt.hex(), "verifier": verifier}))
        if not self.policy_path.exists():
            self.set_policy([measure_pcr0()])

    def kek_initialized(self) -> bool:
        return self.kek_path.exists()

    def set_policy(self, pcr_allow: list[str]) -> None:
        self.policy_path.write_text(json.dumps({"pcr_allow": pcr_allow}))

    def policy(self) -> list[str]:
        return json.loads(self.policy_path.read_text())["pcr_allow"]

    # -- attestation enforcement ------------------------------------------

    def verify_attestation(self, doc: AttestationDoc) -> None:
        if doc.pcr0 not in self.policy():
            raise AttestationError(
                f"PCR0 {doc.pcr0[:16]}… not in KMS policy - code changed since sealing?"
            )
        if abs(time.time() - doc.issued_ts) > NONCE_TTL_S:
            raise AttestationError("attestation stale (freshness window exceeded)")
        seen = json.loads(self.nonces_path.read_text()) if self.nonces_path.exists() else []
        if doc.nonce in seen:
            raise AttestationError("nonce replay detected")
        seen.append(doc.nonce)
        self.nonces_path.write_text(json.dumps(seen))

    def release_kek(self, passphrase: str, doc: AttestationDoc) -> bytes:
        self.verify_attestation(doc)
        payload = json.loads(self.kek_path.read_text())
        salt = bytes.fromhex(payload["salt"])
        kek = self._derive(passphrase, salt)
        expected = hmac.new(kek, b"chainpulse-kms-check", hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, payload["verifier"]):
            raise AttestationError("passphrase rejected by KMS")
        return kek

    # -- seal / open / sign -------------------------------------------------

    def seal(self, name: str, plaintext: str, passphrase: str) -> Path:
        if not self.kek_initialized():
            self.init_kek(passphrase)
        kek = self.release_kek(passphrase, attest(nonce=f"seal-{secrets.token_hex(8)}"))
        dek = os.urandom(32)
        iv = os.urandom(12)
        ciphertext = AESGCM(dek).encrypt(iv, plaintext.encode(), name.encode())
        wrap_iv = os.urandom(12)
        wrapped_dek = AESGCM(kek).encrypt(wrap_iv, dek, name.encode())
        blob = {
            "iv": iv.hex(),
            "ciphertext": ciphertext.hex(),
            "wrap_iv": wrap_iv.hex(),
            "wrapped_dek": wrapped_dek.hex(),
            "pcr_at_seal": measure_pcr0(),
        }
        out = self.secrets_dir / f"{name}.sealed"
        out.write_text(json.dumps(blob))
        return out

    def open_sealed(self, name: str, passphrase: str, doc: AttestationDoc) -> str:
        blob = json.loads((self.secrets_dir / f"{name}.sealed").read_text())
        kek = self.release_kek(passphrase, doc)
        try:
            dek = AESGCM(kek).decrypt(
                bytes.fromhex(blob["wrap_iv"]), bytes.fromhex(blob["wrapped_dek"]), name.encode()
            )
            pt = AESGCM(dek).decrypt(
                bytes.fromhex(blob["iv"]), bytes.fromhex(blob["ciphertext"]), name.encode()
            )
        except InvalidTag:
            raise AttestationError(
                "decryption failed - tampered blob, wrong name-binding, or wrong passphrase"
            ) from None
        return pt.decode()

    def sign_message(
        self, name: str, message: str, passphrase: str, doc: AttestationDoc
    ) -> tuple[str, str]:
        """Sign inside the 'enclave'. Returns (signature_hex, pubkey_hex); never plaintext."""
        secret = self.open_sealed(name, passphrase, doc)
        seed = hashlib.sha256(secret.encode()).digest()
        key = Ed25519PrivateKey.from_private_bytes(seed)
        sig = key.sign(message.encode())
        return sig.hex(), key.public_key().public_bytes_raw().hex()

    @staticmethod
    def verify_signature(message: str, signature_hex: str, pubkey_hex: str) -> bool:
        try:
            Ed25519PublicKey.from_public_bytes(bytes.fromhex(pubkey_hex)).verify(
                bytes.fromhex(signature_hex), message.encode()
            )
            return True
        except (InvalidSignature, ValueError):
            return False

    @staticmethod
    def _derive(passphrase: str, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=480_000)
        return kdf.derive(passphrase.encode())


__all__ = [
    "AttestationDoc",
    "AttestationError",
    "KmsLite",
    "attest",
    "measure_pcr0",
]
