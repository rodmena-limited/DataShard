"""
TransactionManager: hands out Transaction objects and evicts finished ones (split out of
transaction.py for the 500-line file cap).
"""

import threading
from typing import Dict, List

from .file_manager import FileManager
from .metadata_manager import MetadataManager
from .snapshot_manager import SnapshotManager
from .transaction import Transaction


class TransactionManager:
    """Manages multiple transactions and ensures ACID compliance"""

    def __init__(
        self,
        metadata_manager: MetadataManager,
        snapshot_manager: SnapshotManager,
        file_manager: FileManager,
    ):
        self.metadata_manager = metadata_manager
        self.snapshot_manager = snapshot_manager
        self.file_manager = file_manager
        self._active_transactions: Dict[int, "Transaction"] = {}
        self._lock = threading.RLock()

    def begin_transaction(self) -> Transaction:
        """Begin a new transaction"""
        with self._lock:
            # Evict finished transactions so long-running processes don't leak
            # one Transaction object per commit.
            self._cleanup_locked()

            transaction = Transaction(
                self.metadata_manager, self.snapshot_manager, self.file_manager
            )
            transaction_id = id(transaction)
            self._active_transactions[transaction_id] = transaction
            return transaction

    def get_active_transactions(self) -> List[Transaction]:
        """Get all active transactions"""
        with self._lock:
            return [tx for tx in self._active_transactions.values() if tx.is_active()]

    def cleanup_completed_transactions(self) -> None:
        """Remove completed/failed transactions from tracking"""
        with self._lock:
            self._cleanup_locked()

    def _cleanup_locked(self) -> None:
        completed_ids = [
            tx_id for tx_id, tx in self._active_transactions.items() if not tx.is_active()
        ]
        for tx_id in completed_ids:
            del self._active_transactions[tx_id]
