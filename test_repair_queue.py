import sqlite3
import unittest

import repair_queue


class SqliteTurso:
    def __init__(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row

    def execute(self, sql, args=None):
        self.connection.execute(sql, args or [])
        self.connection.commit()

    def query(self, sql, args=None):
        return [dict(row) for row in self.connection.execute(sql, args or [])]

    def batch(self, statements):
        for statement in statements:
            self.connection.execute(statement["sql"], statement["args"])
        self.connection.commit()


class RepairQueueGenerationTests(unittest.TestCase):
    def test_old_claim_cannot_delete_work_enqueued_during_rebuild(self):
        turso = SqliteTurso()
        repair_queue.enqueue(turso, {"network_full": {"required"}}, detected_at=1)
        old_claim = repair_queue.load(turso, "network_full")

        repair_queue.enqueue(turso, {"network_full": {"required"}}, detected_at=2)
        repair_queue.complete(turso, "network_full", old_claim)

        current_claim = repair_queue.load(turso, "network_full")
        self.assertEqual(current_claim, {"required": 2})


if __name__ == "__main__":
    unittest.main()
