"""positive amount constraint

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-05
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CONSTRAINT = "ck_payments_amount_positive"


def upgrade() -> None:
    # Явный SQL: имя обязано совпасть с тем, что даёт naming convention модели
    op.execute(f"ALTER TABLE payments ADD CONSTRAINT {CONSTRAINT} CHECK (amount > 0)")


def downgrade() -> None:
    op.execute(f"ALTER TABLE payments DROP CONSTRAINT {CONSTRAINT}")
