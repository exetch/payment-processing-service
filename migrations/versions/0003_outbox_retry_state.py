"""outbox retry state

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-05
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UNPUBLISHED = sa.text("published_at IS NULL")


def upgrade() -> None:
    op.add_column(
        "outbox",
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "outbox",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column("outbox", sa.Column("last_error", sa.Text(), nullable=True))

    op.drop_index("ix_outbox_unpublished_created_at", table_name="outbox")
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["next_attempt_at", "created_at"],
        postgresql_where=UNPUBLISHED,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["created_at"],
        postgresql_where=UNPUBLISHED,
    )
    op.drop_column("outbox", "last_error")
    op.drop_column("outbox", "next_attempt_at")
    op.drop_column("outbox", "attempts")
