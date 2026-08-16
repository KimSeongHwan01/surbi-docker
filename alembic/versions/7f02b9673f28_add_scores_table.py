"""add scores table

Revision ID: 7f02b9673f28
Revises:
Create Date: 2026-08-12 18:02:26.898397

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7f02b9673f28"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scores",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("district_id", sa.Integer(), nullable=False),
        sa.Column("category_code", sa.String(20), nullable=False),
        sa.Column("score", sa.Numeric(5, 2), nullable=True),
        sa.Column("expected_sales", sa.BigInteger(), nullable=True),
        sa.Column("closure_risk", sa.Numeric(5, 2), nullable=True),
        sa.Column(
            "recorded_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["district_id"], ["districts.id"], name="scores_district_id_fkey"
        ),
        sa.PrimaryKeyConstraint("id", name="scores_pkey"),
        sa.UniqueConstraint(
            "district_id", "category_code", name="uq_scores_district_category"
        ),
    )


def downgrade() -> None:
    op.drop_table("scores")
