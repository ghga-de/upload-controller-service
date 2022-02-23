"""add uploadstate

Revision ID: d12aff1f36b0
Revises: 19e3d538a7cb
Create Date: 2022-02-23 15:39:06.611226

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d12aff1f36b0"
down_revision = "19e3d538a7cb"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    if connection.dialect.name == "postgresql":
        upload_status = postgresql.ENUM(
            "REGISTERED", "PENDING", "UPLOADED", "COMPLETED", name="uploadstate"
        )
        upload_status.create(op.get_bind())
    else:
        upload_status = sa.Enum(
            "REGISTERED", "PENDING", "UPLOADED", "COMPLETED", name="uploadstate"
        )

    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("files", sa.Column("state", upload_status, nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("files", "state")
    # ### end Alembic commands ###
