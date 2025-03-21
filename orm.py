from database import engine, Session, Base
from models import SpimexTradingResults
import pandas as pd


def create_tables():
    Base.metadata.create_all(engine)


def insert_data_pull_to_db(df: pd.DataFrame):
    """Insert pull of data to database."""
    with Session() as session:
        for i, r in df.iterrows():
            obj = SpimexTradingResults(
                exchange_product_id=r['exchange_product_id'],
                exchange_product_name=r['exchange_product_name'],
                oil_id=r['oil_id'],
                delivery_basis_id=r['delivery_basis_id'],
                delivery_basis_name=r['delivery_basis_name'],
                delivery_type_id=r['delivery_type_id'],
                volume=int(r['volume']),
                total=int(r['total']),
                count=int(r['count']),
                date=r['date']
            )
            session.add(obj)
        session.commit()
