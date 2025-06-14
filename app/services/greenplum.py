import psycopg2
import pandas as pd

from sqlalchemy import create_engine
from app.core.config import settings

class GPConnection:
    def __init__(self):
        self.db_config = {
            'database': settings.GP_DATABASE,
            'user': settings.GP_USER,
            'password': settings.GP_PASSWORD,
            'host': settings.GP_HOST,
            'port': settings.GP_PORT
        }
        self.conn = None
        self.engine = None

    def __enter__(self):
        # Создаем SQLAlchemy engine для pandas
        connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        self.engine = create_engine(connection_string)
        
        # Оставляем psycopg2 соединение для других операций если нужно
        self.conn = psycopg2.connect(**self.db_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.engine:
            self.engine.dispose()
        if self.conn:
            self.conn.close()

    def get_first_rec_item(self, nm_id: int):
        try:
            query = f"""
            SELECT *
            FROM ...
            WHERE ...;
            """
            # Используем SQLAlchemy engine для pandas и параметризованный запрос
            df = pd.read_sql_query(query, self.engine, params=(nm_id,))
            
            if df.empty:
                return None
            items = df.loc[0, 'final_rec_items']
            if isinstance(items, list) and items:
                return items[0]
            if isinstance(items, str):
                items_list = [int(i.strip()) for i in items.strip('[]').split(',') if i.strip().isdigit()]
                return items_list[0] if items_list else None
            return None
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Error getting recommendation for nm_id {nm_id}: {str(e)}")
            return None   