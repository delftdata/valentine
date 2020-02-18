from knowledgerepr.ekgstore.pg_store import PGStore


if __name__ == "__main__":

    store = PGStore(db_ip="localhost", db_port="5432", db_name="test_py", db_user="postgres", db_passwd="admin")
    store.init_schema()
    store.close_con()
    print("Schema initialized")

