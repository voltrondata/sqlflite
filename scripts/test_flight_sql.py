import os
import adbc_driver_flightsql.dbapi as flight_sql

flight_password = os.getenv("FLIGHT_PASSWORD")

with flight_sql.connect(uri="grpc+tls://localhost:31337",
                        db_kwargs={"username": "flight_username",
                                   "password": flight_password,
                                   "adbc.flight.sql.client_option.tls_skip_verify": "true" # Not needed if you use a trusted CA-signed TLS cert
                                   }
                        ) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                    parameters=[24]
                    )
        x = cur.fetch_arrow_table()
        print(x)
