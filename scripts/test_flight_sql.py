import os
from time import sleep
import pyarrow
from adbc_driver_flightsql import dbapi as flight_sql, DatabaseOptions


# Setup variables
max_attempts: int = 10
sleep_interval: int = 10
flight_password = os.environ["FLIGHT_PASSWORD"]

def main():
    for attempt in range(max_attempts):
        try:
            with flight_sql.connect(uri="grpc+tls://localhost:31337",
                                    db_kwargs={"username": "flight_username",
                                               "password": flight_password,
                                               # Not needed if you use a trusted CA-signed TLS cert
                                               DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
                                               }
                                    ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                                parameters=[24]
                                )
                    x = cur.fetch_arrow_table()
                    print(x)
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            else:
                print(f"Attempt {attempt + 1} failed: {e}, sleeping for {sleep_interval} seconds")
                sleep(sleep_interval)
        else:
            print("Success!")
            break


if __name__ == "__main__":
    main()
