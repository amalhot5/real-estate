import subprocess


def connect_to_db(
    user="teleport",
    db_name="perchwell",
    db_extended_name="postgres-prod-read-replica",
    port="2023",
    tunnel=True,
) -> None:

    subprocess.Popen(f"kill -9 $(lsof -ti:{port})", shell=True)

    subprocess.run(["tsh", "login", "--proxy=teleport.mgmt.perchwell.com:443"])

    subprocess.run(
        [
            "tsh",
            "db",
            "login",
            f"--db-user={user}",
            f"--db-name={db_name}",
            f"{db_extended_name}",
        ]
    )

    subprocess.run(
        [
            "tsh",
            "db",
            "connect",
            f"--db-user={user}",
            f"--db-name={db_name}",
            f"{db_extended_name}",
        ],
        input=b"\\q\n",
    )
    if tunnel:

        subprocess.run(
            [
                "tsh",
                "proxy",
                "db",
                f"--db-user={user}",
                f"--db-name={db_name}",
                "--tunnel",
                f"{db_extended_name}",
                "--port",
                f"{port}",
            ],
            input=b"CtrlD\n",
        )


import os
from typing import List

# import psycopg2
import psycopg2.extras

# import pandas as pd


def connect_to_dbs(
    user_lst: List[str] = ["teleport"],
    db_name_lst: List[str] = ["perchwell"],
    db_extended_name_lst: List[str] = ["postgres-prod-read-replica"],
    port_lst: List[str] = ["2023"],
) -> None:
    """Connects to PerchWell databasea. Requires AWS VPN and access to Teleport.
        Multiple connections can happen simultaneously by extending list
        parameters.

    Args:
        user_lst (List[str], optional): User used in db connection. Defaults to ["teleport"].
        db_name_lst (List[str], optional): db name. Defaults to ["perchwell"].
        db_extended_name_lst (List[str], optional): db name extended. Defaults to ["postgres-prod-read-replica"].
        port_lst (List[str], optional): ports. Defaults to ["2023"].

    EXAMPLE:
        connect_to_dbs(user_lst=["teleport","teleport"],

        db_name_lst=["perchwell","perchwell"],

        db_extended_name_lst=["postgres-prod-read-replica",'datateam-staging'],

        port_lst=["2023","2024"])

    """
    cmd_lst = []
    for user, db_name, db_extended_name, port in zip(
        user_lst, db_name_lst, db_extended_name_lst, port_lst
    ):
        k_p = f"kill -9 $(lsof -ti:{port})"
        c_t = "tsh login --proxy=teleport.mgmt.perchwell.com:443"
        l_i = f"tsh db login --db-user={user} --db-name={db_name} {db_extended_name}"
        con = (
            f"tsh db connect --db-user={user} --db-name={db_name} {db_extended_name} ^Z"
        )
        tun = f"tsh proxy db --db-user={user} --db-name={db_name} --tunnel {db_extended_name} --port {port}"
        cmd = ";".join([k_p, c_t, l_i, con, tun])
        rslt = (
            '""osascript -e \'tell application "Terminal" to do script "'
            + cmd
            + '"\'""'
        )
        cmd_lst.append(rslt)
    for call in cmd_lst:
        os.system(call)


if __name__ == "__main__":
    # connect_to_db()
    connect_to_db(db_extended_name="euphrates-controller", db_name="pipeline_controller")
    # connect_to_db(db_extended_name='postgres-demo', port=2023)
    # connect_to_db(db_extended_name='euphrates-loader', db_name='mls_data')
    # connect_to_db(db_extended_name='euphrates-geospatial')
    # connect_to_db(db_extended_name='euphrates-loader-devops-806', db_name='mls_data')
    # connect_to_db(db_extended_name='euphrates-controller-devops-806', db_name='pipeline_controller')
    """connect_to_dbs(
        user_lst=["teleport", "teleport"],
        db_name_lst=["postgres", "postgres"],
        db_extended_name_lst=["postgres-prod-read-replica", "euphrates-controller"],
        port_lst=["2023", "2024"],
    )"""
