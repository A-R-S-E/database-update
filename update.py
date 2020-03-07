from mcstatus import MinecraftServer
import psycopg2
import datetime
import socket
import traceback
import concurrent.futures

from dotenv import load_dotenv
import os

load_dotenv()

def connect_db():
    conn = psycopg2.connect(database=os.environ['POSTGRES_DATABASE'], user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'], host=os.environ['POSTGRES_HOST'], port=os.environ['POSTGRES_PORT'])
    conn.autocommit = True
    return conn

conn = connect_db()
with conn.cursor() as c:
    c.execute("SELECT ip, port FROM servers")
    servers = c.fetchall()

def scan(ip, port):
    ipstr = "{}:{}".format(ip, port)
    print(ipstr)
    try:
        status = MinecraftServer.lookup(ipstr).status(retries=2)
    except (socket.timeout, ConnectionRefusedError, ConnectionResetError, OSError):
        return (ip, port, None)
    except Exception as e:
        print(e)
        return (ip, port, None)
    database(ip, port, status)

def database(ip, port, status):
    local_conn = connect_db()
    with local_conn.cursor() as c:
        if status:
            vers = status.version.name
            ping = status.latency
            now = datetime.datetime.utcnow()
            prot = status.version.protocol
            maxP = status.players.max
            curP = status.players.online
            modded = bool(status.raw.get('modinfo', {"modList":[]})['modList'])
            text = status.description.get("text") if isinstance(status.description, dict) else str(status.description)
            c.execute("""UPDATE public.servers SET "version"=%s, latency=%s, last_checked=%s, protocol=%s, max_users=%s, online_users=%s, motd=%s, modded=%s WHERE ip=%s AND port=%s RETURNING id;""", 
                        (vers, ping, now, prot, maxP, curP, text, modded, ip, port))
            server_id = c.fetchall()[0][0]
            if status.players.__dict__.get("sample", None):
                sample = status.players.sample
                for player in sample:
                    try:
                        c.execute("""INSERT INTO public.users (username, uuid) VALUES(%s, %s) ON CONFLICT DO NOTHING RETURNING id;""", (player.name, player.id))
                        res = c.fetchall()
                        try:
                            user_id = res[0][0]
                        except IndexError:
                            c.execute("""SELECT id FROM public.users WHERE uuid LIKE %s;""", (player.id,))
                            res = c.fetchall()
                            user_id = res[0][0]
                        c.execute("""INSERT INTO public.server_users (server_id, user_id) VALUES(%s, %s) ON CONFLICT DO NOTHING;""", (server_id, user_id))
                    except psycopg2.errors.StringDataRightTruncation:
                        continue
                    except Exception as e:
                        print(e)

def main():
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        for ip, port in servers:
            futures.append(executor.submit(scan, ip, port))
    
    for done_process in concurrent.futures.as_completed(futures):
        pass

if __name__ == "__main__":
    main()