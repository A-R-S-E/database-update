from mcstatus import MinecraftServer
import psycopg2
import datetime
import socket
import concurrent.futures

from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(database=os.environ['POSTGRES_DATABASE'], user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'], host=os.environ['POSTGRES_HOST'], port=os.environ['POSTGRES_PORT'])
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
    return (ip, port, status)

def main():
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        for ip, port in servers:
            futures.append(executor.submit(scan, ip, port))
    
    with conn.cursor() as c:
        for done_process in concurrent.futures.as_completed(futures):
            ip, port, status = done_process.result()
            if status:
                vers = status.version.name
                ping = status.latency
                now = datetime.datetime.utcnow()
                prot = status.version.protocol
                maxP = status.players.max
                curP = status.players.online
                modded = bool(status.raw.get('modinfo', {"modList":[]})['modList'])
                text = status.description.get("text") if isinstance(status.description, dict) else str(status.description)
                c.execute("""UPDATE public.servers SET "version"=%s, latency=%s, last_checked=%s, protocol=%s, max_users=%s, online_users=%s, motd=%s, modded=%s WHERE ip=%s AND port=%s;""", 
                          (vers, ping, now, prot, maxP, curP, text, modded, ip, port))
                conn.commit()

if __name__ == "__main__":
    main()