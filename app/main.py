import socket  # noqa: F401
import threading


async def handle_connection(client: socket.socket):
    """
    This function is called for each new client connection.
    """
    # Get the client's address for logging
    with client: 
        while True:
            data = client.recv(1024)
            if not data:
                break

            print(f"Received data: {data}")
            commands = data.split(b" ")
            if data.startswith(b"*1\r\n$4\r\nPING\r\n"):
                client.sendall(b"+PONG\r\n")
            if data.startswith(b"*2\r\n$4\r\nECHO\r\n"):
                msg = data.split(b"\r\n")[-2]
                client.sendall(b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n")

        

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_connection, args=(connection,)).start()


if __name__ == "__main__":
    main()