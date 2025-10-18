import argparse
import socket  # noqa: F401
import asyncio


async def handle_connection(reader, writer):
    """
    This function is called for each new client connection.
    """
    # Get the client's address for logging
    client_addr = writer.get_extra_info("peername")
    print(f"✅ New connection from {client_addr}")

    try:
        while True:
            data = await asyncio.wait_for(reader.read(1024), timeout=60.0)

            if not data:
                print(f"⭕️ Client {client_addr} disconnected.")
                break

            message = data.decode().strip()
            print(f"➡️ Received '{message}' from {client_addr}")

            writer.write(b"+PONG\r\n")
            # Send the data immediatly
            await writer.drain()

    except asyncio.TimeoutError:
        print(f"🕰️ Connection with {client_addr} timed out.")
    except ConnectionResetError:
        print(f"❌ Connection reset by {client_addr}.")
    except Exception as e:
        print(f"An unexpected error occurred with {client_addr}: {e}")
    finally:
        # No matter what happens, always close the connection
        print(f"Closing connection with {client_addr}")
        writer.close()
        await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser(description="A simple Redis-like server.")

    parser.add_argument(
        "-H",
        "--host",
        type=str,
        default="127.0.0.1",
        help="The interface to listen on (default: 127.0.0.1).",
    )

    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=6379,
        help="The port to listen on (default: 6380).",
    )

    args = parser.parse_args()

    print("🚀 Launching server on...")
    print(f"   Host: {args.host}")
    print(f"   Port: {args.port}")

    server = await asyncio.start_server(handle_connection, args.host, args.port)

    print(f"🚀 Server listening on {args.host}:{args.port}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())