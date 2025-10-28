import json
import socket
import time
import pandas as pd


def send_over_socket(file_path, host='0.0.0.0', port=9999, chunk_size=2):
    # Initialize TCP socket server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Listening for connections on {host}:{port}")

    last_sent_index = 0  # Tracks progress for resuming after disconnection

    # Continuous loop to handle reconnections and resume data streaming
    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")
        try:
            with open(file_path, 'r') as file:
                # Skip lines already sent (resuming from last_sent_index)
                for _ in range(last_sent_index):
                    next(file)

                records = []
                for line in file:
                    records.append(json.loads(line))
                    if len(records) == chunk_size:
                        # Display current chunk being sent
                        chunk = pd.DataFrame(records)
                        print(chunk)

                        # Send records over socket in JSON format
                        for record in chunk.to_dict(orient='records'):
                            serialize_data = json.dumps(record).encode('utf-8')
                            conn.send(serialize_data + b'\n')
                            time.sleep(5)
                            last_sent_index += 1

                        records = []

        except (BrokenPipeError, ConnectionResetError):
            # Handle client disconnection gracefully
            print("Client disconnected.")

        finally:
            conn.close()
            print("Connection closed")


if __name__ == "__main__":
    # Start socket server and stream data from Yelp dataset
    send_over_socket("datasets/yelp_academic_dataset_review.json")
