import argparse
import socket

from utils import PacketHeader, compute_checksum


def receiver(receiver_ip, receiver_port, window_size):
    """TODO: Listen on socket and print received message to sys.stdout."""
    import sys

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    expected_seq = 0
    buffer = {}
    in_connection = False

    while True:
        pkt, address = s.recvfrom(2048)
        if len(pkt) < 16:
            continue

        # Tách header và payload
        pkt_header = PacketHeader(pkt[:16])
        msg = pkt[16 : 16 + pkt_header.length]

        # Kiểm tra checksum
        pkt_checksum = pkt_header.checksum
        pkt_header.checksum = 0
        computed_checksum = compute_checksum(bytes(pkt_header) + msg)
        if pkt_checksum != computed_checksum:
            continue

        pkt_type = pkt_header.type
        seq_num = pkt_header.seq_num

        # 1. Xử lý gói START (type=0)
        if pkt_type == 0:
            if not in_connection:
                in_connection = True
                expected_seq = 1
                buffer.clear()
                # Phản hồi ACK xác nhận START (seq_num=1)
                ack_hdr = PacketHeader(type=3, seq_num=1, length=0, checksum=0)
                ack_hdr.checksum = compute_checksum(bytes(ack_hdr))
                s.sendto(bytes(ack_hdr), address)
            continue

        # 2. Xử lý gói DATA (type=2)
        elif pkt_type == 2:
            if not in_connection:
                continue

            # Loại bỏ nếu seq_num >= expected_seq + window_size
            if seq_num >= expected_seq + window_size:
                continue

            # Nhận và đệm gói tin nếu chưa có trong buffer
            if seq_num >= expected_seq and seq_num not in buffer:
                buffer[seq_num] = msg

            # Xả dữ liệu liên tục ra stdout nếu có sẵn trong buffer
            while expected_seq in buffer:
                sys.stdout.buffer.write(buffer.pop(expected_seq))
                sys.stdout.buffer.flush()
                expected_seq += 1

            # Gửi Cumulative ACK với expected_seq tiếp theo mong muốn
            ack_hdr = PacketHeader(type=3, seq_num=expected_seq, length=0, checksum=0)
            ack_hdr.checksum = compute_checksum(bytes(ack_hdr))
            s.sendto(bytes(ack_hdr), address)

        # 3. Xử lý gói END (type=1)
        elif pkt_type == 1:
            if in_connection:
                # Gửi ACK cho gói END và thoát chương trình
                ack_hdr = PacketHeader(type=3, seq_num=expected_seq, length=0, checksum=0)
                ack_hdr.checksum = compute_checksum(bytes(ack_hdr))
                s.sendto(bytes(ack_hdr), address)
                break

    s.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
