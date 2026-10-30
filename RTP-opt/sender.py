import argparse
import socket

from utils import PacketHeader, compute_checksum


def sender(receiver_ip, receiver_port, window_size):
    """TODO: Open socket and send message from sys.stdin."""
    import select
    import sys
    import time

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_addr = (receiver_ip, receiver_port)

    raw_data = sys.stdin.buffer.read()
    max_payload = 1456
    chunks = [
        raw_data[i : i + max_payload]
        for i in range(0, len(raw_data), max_payload)
    ]
    num_chunks = len(chunks)

    def create_packet(pkt_type, seq_num, payload=b""):
        hdr = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload), checksum=0)
        hdr.checksum = compute_checksum(bytes(hdr) + payload)
        return bytes(hdr) + payload

    def parse_ack(pkt_bytes):
        if len(pkt_bytes) < 16:
            return False, None
        hdr = PacketHeader(pkt_bytes[:16])
        orig_checksum = hdr.checksum
        hdr.checksum = 0
        computed = compute_checksum(bytes(hdr) + pkt_bytes[16:])
        if orig_checksum != computed:
            return False, None
        return True, hdr

    # 1. Bắt tay (START)
    start_pkt = create_packet(0, 0, b"")
    while True:
        s.sendto(start_pkt, receiver_addr)
        r, _, _ = select.select([s], [], [], 0.5)
        if r:
            pkt, _ = s.recvfrom(2048)
            is_valid, ack_hdr = parse_ack(pkt)
            if is_valid and ack_hdr.type == 3 and ack_hdr.seq_num == 1:
                break

    # 2. Truyền dữ liệu với Selective Repeat
    base = 1
    next_seq = 1
    timer_start = None
    acked = [False] * (num_chunks + 2)

    while base <= num_chunks:
        # Gửi các gói tin mới trong cửa sổ
        while next_seq < base + window_size and next_seq <= num_chunks:
            chunk = chunks[next_seq - 1]
            pkt = create_packet(2, next_seq, chunk)
            s.sendto(pkt, receiver_addr)
            if timer_start is None:
                timer_start = time.time()
            next_seq += 1

        # Chờ nhận ACK cá nhân
        time_left = 0.5 - (time.time() - timer_start) if timer_start else 0.5
        time_left = max(0.001, time_left)

        r, _, _ = select.select([s], [], [], time_left)
        if r:
            pkt, _ = s.recvfrom(2048)
            is_valid, ack_hdr = parse_ack(pkt)
            if is_valid and ack_hdr.type == 3:
                ack_num = ack_hdr.seq_num
                if 1 <= ack_num <= num_chunks:
                    acked[ack_num] = True

                # Trượt base khi các gói ở đầu cửa sổ đã được ACK
                old_base = base
                while base <= num_chunks and acked[base]:
                    base += 1

                # Nếu base dịch chuyển, reset hoặc dừng timer
                if base > old_base:
                    if base == next_seq:
                        timer_start = None
                    else:
                        timer_start = time.time()

        # Timeout 500ms -> Chỉ truyền lại các gói chưa ACK trong cửa sổ hiện tại
        if timer_start is not None and (time.time() - timer_start >= 0.5):
            timer_start = time.time()
            for seq in range(base, next_seq):
                if not acked[seq]:
                    chunk = chunks[seq - 1]
                    pkt = create_packet(2, seq, chunk)
                    s.sendto(pkt, receiver_addr)

    # 3. Đóng kết nối (END)
    end_pkt = create_packet(1, base, b"")
    s.sendto(end_pkt, receiver_addr)
    r, _, _ = select.select([s], [], [], 0.5)
    if r:
        try:
            s.recvfrom(2048)
        except Exception:
            pass

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

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
