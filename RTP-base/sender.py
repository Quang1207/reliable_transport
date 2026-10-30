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

    # Đọc dữ liệu nhị phân từ stdin và chia nhỏ thành các khối tối đa 1456 bytes
    # Giới hạn payload = 1472 (UDP MTU chuẩn) - 16 (Header) = 1456 bytes
    data_stream = sys.stdin.buffer.read()
    max_payload = 1456
    chunks = [
        data_stream[i : i + max_payload]
        for i in range(0, len(data_stream), max_payload)
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

    # 1. Bắt đầu kết nối (Gửi gói START: type=0, seq_num=0)
    start_pkt = create_packet(0, 0, b"")
    while True:
        s.sendto(start_pkt, receiver_addr)
        r, _, _ = select.select([s], [], [], 0.5)
        if r:
            pkt, _ = s.recvfrom(2048)
            is_valid, ack_hdr = parse_ack(pkt)
            if is_valid and ack_hdr.type == 3 and ack_hdr.seq_num == 1:
                break

    # 2. Truyền tải dữ liệu với Cửa sổ trượt (Go-Back-N / Cumulative ACK)
    base = 1
    next_seq = 1
    timer_start = None

    while base <= num_chunks:
        # Gửi toàn bộ các gói tin mới trong phạm vi cửa sổ
        while next_seq < base + window_size and next_seq <= num_chunks:
            chunk = chunks[next_seq - 1]
            pkt = create_packet(2, next_seq, chunk)
            s.sendto(pkt, receiver_addr)
            if timer_start is None:
                timer_start = time.time()
            next_seq += 1

        # Chờ nhận ACK (tính toán thời gian timeout còn lại trong mốc 500ms)
        time_left = 0.5 - (time.time() - timer_start) if timer_start else 0.5
        time_left = max(0.001, time_left)

        r, _, _ = select.select([s], [], [], time_left)
        if r:
            pkt, _ = s.recvfrom(2048)
            is_valid, ack_hdr = parse_ack(pkt)
            if is_valid and ack_hdr.type == 3:
                # Cumulative ACK: Di chuyển cửa sổ nếu nhận được ACK lớn hơn base
                if ack_hdr.seq_num > base:
                    base = ack_hdr.seq_num
                    if base == next_seq:
                        # Tất cả các gói đã gửi đều đã được ACK -> dừng timer
                        timer_start = None
                    else:
                        # Cửa sổ di chuyển -> reset lại timer 500ms
                        timer_start = time.time()

        # Kiểm tra quá hạn 500ms -> Truyền lại tất cả các gói chưa được ACK trong cửa sổ
        if timer_start is not None and (time.time() - timer_start >= 0.5):
            timer_start = time.time()
            for seq in range(base, next_seq):
                chunk = chunks[seq - 1]
                pkt = create_packet(2, seq, chunk)
                s.sendto(pkt, receiver_addr)

    # 3. Kết thúc kết nối (Gửi gói END: type=1, seq_num=base)
    end_pkt = create_packet(1, base, b"")
    s.sendto(end_pkt, receiver_addr)
    # Bắt đầu timer 500ms: Thoát nếu nhận được ACK hoặc khi hết 500ms
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
