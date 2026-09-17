# Bài tập 3 (BT3) - Kafka Broker Topology

## 1. Phân tích lỗi BUG-04
**Tình huống**: Lập trình viên copy code và đặt chung `group-id="storex-system"` cho cả hai service là Kho (Inventory) và Điểm thưởng (Loyalty). Hậu quả: Đơn hàng bị trừ kho thì không được cộng điểm và ngược lại.

**Nguyên nhân gốc rễ**:
- Trong Kafka, các Consumer có cùng một `group.id` sẽ tạo thành một **Consumer Group**.
- Cơ chế của Kafka đảm bảo rằng **mỗi Partition trong một Topic chỉ được tiêu thụ bởi tối đa một Consumer trong cùng một Consumer Group** tại một thời điểm nhất định.
- Khi Inventory và Loyalty dùng chung `group-id="storex-system"`, chúng trở thành các worker chia sẻ chung tải (Point-to-Point/Queue mechanism). Kết quả là một Event (ví dụ: `order.created`) khi được gửi vào Topic sẽ chỉ được giao cho **một trong hai** service xử lý. 
- Nếu Inventory nhận được message đó, Loyalty sẽ không nhận được nữa (không được cộng điểm). Ngược lại, nếu Loyalty nhận được, Inventory sẽ không trừ kho.

**Cách khắc phục (Cơ chế Fan-out / Publish-Subscribe)**:
Để cả hai Service cùng nhận được 100% dữ liệu sự kiện một cách độc lập, chúng ta phải đặt chúng vào hai Consumer Group khác nhau. Khi đó, Kafka sẽ coi chúng là 2 nhóm hoàn toàn độc lập và broadcast đầy đủ sự kiện cho cả hai nhóm:
- Inventory-Service: `group-id="inventory-group"`
- Loyalty-Service: `group-id="loyalty-group"`

---

## 2. Thiết kế Partition (REQ-01)
**Tình huống**: Cần scale-up `inventory-service` lên 3 instance và chia đều tải (33% lượng đơn) trong đợt sale. Cần tính toán số lượng partition tối thiểu cho Topic `storex-order-events`.

**Phân tích và Đề xuất**:
- Trong một Consumer Group (ở đây là `inventory-group`), một Partition không thể được tiêu thụ đồng thời bởi nhiều hơn 1 instance.
- Nghĩa là: `Số lượng Instance hoạt động hiệu quả <= Số lượng Partitions`.
- Nếu Topic chỉ có 1 hoặc 2 Partitions mà có 3 instance, sẽ có 1-2 instance phải chịu trạng thái **Idle** (chờ việc, không làm gì cả).
- Để cả 3 instance của `inventory-service` cùng hoạt động và chia đều tải, Topic `storex-order-events` cần có **tối thiểu là 3 Partitions**.

**Kết luận đề xuất**: 
- **Số lượng Partitions khuyến nghị**: Ít nhất là **3** (hoặc bội số của 3 như 6, 9 nếu có dự định scale lớn hơn trong tương lai). 
- Câu lệnh tạo topic mẫu: 
  ```bash
  kafka-topics.sh --create --topic storex-order-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
  ```
